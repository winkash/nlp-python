from affine.model.admin_decisions import *
from affine.model.apps import *
from affine.model.external_app_ids import *
from affine.model.base import *
from affine.model.preferences import *
from affine.model.boxes import *
from affine.model.campaigns import *
from affine.model.channels import *
from affine.model.classifier_target_labels import *
from affine.model.detection import *
from affine.model.forecasting import *
from affine.model.beta_detection import *
from affine.model.detection_failures import *
from affine.model.detector_logging import *
from affine.model.text_detection_versions import *
from affine.model.labels import *
from affine.model.label_hashes import *
from affine.model.line_items import *
from affine.model.named_entities import *
from affine.model.secondary_tables import *
from affine.model.settings import *
from affine.model.url_blacklist import *
from affine.model.user_keywords import *
from affine.model.users import *
from affine.model.videos import *
from affine.model.web_pages import *
from affine.model.web_page_label_results import *
from affine.model.line_item_groups import *
from affine.model.publishers import *
from affine.model.label_inventory import *
from affine.model.label_recall import *
from affine.model.training_data import *
from affine.model.mturk import *
from affine.model.placement import *
from affine.model.ingestion_settings import IngestionSettings
# Ensure all relations and backrefs are wired up
from sqlalchemy.orm import configure_mappers
configure_mappers()
del configure_mappers
