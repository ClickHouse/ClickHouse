import glob
import json
import logging
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

import pyspark
import pytest
from azure.storage.blob import BlobServiceClient
from minio.deleteobjects import DeleteObject
from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    list_s3_objects,
    prepare_s3_bucket,
)
from helpers.test_tools import TSV

from helpers.iceberg_utils import (
    default_upload_directory,
    default_download_directory,
    execute_spark_query_general,
    get_creation_expression,
    write_iceberg_from_df,
    generate_data,
    create_iceberg_table,
    drop_iceberg_table,
    check_schema_and_data,
    get_uuid_str,
    check_validity_and_get_prunned_files_general,
    get_last_snapshot
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
