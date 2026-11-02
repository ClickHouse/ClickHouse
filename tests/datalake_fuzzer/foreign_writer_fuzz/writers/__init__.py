from .base import Capabilities, ForeignReadError, OpRejected, Source, Writer
from .clickhouse_writers import ClickHouseDeltaWriter, ClickHouseIcebergWriter
from .deltars import DeltaRsWriter
from .pyiceberg_rest import PyIcebergRestWriter
from .spark_delta import SparkDeltaWriter
from .spark_hudi import SparkHudiWriter
from .spark_iceberg import SparkIcebergWriter
from .spark_paimon import SparkPaimonWriter

WRITERS = {
    cls.name: cls
    for cls in (
        SparkIcebergWriter,
        PyIcebergRestWriter,
        SparkDeltaWriter,
        DeltaRsWriter,
        SparkHudiWriter,
        SparkPaimonWriter,
        ClickHouseIcebergWriter,
        ClickHouseDeltaWriter,
    )
}

__all__ = [
    "WRITERS",
    "Capabilities",
    "ForeignReadError",
    "OpRejected",
    "Source",
    "Writer",
]
