from enum import Enum
from pyspark.sql.types import DataType


class TableStorage(Enum):
    Unkown = 0
    S3 = 1
    Azure = 2
    Local = 3

    @staticmethod
    def storage_from_str(loc: str):
        if loc.lower() == "s3":
            return TableStorage.S3
        if loc.lower() == "azure":
            return TableStorage.Azure
        if loc.lower() == "local":
            return TableStorage.Local
        return TableStorage.Unkown


class FileFormat(Enum):
    Any = 0
    Parquet = 1
    ORC = 2
    Avro = 3

    @staticmethod
    def file_from_str(f: str):
        if f.lower() == "parquet":
            return FileFormat.Parquet
        if f.lower() == "orc":
            return FileFormat.ORC
        if f.lower() == "avro":
            return FileFormat.Avro
        return FileFormat.Any

    @staticmethod
    def file_to_str(f):
        if f == FileFormat.Parquet:
            return "parquet"
        if f == FileFormat.ORC:
            return "orc"
        if f == FileFormat.Avro:
            return "avro"
        return "Any"


class LakeFormat(Enum):
    Unkown = 0
    Iceberg = 1
    DeltaLake = 2

    @staticmethod
    def lakeformat_from_str(loc: str):
        if loc.lower() == "iceberg":
            return LakeFormat.Iceberg
        if loc.lower().startswith("delta"):
            return LakeFormat.DeltaLake
        return LakeFormat.Unkown


class LakeCatalogs(Enum):
    NoCatalog = 0
    Glue = 1
    Hive = 2
    REST = 3
    Unity = 4
    Nessie = 5

    @staticmethod
    def catalog_from_str(loc: str):
        if loc.lower() == "glue":
            return LakeCatalogs.Glue
        if loc.lower() == "hive":
            return LakeCatalogs.Hive
        if loc.lower() == "rest":
            return LakeCatalogs.REST
        if loc.lower() == "unity":
            return LakeCatalogs.Unity
        if loc.lower() == "nessie":
            return LakeCatalogs.Nessie
        return LakeCatalogs.NoCatalog


class SparkColumn:
    def __init__(
        self,
        _column_name: str,
        _spark_type: DataType,
        _nullable: bool,
    ):
        self.column_name = _column_name
        self.spark_type = _spark_type
        self.nullable = _nullable


class SparkTable:
    def __init__(
        self,
        _catalog_name: str,
        _database_name: str,
        _table_name: str,
        _columns: dict[str, SparkColumn],
        _deterministic: bool,
        _location: str,
        _lake_format: LakeFormat,
        _file_format: FileFormat,
        _storage: TableStorage,
    ):
        self.catalog_name = _catalog_name
        self.database_name = _database_name
        self.table_name = _table_name
        self.columns = _columns
        self.deterministic = _deterministic
        self.location = _location
        self.lake_format = _lake_format
        self.file_format = _file_format
        self.storage = _storage

    def get_namespace_path(self) -> str:
        return f"test.{self.table_name}"

    def get_table_full_path(self) -> str:
        return f"{self.catalog_name}.test.{self.table_name}"

    def get_clickhouse_path(self) -> str:
        return f"{self.database_name}.{self.table_name}"
