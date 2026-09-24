from enum import Enum
from pyspark.sql.types import DataType, StructType


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
    Paimon = 3

    @staticmethod
    def lakeformat_from_str(loc: str):
        if loc.lower() == "iceberg":
            return LakeFormat.Iceberg
        if loc.lower().startswith("delta"):
            return LakeFormat.DeltaLake
        if loc.lower() == "paimon":
            return LakeFormat.Paimon
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
        _generated: bool,
        _clickhouse_type: str = "",
    ):
        self.column_name = _column_name
        self.spark_type = _spark_type
        self.nullable = _nullable
        self.generated = _generated
        # Original ClickHouse type string; empty when the column has no CH-side origin
        # (e.g. added via a Spark-side ALTER).
        self.clickhouse_type = _clickhouse_type

    def _flat_column(
        self, res: dict[str, DataType], next_path: str, next_type: DataType
    ):
        res[next_path] = next_type
        if isinstance(next_type, StructType):
            for f in next_type.fields:
                self._flat_column(res, f"{next_path}.`{f.name}`", f.dataType)

    def flat_column(self, res: dict[str, DataType]):
        self._flat_column(res, self.column_name, self.spark_type)


class SparkTable:
    def __init__(
        self,
        _catalog_name: str,
        _database_name: str,
        _table_name: str,
        _columns: dict[str, SparkColumn],
        _deterministic: bool,
        _lake_format: LakeFormat,
        _file_format: FileFormat,
        _storage: TableStorage,
        _catalog: LakeCatalogs,
    ):
        self.catalog_name = _catalog_name
        self.database_name = _database_name
        self.table_name = _table_name
        self.columns = _columns
        self.deterministic = _deterministic
        self.lake_format = _lake_format
        self.file_format = _file_format
        self.storage = _storage
        self.catalog = _catalog
        self.check_constraints: dict[str, str] = {}
        # Delta liquid clustering (CLUSTER BY). Kept in sync at CREATE and by the
        # ALTER ... CLUSTER BY / CLUSTER BY NONE statements. Databricks/Delta treats
        # clustered tables differently in OPTIMIZE: FULL reclusters them, ZORDER BY is
        # rejected, and a WHERE predicate is not supported.
        self.clustered: bool = False
        # Plain partition-key column names (PARTITIONED BY). OPTIMIZE ... WHERE only
        # accepts partition predicates, so the WHERE column must come from here.
        # NOTE: this is the WHERE-usable column list, NOT a partitioned-vs-unpartitioned
        # flag -- it stays empty for transformed Iceberg specs (bucket/year/truncate/void)
        # and for tables created via the pyiceberg catalog path. Use `partitioned` for
        # "is this table partitioned at all".
        self.partition_keys: list[str] = []
        # Whether the table has ANY partitioning (plain or transformed, SQL DDL or catalog
        # PartitionSpec). Operations that assume no partition tuple / partition filter
        # (e.g. Iceberg equality deletes, add_files) must gate on this, not partition_keys.
        self.partitioned: bool = False
        # Active Iceberg partition-transform clauses, exactly as they appear in Spark SQL
        # (e.g. `bucket(16, c0)`, `year(c1)`, `c2`). Kept in sync at CREATE and by
        # ALTER ... ADD/DROP/REPLACE PARTITION FIELD so DROP/REPLACE can target a field that
        # actually exists. Iceberg-only; empty for Delta/Paimon.
        self.partition_fields: list[str] = []

    def get_namespace_path(self) -> str:
        return f"test.{self.table_name}"

    def get_table_full_path(self) -> str:
        return f"{self.catalog_name}.test.{self.table_name}"

    def get_clickhouse_path(self) -> str:
        if self.catalog == LakeCatalogs.NoCatalog:
            return f"{self.database_name}.{self.table_name}"
        return f"{self.database_name}.`test.{self.table_name}`"

    def flat_columns(self) -> dict[str, DataType]:
        res = {}
        for _, val in self.columns.items():
            val.flat_column(res)
        return res
