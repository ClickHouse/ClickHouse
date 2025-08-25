#!/usr/bin/env python3

import random
from abc import abstractmethod
from enum import Enum
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, date
import math
import string
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
    DataType,
)

from .clickhousetospark import ClickHouseSparkTypeMapper


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
        if loc.lower() == "deltalake":
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
        _table_name: str,
        _columns: dict[str, SparkColumn],
    ):
        self.table_name = _table_name
        self.columns = _columns


class LakeTableGenerator:
    def __init__(self, _bucket: str):
        self.bucket = _bucket
        self.type_mapper = ClickHouseSparkTypeMapper()
        self.write_format = FileFormat.Parquet
        self._min_nested = 0
        self._max_nested = 100
        self._min_str_len = 0
        self._max_str_len = 100

    @staticmethod
    def get_next_generator(bucket: str, lake: LakeFormat):
        return (
            IcebergTableGenerator(bucket)
            if lake == LakeFormat.Iceberg
            else DeltaLakePropertiesGenerator(bucket)
        )

    @abstractmethod
    def generate_table_properties(
        self, columns: list[dict[str, str]], include_all: bool = False
    ) -> dict[str, str]:
        pass

    @abstractmethod
    def get_format(self) -> str:
        pass

    @abstractmethod
    def set_basic_properties(self) -> dict[str, str]:
        return {}

    def generate_create_table_ddl(
        self,
        catalog_name: str,
        table_name: str,
        columns: list[dict[str, str]],
        format: str,
    ) -> tuple[str, SparkTable]:
        """
        Generate a complete CREATE TABLE DDL statement with random properties

        Args:
            table_name: Name of the table
        """
        self.write_format = FileFormat.file_from_str(format)

        ddl = f"CREATE TABLE IF NOT EXISTS {catalog_name}.test.{table_name} ("
        columns_list = []
        columns_str = []
        columns_spark = {}
        for val in columns:
            # Convert columns
            str_type, nullable, spark_type = self.type_mapper.clickhouse_to_spark(
                val["type"], False
            )
            columns_str.append(
                f"{val["name"]} {str_type}{"" if nullable else " NOT NULL"}"
            )
            columns_list.append(val["name"])
            columns_spark[val["name"]] = spark_type
        ddl += ",".join(columns_str)
        ddl += ")"

        # Add USING clause
        ddl += f" USING {self.get_format()}"

        # Add Partition by
        if random.randint(1, 3) == 1:
            random_subset = random.sample(
                columns_list, k=random.randint(1, len(columns_list))
            )
            random.shuffle(random_subset)
            ddl += f" PARTITIONED BY ({",".join(random_subset)})"

        properties = self.set_basic_properties()
        # Add table properties
        if random.randint(1, 2) == 1:
            properties.update(self.generate_table_properties(columns))
        if len(properties) > 0:
            ddl += " TBLPROPERTIES ("
            prop_lines = []
            for key, value in properties.items():
                prop_lines.append(f"'{key}' = '{value}'")
            ddl += ",".join(prop_lines)
            ddl += ")"
        return (ddl + ";", SparkTable(table_name, columns_spark))

    def generate_alter_table_statements(
        self, table_name: str, columns: list[dict[str, str]]
    ) -> str:
        """Generate random ALTER TABLE statements for testing"""
        properties = self.generate_table_properties(columns)

        if properties and random.randint(1, 2) == 1:
            # Set random properties
            key = random.choice(list(properties.keys()))
            return f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{key}' = '{properties[key]}');"
        elif properties:
            # Unset a property
            key = random.choice(list(properties.keys()))
            return f"ALTER TABLE {table_name} UNSET TBLPROPERTIES ('{key}');"
        return ""

    # ============================================================
    # Random data
    # ============================================================
    def _rand_bool(self):
        return random.choice([True, False])

    def _rand_int(self, lo, hi):
        return random.randint(lo, hi)

    def _rand_float(self, lo, hi):
        r = random.random()
        if r <= 0.01:
            return float("nan")
        if r <= 0.02:
            return math.inf if random.random() < 0.5 else -math.inf
        if r <= 0.03:
            return float(0.0) if random.random() < 0.5 else float(-0.0)
        # otherwise finite, keep ranges reasonable to avoid overflow when casting to FloatType
        return float(lo) + (float(hi) - float(lo)) * random.random()

    def _rand_string(self):
        alphabet = (
            string.ascii_letters + string.digits + " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
        )
        return "".join(random.choice(alphabet) for _ in range(self._max_str_len))

    def _rand_binary(self):
        any_chars = (
            "\t\n\r"  # allowed control chars
            + string.ascii_letters
            + string.digits
            + " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"  # punctuation
            + "\u0020\ud7ff\ue000-\ufffd"  # other valid Unicode ranges
        )
        valid_chars = []
        valid_chars.extend(c for c in any_chars if ord(c) < 0xD800)
        valid_chars.extend(chr(c) for c in range(0xE000, 0xFFFD + 1))
        # Generate the random string
        return "".join(random.choice(valid_chars) for _ in range(self._max_str_len))

    def _rand_date(self):
        start = date(1900, 1, 1).toordinal()
        end = date(2300, 12, 31).toordinal()
        return date.fromordinal(self._rand_int(start, end))

    def _rand_timestamp(self):
        start = datetime(1900, 1, 1)
        end = datetime(2300, 12, 31)
        delta = end - start
        secs = self._rand_int(0, int(delta.total_seconds()))
        micros = self._rand_int(0, 999999)
        return start + timedelta(seconds=secs, microseconds=micros)

    def _rand_decimal(self, precision, scale):
        # Set context a bit higher to avoid rounding surprises
        getcontext().prec = max(precision, 38)
        int_digits = precision - scale
        # Largest integer part allowed (e.g., p=5,s=2 -> int_digits=3 -> up to 999)
        max_int = 10**int_digits - 1
        int_part = self._rand_int(0, max(0, max_int))
        frac_part = self._rand_int(0, 10**scale - 1) if scale > 0 else 0
        sign = -1 if random.random() < 0.5 else 1
        if scale > 0:
            s = f"{sign*int_part}.{frac_part:0{scale}d}"
        else:
            s = f"{sign*int_part}"
        return Decimal(s)

    def _random_value_for_type(self, dtype: DataType):
        """Return a random Python value that conforms to the given Spark DataType."""
        if isinstance(dtype, BooleanType):
            return self._rand_bool()
        if isinstance(dtype, ByteType):
            return self._rand_int(-128, 127)
        if isinstance(dtype, ShortType):
            return self._rand_int(-32768, 32767)
        if isinstance(dtype, IntegerType):
            return self._rand_int(-2_147_483_648, 2_147_483_647)
        if isinstance(dtype, LongType):
            return self._rand_int(-9_223_372_036_854_775_808, 9_223_372_036_854_775_807)
        if isinstance(dtype, FloatType):
            return float(self._rand_float(-1e5, 1e5))
        if isinstance(dtype, DoubleType):
            return float(self._rand_float(-1e9, 1e9))
        if isinstance(dtype, DecimalType):
            return self._rand_decimal(dtype.precision, dtype.scale)
        if isinstance(dtype, StringType):
            return self._rand_string()
        if isinstance(dtype, BinaryType):
            return self._rand_binary()
        if isinstance(dtype, DateType):
            return self._rand_date()
        if isinstance(dtype, TimestampType):
            return self._rand_timestamp()
        if isinstance(dtype, ArrayType):
            # arrays of variable length
            n = self._rand_int(self._min_nested, self._max_nested)
            return [self._random_value_for_type(dtype.elementType) for _ in range(n)]
        if isinstance(dtype, MapType):
            i = 0
            n = self._rand_int(self._min_nested, self._max_nested)
            result = {}
            # Map keys must be hashable and non-null; regenerate if None
            while i < n:
                k = self._random_value_for_type(dtype.keyType)
                v = self._random_value_for_type(dtype.valueType)
                result[k] = v
                i += 1
            return result
        return self._rand_string()

    def _maybe_null(self, value_fn, null_rate: float):
        return None if random.random() < null_rate else value_fn()

    def insert_random_data(
        self,
        spark: SparkSession,
        table: SparkTable,
        n_rows: int,
        null_rate: float = 0.05,
    ):
        """
        Build a DataFrame of random rows for the given schema (types as strings are fine).
        - null_rate: probability any value is None (ignored for map keys)
        """
        # Set limits
        self._min_nested = random.randint(0, 100)
        self._max_nested = max(self._min_nested, random.randint(0, 100))
        self._min_str_len = random.randint(0, 100)
        self._max_str_len = max(self._min_str_len, random.randint(0, 100))

        struct = StructType(
            [
                StructField(name, val.spark_type, True)
                for name, val in table.columns.items()
            ]
        )
        rows = []
        for _ in range(n_rows):
            rec = {}
            for field in struct.fields:
                rec[field.name] = (
                    None
                    if random.random() < null_rate
                    else self._random_value_for_type(field.dataType)
                )
            rows.append(Row(**rec))
        # Use explicit schema so types match exactly
        df = spark.createDataFrame(rows, schema=struct)
        df.write.insertInto(table.table_name, overwrite=False)


class IcebergTableGenerator(LakeTableGenerator):

    def __init__(self, _bucket):
        super().__init__(_bucket)

    def get_format(self) -> str:
        return "iceberg"

    def set_basic_properties(self) -> dict[str, str]:
        properties = {}
        out_format = FileFormat.file_to_str(self.write_format)
        if out_format == "any":
            out_format = random.choice(["parquet", "orc", "avro"])
            self.write_format = FileFormat.file_from_str(out_format)
        properties["write.format.default"] = out_format
        return properties

    def generate_table_properties(
        self, columns: list[dict[str, str]], include_all: bool = False
    ) -> dict[str, str]:
        """
        Generate random Iceberg table properties

        Args:
            include_all: If True, includes all possible properties.
                        If False, randomly selects which properties to include.
        """
        properties = {}

        # File format properties
        if include_all or random.random() > 0.3:
            properties.update(self._generate_format_properties())

        # Compaction properties
        if include_all or random.random() > 0.4:
            properties.update(self._generate_compaction_properties())

        # Snapshot properties
        if include_all or random.random() > 0.5:
            properties.update(self._generate_snapshot_properties())

        # Commit properties
        if include_all or random.random() > 0.4:
            properties.update(self._generate_commit_properties())

        # Manifest properties
        if include_all or random.random() > 0.5:
            properties.update(self._generate_manifest_properties())

        # Metadata properties
        if include_all or random.random() > 0.6:
            properties.update(self._generate_metadata_properties(columns))

        # Write properties
        if include_all or random.random() > 0.3:
            properties.update(self._generate_write_properties())

        # Read properties
        if include_all or random.random() > 0.5:
            properties.update(self._generate_read_properties())

        # Table behavior properties
        if include_all or random.random() > 0.4:
            properties.update(self._generate_behavior_properties())

        return properties

    def _generate_format_properties(self) -> dict[str, str]:
        """Generate file format related properties"""
        properties = {}

        # Default file format
        format_version = random.choice(["1", "2"])
        properties["format-version"] = format_version

        # Parquet specific properties
        if self.write_format == FileFormat.Parquet:
            properties["write.parquet.compression-codec"] = random.choice(
                ["snappy", "gzip", "zstd", "lz4", "brotli", "uncompressed"]
            )
            properties["write.parquet.compression-level"] = str(random.randint(1, 9))
            properties["write.parquet.dict-size-bytes"] = str(
                random.choice([2097152, 4194304, 8388608])  # 2MB, 4MB, 8MB
            )
            properties["write.parquet.page-size-bytes"] = str(
                random.choice([65536, 131072, 1048576])  # 64KB, 128KB, 1MB
            )
            properties["write.parquet.row-group-size-bytes"] = str(
                random.choice([134217728, 268435456, 536870912])  # 128MB, 256MB, 512MB
            )

        # ORC specific properties
        elif self.write_format == FileFormat.ORC:
            properties["write.orc.compression-codec"] = random.choice(
                ["snappy", "zlib", "lzo", "zstd", "none"]
            )
            properties["write.orc.compression-strategy"] = random.choice(
                ["speed", "compression"]
            )
            properties["write.orc.stripe-size-bytes"] = str(
                random.choice([67108864, 134217728, 268435456])  # 64MB, 128MB, 256MB
            )
            properties["write.orc.block-size-bytes"] = str(
                random.choice([262144, 524288, 1048576])  # 256KB, 512KB, 1MB
            )

        # AVRO specific properties
        elif self.write_format == FileFormat.Avro:
            properties["write.avro.compression-codec"] = random.choice(
                ["snappy", "deflate", "bzip2", "xz", "zstandard", "uncompressed"]
            )
            if properties["write.avro.compression-codec"] == "deflate":
                properties["write.avro.compression-level"] = str(random.randint(1, 9))

        return properties

    def _generate_compaction_properties(self) -> dict[str, str]:
        """Generate compaction related properties"""
        properties = {}

        # Target file size
        properties["write.target-file-size-bytes"] = str(
            random.choice(
                [
                    134217728,  # 128MB
                    268435456,  # 256MB
                    536870912,  # 512MB
                    1073741824,  # 1GB
                ]
            )
        )

        # Compaction settings
        properties["commit.manifest.target-size-bytes"] = str(
            random.choice([8388608, 16777216, 33554432])  # 8MB, 16MB, 32MB
        )
        properties["commit.manifest.min-count-to-merge"] = str(
            random.choice([50, 100, 200, 500])
        )
        properties["commit.manifest-merge.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        return properties

    def _generate_snapshot_properties(self) -> dict[str, str]:
        """Generate snapshot management properties"""
        properties = {}

        # Snapshot retention
        properties["history.expire.max-snapshot-age-ms"] = str(
            random.choice(
                [
                    432000000,  # 5 days
                    604800000,  # 7 days
                    1209600000,  # 14 days
                    2592000000,  # 30 days
                ]
            )
        )
        properties["history.expire.min-snapshots-to-keep"] = str(
            random.choice([1, 3, 5, 10])
        )

        # Snapshot references
        properties["history.expire.max-ref-age-ms"] = str(
            random.choice(
                [
                    604800000,  # 7 days
                    1209600000,  # 14 days
                    2592000000,  # 30 days
                    5184000000,  # 60 days
                ]
            )
        )

        return properties

    def _generate_commit_properties(self) -> dict[str, str]:
        """Generate commit related properties"""
        properties = {}

        # Retry settings
        properties["commit.retry.num-retries"] = str(random.randint(3, 10))
        properties["commit.retry.min-wait-ms"] = str(random.choice([100, 500, 1000]))
        properties["commit.retry.max-wait-ms"] = str(
            random.choice([10000, 30000, 60000])
        )
        properties["commit.retry.total-timeout-ms"] = str(
            random.choice([60000, 180000, 300000])  # 1min, 3min, 5min
        )

        # Locking
        properties["commit.lock.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        if properties["commit.lock.enabled"] == "true":
            properties["commit.lock.timeout-ms"] = str(
                random.choice([30000, 60000, 120000])  # 30s, 1min, 2min
            )

        return properties

    def _generate_manifest_properties(self) -> dict[str, str]:
        """Generate manifest file properties"""
        properties = {}

        properties["write.manifest.min-added-files"] = str(
            random.choice([100, 500, 1000, 5000])
        )
        properties["write.manifest.max-added-files"] = str(
            random.choice([10000, 50000, 100000])
        )

        # Manifest list parallelism
        properties["manifest-lists.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        if properties["manifest-lists.enabled"] == "true":
            properties["manifest-lists.parallelism"] = str(
                random.choice([1, 2, 4, 8, 16])
            )

        return properties

    def _generate_metadata_properties(
        self, columns: list[dict[str, str]]
    ) -> dict[str, str]:
        """Generate metadata related properties"""
        properties = {}

        # Metadata deletion
        properties["write.metadata.delete-after-commit.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()
        properties["write.metadata.previous-versions-max"] = str(
            random.choice([3, 5, 10, 20])
        )

        # Metrics collection
        properties["write.metadata.metrics.default"] = random.choice(
            ["none", "counts", "truncate(16)", "full"]
        )

        # Column statistics
        properties[
            f"write.metadata.metrics.column.{random.choice(columns)["name"]}"
        ] = random.choice(["none", "counts", "truncate(8)", "truncate(16)", "full"])

        return properties

    def _generate_write_properties(self) -> dict[str, str]:
        """Generate write operation properties"""
        properties = {}

        # Distribution mode
        properties["write.distribution-mode"] = random.choice(["none", "hash", "range"])

        # Write parallelism
        properties["write.tasks.max"] = str(random.choice([100, 500, 1000, 2000]))
        properties["write.tasks.min"] = str(random.choice([1, 10, 50, 100]))

        # Sorting
        properties["write.sort.enabled"] = str(random.choice(["true", "false"])).lower()

        if properties["write.sort.enabled"] == "true":
            properties["write.sort.order"] = random.choice(
                ["id ASC", "timestamp DESC", "id ASC, timestamp DESC"]
            )

        # Write modes
        properties["write.update.mode"] = random.choice(
            ["copy-on-write", "merge-on-read"]
        )
        properties["write.delete.mode"] = random.choice(
            ["copy-on-write", "merge-on-read"]
        )
        properties["write.merge.mode"] = random.choice(
            ["copy-on-write", "merge-on-read"]
        )

        return properties

    def _generate_read_properties(self) -> dict[str, str]:
        """Generate read operation properties"""
        properties = {}

        # Split size
        properties["read.split.target-size"] = str(
            random.choice([134217728, 268435456, 536870912])  # 128MB  # 256MB  # 512MB
        )
        properties["read.split.metadata-target-size"] = str(
            random.choice([33554432, 67108864, 134217728])  # 32MB  # 64MB  # 128MB
        )
        properties["read.split.planning-lookback"] = str(random.choice([10, 50, 100]))

        # Streaming
        properties["read.stream.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        if properties["read.stream.enabled"] == "true":
            properties["read.stream.skip-delete-snapshots"] = str(
                random.choice(["true", "false"])
            ).lower()
            properties["read.stream.skip-overwrite-snapshots"] = str(
                random.choice(["true", "false"])
            ).lower()

        # Parquet vectorization
        properties["read.parquet.vectorization.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        if properties["read.parquet.vectorization.enabled"] == "true":
            properties["read.parquet.vectorization.batch-size"] = str(
                random.choice([1024, 2048, 4096, 8192])
            )

        return properties

    def _generate_behavior_properties(self) -> dict[str, str]:
        """Generate table behavior properties"""
        properties = {}

        # Compatibility
        properties["compatibility.snapshot-id-inheritance.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        # Schema evolution
        properties["schema.auto-evolve"] = str(random.choice(["true", "false"])).lower()
        properties["schema.name-mapping.default"] = random.choice(["v1", "v2", None])
        if properties["schema.name-mapping.default"]:
            properties["schema.name-mapping.default"] = str(
                properties["schema.name-mapping.default"]
            )
        else:
            del properties["schema.name-mapping.default"]

        # Data locality
        properties["write.data.locality.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        return properties


class DeltaLakePropertiesGenerator(LakeTableGenerator):

    def __init__(self, _bucket):
        super().__init__(_bucket)

    def get_format(self) -> str:
        return "delta"

    def set_basic_properties(self) -> dict[str, str]:
        return {}

    def generate_table_properties(
        self, columns: list[dict[str, str]], include_all: bool = False
    ) -> dict[str, str]:
        """
        Generate random Delta Lake table properties

        Args:
            include_all: If True, includes all possible properties.
                        If False, randomly selects which properties to include.
        """
        properties = {}

        # Data retention and vacuum properties
        if include_all or random.random() > 0.3:
            properties.update(self._generate_retention_properties())

        # Optimization properties
        if include_all or random.random() > 0.4:
            properties.update(self._generate_optimization_properties())

        # Delta cache properties
        if include_all or random.random() > 0.5:
            properties.update(self._generate_cache_properties())

        # Column mapping properties
        if include_all or random.random() > 0.6:
            properties.update(self._generate_column_mapping_properties())

        # File size properties
        if include_all or random.random() > 0.3:
            properties.update(self._generate_file_properties())

        # Checkpoint properties
        if include_all or random.random() > 0.5:
            properties.update(self._generate_checkpoint_properties())

        # Data skipping properties
        if include_all or random.random() > 0.4:
            properties.update(self._generate_data_skipping_properties())

        # Change data feed properties
        if include_all or random.random() > 0.5:
            properties.update(self._generate_cdf_properties())

        # Table features
        if include_all or random.random() > 0.4:
            properties.update(self._generate_table_features())

        return properties

    def _generate_retention_properties(self) -> dict[str, str]:
        """Generate data retention and vacuum related properties"""
        properties = {}

        # Log retention
        properties["delta.logRetentionDuration"] = random.choice(
            [
                "interval 7 days",
                "interval 14 days",
                "interval 30 days",
                "interval 60 days",
                "interval 90 days",
            ]
        )

        # Deleted file retention (for VACUUM)
        properties["delta.deletedFileRetentionDuration"] = random.choice(
            [
                "interval 1 day",
                "interval 7 days",
                "interval 14 days",
                "interval 30 days",
            ]
        )

        # Sample ratio for stats collection
        properties["delta.dataSkippingNumIndexedCols"] = str(
            random.choice([32, 64, 128, 256])
        )

        return properties

    def _generate_optimization_properties(self) -> dict[str, str]:
        """Generate optimization related properties"""
        properties = {}

        # Auto optimize
        properties["delta.autoOptimize.optimizeWrite"] = str(
            random.choice(["true", "false"])
        ).lower()

        properties["delta.autoOptimize.autoCompact"] = str(
            random.choice(["true", "false"])
        ).lower()

        # Optimize write
        properties["spark.databricks.delta.autoCompact.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        # Adaptive shuffle
        properties["spark.databricks.delta.optimizeWrite.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        return properties

    def _generate_cache_properties(self) -> dict[str, str]:
        """Generate Delta cache related properties"""
        properties = {}

        # Delta cache
        properties["spark.databricks.io.cache.enabled"] = str(
            random.choice(["true", "false"])
        ).lower()

        if properties["spark.databricks.io.cache.enabled"] == "true":
            properties["spark.databricks.io.cache.maxDiskUsage"] = random.choice(
                ["10g", "20g", "50g", "100g"]
            )

            properties["spark.databricks.io.cache.maxMetaDataCache"] = random.choice(
                ["1g", "2g", "5g", "10g"]
            )

            properties["spark.databricks.io.cache.compression.enabled"] = str(
                random.choice(["true", "false"])
            ).lower()

        return properties

    def _generate_column_mapping_properties(self) -> dict[str, str]:
        """Generate column mapping properties"""
        properties = {}

        # Column mapping mode
        mapping_mode = random.choice(["none", "name", "id"])
        properties["delta.columnMapping.mode"] = mapping_mode

        # Min reader/writer version based on features
        if mapping_mode != "none":
            properties["delta.minReaderVersion"] = "2"
            properties["delta.minWriterVersion"] = "5"
        else:
            properties["delta.minReaderVersion"] = "1"
            properties["delta.minWriterVersion"] = random.choice(["2", "3", "4"])

        return properties

    def _generate_file_properties(self) -> dict[str, str]:
        """Generate file size and format properties"""
        properties = {}

        # Target file size
        properties["spark.databricks.delta.optimize.maxFileSize"] = random.choice(
            ["64mb", "128mb", "256mb", "512mb", "1gb"]
        )

        # Parquet compression
        properties["spark.sql.parquet.compression.codec"] = random.choice(
            ["snappy", "gzip", "lzo", "brotli", "lz4", "zstd", "uncompressed"]
        )

        # Parquet file size
        properties["spark.databricks.delta.parquet.blockSize"] = str(
            random.choice([134217728, 268435456, 536870912])  # 128MB  # 256MB  # 512MB
        )

        return properties

    def _generate_checkpoint_properties(self) -> dict[str, str]:
        """Generate checkpoint related properties"""
        properties = {}

        # Checkpoint interval
        properties["delta.checkpointInterval"] = str(random.choice([10, 20, 50, 100]))

        # Checkpoint retention
        properties["delta.checkpointRetentionDuration"] = random.choice(
            ["interval 2 days", "interval 7 days", "interval 30 days"]
        )

        # Compatibility
        if "delta.enableDeletionVectors" not in properties:
            properties["delta.compatibility.symlinkFormatManifest.enabled"] = str(
                random.choice(["true", "false"])
            ).lower()

        return properties

    def _generate_data_skipping_properties(self) -> dict[str, str]:
        """Generate data skipping and statistics properties"""
        properties = {}

        # Statistics columns
        properties["delta.dataSkippingNumIndexedCols"] = str(
            random.choice([32, 64, 128, 256])
        )

        # Statistics collection
        properties["delta.checkpoint.writeStatsAsJson"] = str(
            random.choice(["true", "false"])
        ).lower()

        properties["delta.checkpoint.writeStatsAsStruct"] = str(
            random.choice(["true", "false"])
        ).lower()

        # Sampling for stats
        properties["spark.databricks.delta.stats.skipping"] = str(
            random.choice(["true", "false"])
        ).lower()

        return properties

    def _generate_cdf_properties(self) -> dict[str, str]:
        """Generate Change Data Feed properties"""
        properties = {}

        # Enable CDF
        cdf_enabled = random.choice(["true", "false"])
        properties["delta.enableChangeDataFeed"] = cdf_enabled.lower()

        if cdf_enabled == "true":
            # CDF requires specific versions
            properties["delta.minReaderVersion"] = "1"
            properties["delta.minWriterVersion"] = "4"

        return properties

    def _generate_table_features(self) -> dict[str, str]:
        """Generate table feature properties (Delta 3.0+)"""
        properties = {}

        # Append-only table
        properties["delta.appendOnly"] = str(random.choice(["true", "false"])).lower()

        # Enable deletion vectors (Delta 3.0+)
        if "delta.compatibility.symlinkFormatManifest.enabled" not in properties:
            properties["delta.enableDeletionVectors"] = str(
                random.choice(["true", "false"])
            ).lower()

        # Row tracking
        properties["delta.enableRowTracking"] = str(
            random.choice(["true", "false"])
        ).lower()

        # Type widening
        properties["delta.enableTypeWidening"] = str(
            random.choice(["true", "false"])
        ).lower()

        # Timestamp NTZ support
        properties["delta.feature.timestampNtz"] = str(
            random.choice(["supported", "enabled"])
        )

        return properties
