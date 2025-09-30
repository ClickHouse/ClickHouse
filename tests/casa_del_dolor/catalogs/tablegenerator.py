import random
from abc import abstractmethod

from .clickhousetospark import ClickHouseSparkTypeMapper
from .laketables import (
    LakeFormat,
    SparkTable,
    FileFormat,
    TableStorage,
    SparkColumn,
)

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType, StructType, DataType


class LakeTableGenerator:
    def __init__(self):
        self.type_mapper = ClickHouseSparkTypeMapper()
        self.write_format: FileFormat = FileFormat.Parquet

    @staticmethod
    def get_next_generator(lake: LakeFormat):
        return (
            IcebergTableGenerator()
            if lake == LakeFormat.Iceberg
            else DeltaLakePropertiesGenerator()
        )

    @abstractmethod
    def generate_table_properties(
        self,
        table: SparkTable,
        include_all: bool = False,
    ) -> dict[str, str]:
        pass

    @abstractmethod
    def get_format(self) -> str:
        pass

    @abstractmethod
    def set_table_location(self, next_location: str) -> str:
        return ""

    @abstractmethod
    def set_basic_properties(self) -> dict[str, str]:
        return {}

    @abstractmethod
    def add_partition_clauses(self, table: SparkTable) -> list[str]:
        return []

    def random_ordered_columns(self, table: SparkTable, with_asc_desc: bool):
        columns_list = []
        flattened_columns = table.flat_columns()
        for k in flattened_columns.keys():
            if with_asc_desc:
                columns_list.append(f"{k} ASC NULLS FIRST")
                columns_list.append(f"{k} ASC NULLS LAST")
                columns_list.append(f"{k} DESC NULLS FIRST")
                columns_list.append(f"{k} DESC NULLS LAST")
            else:
                columns_list.append(f"{k}")
        random_subset = random.sample(
            columns_list, k=random.randint(1, min(4, len(columns_list)))
        )
        random.shuffle(random_subset)
        return ",".join(random_subset)

    def generate_create_table_ddl(
        self,
        catalog_name: str,
        database_name: str,
        table_name: str,
        columns: list[dict[str, str]],
        file_format: str,
        deterministic: bool,
        next_storage: TableStorage,
        next_location: str,
    ) -> tuple[str, SparkTable]:
        """
        Generate a complete CREATE TABLE DDL statement with random properties

        Args:
            table_name: Name of the table
        """
        self.write_format = FileFormat.file_from_str(file_format)

        ddl = f"CREATE TABLE IF NOT EXISTS {catalog_name}.test.{table_name} ("
        columns_def = []
        columns_spark = {}
        for val in columns:
            # Convert columns
            str_type, nullable, spark_type = self.type_mapper.clickhouse_to_spark(
                val["type"], False
            )
            columns_def.append(
                f"{val["name"]} {str_type}{"" if nullable else " NOT NULL"}"
            )
            columns_spark[val["name"]] = SparkColumn(val["name"], spark_type, nullable)
        ddl += ",".join(columns_def)
        ddl += ")"

        # Add USING clause
        ddl += f" USING {self.get_format()}"

        res = SparkTable(
            catalog_name,
            database_name,
            table_name,
            columns_spark,
            deterministic,
            next_location,
            LakeFormat.lakeformat_from_str(self.get_format()),
            self.write_format,
            next_storage,
        )

        # Add Partition by, can't partition by all columns
        if random.randint(1, 5) == 1:
            partition_clauses = self.add_partition_clauses(res)
            random.shuffle(partition_clauses)
            random_subset = random.sample(
                partition_clauses, k=random.randint(1, min(3, len(partition_clauses)))
            )
            ddl += f" PARTITIONED BY ({",".join(random_subset)})"

        ddl += self.set_table_location(next_location)

        properties = self.set_basic_properties()
        # Add table properties
        if random.randint(1, 2) == 1:
            properties.update(self.generate_table_properties(res))
        if len(properties) > 0:
            ddl += " TBLPROPERTIES ("
            prop_lines = []
            for key, value in properties.items():
                prop_lines.append(f"'{key}' = '{value}'")
            ddl += ",".join(prop_lines)
            ddl += ")"
        return (ddl + ";", res)

    def generate_alter_table_statements(
        self,
        table: SparkTable,
    ) -> str:
        """Generate random ALTER TABLE statements for testing"""
        next_operation = random.randint(
            1, 500 if self.get_format() == "delta" else 1000
        )

        if next_operation <= 250:
            # Set random properties
            properties = self.generate_table_properties(table)
            if properties:
                key = random.choice(list(properties.keys()))
                return f"ALTER TABLE {table.get_table_full_path()} SET TBLPROPERTIES ('{key}' = '{properties[key]}');"
        elif next_operation <= 500:
            # Unset a property
            properties = self.generate_table_properties(table)
            if properties:
                key = random.choice(list(properties.keys()))
                return f"ALTER TABLE {table.get_table_full_path()} UNSET TBLPROPERTIES ('{key}');"
        elif next_operation <= 600:
            # Add or drop partition field
            partition_clauses = self.add_partition_clauses(table)
            random.shuffle(partition_clauses)
            random_subset = random.sample(
                partition_clauses, k=random.randint(1, min(3, len(partition_clauses)))
            )
            return f"ALTER TABLE {table.get_table_full_path()} {random.choice(["ADD", "DROP"])} PARTITION FIELD {random.choice(list(random_subset))}"
        elif next_operation <= 700:
            # Replace partition field
            partition_clauses = self.add_partition_clauses(table)
            random.shuffle(partition_clauses)
            random_subset1 = random.sample(
                partition_clauses, k=random.randint(1, min(3, len(partition_clauses)))
            )
            random.shuffle(partition_clauses)
            random_subset2 = random.sample(
                partition_clauses, k=random.randint(1, min(3, len(partition_clauses)))
            )
            return f"ALTER TABLE {table.get_table_full_path()} REPLACE PARTITION FIELD {random.choice(list(random_subset1))} WITH {random.choice(list(random_subset2))}"
        elif next_operation <= 800:
            # Set ORDER BY
            if random.randint(1, 2) == 1:
                return f"ALTER TABLE {table.get_table_full_path()} WRITE UNORDERED"
            return f"ALTER TABLE {table.get_table_full_path()} WRITE{random.choice([" LOCALLY", ""])} ORDERED BY {self.random_ordered_columns(table, True)}"
        elif next_operation <= 900:
            # Set distribution
            if random.randint(1, 2) == 1:
                return f"ALTER TABLE {table.get_table_full_path()} WRITE DISTRIBUTED BY PARTITION"
            return f"ALTER TABLE {table.get_table_full_path()} WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY {self.random_ordered_columns(table, True)}"
        elif next_operation <= 1000:
            # Set identifier fields
            return f"ALTER TABLE {table.get_table_full_path()} {random.choice(["SET", "DROP"])} IDENTIFIER FIELDS {self.random_ordered_columns(table, False)}"
        return ""

    @abstractmethod
    def generate_extra_statement(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        return ""


class IcebergTableGenerator(LakeTableGenerator):

    def __init__(self):
        super().__init__()

    def get_format(self) -> str:
        return "iceberg"

    def set_table_location(self, next_location: str) -> str:
        return ""

    def set_basic_properties(self) -> dict[str, str]:
        properties = {}
        out_format = FileFormat.file_to_str(self.write_format)
        if out_format.lower() == "any":
            out_format = random.choice(["parquet", "orc", "avro"])
            self.write_format = FileFormat.file_from_str(out_format)
        properties["write.format.default"] = out_format
        return properties

    def add_partition_clauses(self, table: SparkTable) -> list[str]:
        res = []
        flattened_columns = table.flat_columns()
        for k, val in flattened_columns.items():
            res.append(k)
            if (
                isinstance(val, TimestampType)
                or isinstance(val, DateType)
                or random.randint(0, 9) == 0
            ):
                res.append(f"year({k})")
                res.append(f"month({k})")
                res.append(f"day({k})")
                res.append(f"hour({k})")
            res.append(f"bucket({random.randint(0, 1000)}, {k})")
            res.append(f"truncate({random.randint(0, 1000)}, {k})")
        return res

    def generate_table_properties(
        self,
        table: SparkTable,
        include_all: bool = False,
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
            properties.update(self._generate_metadata_properties(table))

        # Write properties
        if include_all or random.random() > 0.3:
            properties.update(self._generate_write_properties(table))

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
        properties["format-version"] = random.choice(["1", "2"])

        # Parquet specific properties
        if self.write_format == FileFormat.Parquet:
            properties["write.parquet.compression-codec"] = random.choice(
                ["snappy", "gzip", "zstd", "lz4", "uncompressed"]
            )
            properties["write.parquet.compression-level"] = str(random.randint(1, 9))
            properties["write.parquet.dict-size-bytes"] = str(
                random.choice(
                    [1048576, 2097152, 4194304, 8388608]
                )  # 1MB, 2MB, 4MB, 8MB
            )
            properties["write.parquet.page-size-bytes"] = str(
                random.choice(
                    [1048576, 2097152, 4194304, 8388608]
                )  # 1MB, 2MB, 4MB, 8MB
            )
            properties["write.parquet.row-group-size-bytes"] = str(
                random.choice(
                    [1048576, 2097152, 4194304, 8388608]
                )  # 1MB, 2MB, 4MB, 8MB
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
                random.choice(
                    [1048576, 2097152, 4194304, 8388608]
                )  # 1MB, 2MB, 4MB, 8MB
            )
            properties["write.orc.block-size-bytes"] = str(
                random.choice(
                    [1048576, 2097152, 4194304, 8388608]
                )  # 1MB, 2MB, 4MB, 8MB
            )

        # AVRO specific properties
        elif self.write_format == FileFormat.Avro:
            properties["write.avro.compression-codec"] = random.choice(
                ["snappy", "bzip2", "xz", "uncompressed"]
            )

        return properties

    def _generate_compaction_properties(self) -> dict[str, str]:
        """Generate compaction related properties"""
        properties = {}

        # Target file size
        properties["write.target-file-size-bytes"] = str(
            random.choice(
                [
                    1048576,  # 1MB
                    2097152,  # 2MB
                    134217728,  # 128MB
                    268435456,  # 256MB
                    536870912,  # 512MB
                    1073741824,  # 1GB
                ]
            )
        )

        # Compaction settings
        properties["commit.manifest.target-size-bytes"] = str(
            random.choice(
                [1048576, 2097152, 4194304, 8388608, 16777216, 33554432]
            )  # 1MB, 2MB, 4MB, 8MB, 16MB, 32MB
        )
        properties["commit.manifest.min-count-to-merge"] = str(
            random.choice([1, 2, 8, 50, 100, 200, 500])
        )
        properties["commit.manifest-merge.enabled"] = random.choice(["true", "false"])

        return properties

    def _generate_snapshot_properties(self) -> dict[str, str]:
        """Generate snapshot management properties"""
        properties = {}

        # Snapshot retention
        properties["history.expire.max-snapshot-age-ms"] = str(
            random.choice(
                [
                    1000,  # 1 second
                    60000,  # 1 minute
                    3600000,  # 1 hour
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
                    1000,  # 1 second
                    60000,  # 1 minute
                    3600000,  # 1 hour
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
            random.choice([1000, 10000, 30000, 60000])
        )
        properties["commit.retry.total-timeout-ms"] = str(
            random.choice([1000, 60000, 180000, 300000])
        )

        # Locking
        properties["commit.lock.enabled"] = random.choice(["true", "false"])

        if properties["commit.lock.enabled"] == "true":
            properties["commit.lock.timeout-ms"] = str(
                random.choice([1000, 30000, 60000, 120000])
            )

        return properties

    def _generate_manifest_properties(self) -> dict[str, str]:
        """Generate manifest file properties"""
        properties = {}

        properties["write.manifest.min-added-files"] = str(
            random.choice([1, 8, 16, 100, 500, 1000, 5000])
        )
        properties["write.manifest.max-added-files"] = str(
            random.choice([1, 100, 1000, 10000, 50000, 100000])
        )

        # Manifest list parallelism
        properties["manifest-lists.enabled"] = random.choice(["true", "false"])

        if properties["manifest-lists.enabled"] == "true":
            properties["manifest-lists.parallelism"] = str(
                random.choice([1, 2, 4, 8, 16])
            )

        return properties

    def _generate_metadata_properties(self, table: SparkTable) -> dict[str, str]:
        """Generate metadata related properties"""
        properties = {}

        # Metadata deletion
        properties["write.metadata.delete-after-commit.enabled"] = random.choice(
            ["true", "false"]
        )
        properties["write.metadata.previous-versions-max"] = str(
            random.choice([1, 2, 3, 5, 10, 20])
        )

        # Metrics collection
        properties["write.metadata.metrics.default"] = random.choice(
            ["none", "counts", "truncate(16)", "full"]
        )

        # Column statistics
        properties[
            f"write.metadata.metrics.column.{random.choice(list(table.columns.keys()))}"
        ] = random.choice(["none", "counts", "truncate(8)", "truncate(16)", "full"])

        return properties

    def _generate_write_properties(self, table: SparkTable) -> dict[str, str]:
        """Generate write operation properties"""
        properties = {}

        # Distribution mode
        properties["write.distribution-mode"] = random.choice(["none", "hash", "range"])

        # Write parallelism
        properties["write.tasks.max"] = str(
            random.choice([1, 2, 8, 100, 500, 1000, 2000])
        )
        properties["write.tasks.min"] = str(random.choice([1, 2, 10, 50, 100]))

        # Sorting
        properties["write.sort.enabled"] = random.choice(["true", "false"])

        if properties["write.sort.enabled"] == "true":
            properties["write.sort.order"] = self.random_ordered_columns(table, True)

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
            random.choice(
                [1048576, 2097152, 4194304, 8388608, 134217728, 268435456, 536870912]
            )  # 128MB  # 256MB  # 512MB
        )
        properties["read.split.metadata-target-size"] = str(
            random.choice(
                [1048576, 2097152, 4194304, 8388608, 33554432, 67108864, 134217728]
            )  # 32MB  # 64MB  # 128MB
        )
        properties["read.split.planning-lookback"] = str(random.choice([10, 50, 100]))

        # Streaming
        properties["read.stream.enabled"] = random.choice(["true", "false"])

        if properties["read.stream.enabled"] == "true":
            properties["read.stream.skip-delete-snapshots"] = random.choice(
                ["true", "false"]
            )
            properties["read.stream.skip-overwrite-snapshots"] = random.choice(
                ["true", "false"]
            )

        # Parquet vectorization
        properties["read.parquet.vectorization.enabled"] = random.choice(
            ["true", "false"]
        )

        if properties["read.parquet.vectorization.enabled"] == "true":
            properties["read.parquet.vectorization.batch-size"] = str(
                random.choice([1, 16, 32, 128, 1024, 2048, 4096, 8192])
            )

        return properties

    def _generate_behavior_properties(self) -> dict[str, str]:
        """Generate table behavior properties"""
        properties = {}

        # Compatibility
        properties["compatibility.snapshot-id-inheritance.enabled"] = random.choice(
            ["true", "false"]
        )

        # Schema evolution
        properties["schema.auto-evolve"] = random.choice(["true", "false"])

        # Data locality
        properties["write.data.locality.enabled"] = random.choice(["true", "false"])

        return properties

    def get_snapshots(self, spark: SparkSession, table: SparkTable):
        result = spark.sql(
            f"SELECT snapshot_id FROM {table.get_table_full_path()}.snapshots;"
        ).collect()
        return [r.snapshot_id for r in result]

    def get_timestamps(self, spark: SparkSession, table: SparkTable):
        result = spark.sql(
            f"SELECT made_current_at FROM {table.get_table_full_path()}.history;"
        ).collect()
        return [r.made_current_at for r in result]

    def generate_extra_statement(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        next_option = random.randint(1, 13)

        if next_option == 1:
            res = f"CALL `{table.catalog_name}`.system.remove_orphan_files(table => '{table.get_namespace_path()}'"
            if random.randint(1, 2) == 1:
                res += f", dry_run => {random.choice(["true", "false"])}"
            timestamps = self.get_timestamps(spark, table)
            if len(timestamps) > 0 and random.randint(1, 2) == 1:
                res += f", older_than => TIMESTAMP '{random.choice(timestamps)}'"
            res += ")"
            return res
        if next_option == 2:
            next_strategy = random.choice(["sort", "binpack"])

            res = f"CALL `{table.catalog_name}`.system.rewrite_data_files(table => '{table.get_namespace_path()}', strategy => '{next_strategy}'"
            if next_strategy == "sort" and random.randint(1, 4) != 4:
                zorder = random.randint(1, 2) == 1
                res += ", sort_order => '"
                if zorder:
                    res += "zorder("
                res += self.random_ordered_columns(table, not zorder)
                if zorder:
                    res += ")"
                res += "'"
            res += ")"
            return res
        if next_option == 3:
            res = f"CALL `{table.catalog_name}`.system.rewrite_manifests(table => '{table.get_namespace_path()}'"
            if random.randint(1, 2) == 1:
                res += f", use_caching => {random.choice(["true", "false"])}"
            res += ")"
            return res
        if next_option == 4:
            return f"CALL `{table.catalog_name}`.system.rewrite_position_delete_files('{table.get_namespace_path()}')"
        if next_option == 5:
            res = f"CALL `{table.catalog_name}`.system.expire_snapshots(table => '{table.get_namespace_path()}'"
            timestamps = self.get_timestamps(spark, table)
            if len(timestamps) > 0 and random.randint(1, 2) == 1:
                res += f", older_than => TIMESTAMP '{random.choice(timestamps)}'"
            if random.randint(1, 2) == 1:
                res += f", stream_results => {random.choice(["true", "false"])}"
            if random.randint(1, 2) == 1:
                res += f", retain_last => {random.randint(1, 10)}"
            res += ")"
            return res
        if next_option == 6:
            res = f"CALL `{table.catalog_name}`.system.compute_table_stats(table => '{table.get_namespace_path()}'"
            snapshots = self.get_snapshots(spark, table)
            if len(snapshots) > 0 and random.randint(1, 2) == 1:
                res += f", snapshot_id  => {random.choice(snapshots)}"
            res += ")"
            return res
        if next_option == 7:
            res = f"CALL `{table.catalog_name}`.system.compute_partition_stats(table => '{table.get_namespace_path()}'"
            snapshots = self.get_snapshots(spark, table)
            if len(snapshots) > 0 and random.randint(1, 2) == 1:
                res += f", snapshot_id  => {random.choice(snapshots)}"
            res += ")"
            return res
        if next_option == 8:
            return f"CALL `{table.catalog_name}`.system.ancestors_of('{table.get_namespace_path()}')"
        snapshots = self.get_snapshots(spark, table)
        if len(snapshots) > 0 and next_option in (9, 10, 11):
            calls = [
                "rollback_to_snapshot",
                "set_current_snapshot",
                "cherrypick_snapshot",
            ]
            return f"CALL `{table.catalog_name}`.system.{random.choice(calls)}(table => '{table.get_namespace_path()}', snapshot_id => {random.choice(snapshots)})"
        timestamps = self.get_timestamps(spark, table)
        if len(timestamps) > 0 and next_option in (12, 13):
            return f"CALL `{table.catalog_name}`.system.rollback_to_timestamp(table => '{table.get_namespace_path()}', timestamp => TIMESTAMP '{random.choice(timestamps)}')"
        return ""


class DeltaLakePropertiesGenerator(LakeTableGenerator):

    def __init__(self):
        super().__init__()

    def get_format(self) -> str:
        return "delta"

    def set_table_location(self, next_location: str) -> str:
        return f" LOCATION '{next_location}'"

    def set_basic_properties(self) -> dict[str, str]:
        return {}

    def add_partition_clauses(self, table: SparkTable) -> list[str]:
        res = []
        # No partition by subcolumns in delta
        for k in table.columns.keys():
            res.append(k)
        return res

    def generate_table_properties(
        self,
        table: SparkTable,
        include_all: bool = False,
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
                "interval 1 second",
                "interval 1 minute",
                "interval 1 hour",
            ]
        )

        # Deleted file retention (for VACUUM)
        properties["delta.deletedFileRetentionDuration"] = random.choice(
            [
                "interval 1 second",
                "interval 1 minute",
                "interval 1 hour",
            ]
        )

        # Sample ratio for stats collection
        properties["delta.dataSkippingNumIndexedCols"] = str(
            random.choice([1, 8, 16, 32, 64, 128, 256])
        )

        return properties

    def _generate_optimization_properties(self) -> dict[str, str]:
        """Generate optimization related properties"""
        properties = {}

        # Auto optimize
        properties["delta.autoOptimize.optimizeWrite"] = random.choice(
            ["true", "false"]
        )

        properties["delta.autoOptimize.autoCompact"] = random.choice(["true", "false"])

        # Optimize write
        properties["spark.databricks.delta.autoCompact.enabled"] = random.choice(
            ["true", "false"]
        )

        # Adaptive shuffle
        properties["spark.databricks.delta.optimizeWrite.enabled"] = random.choice(
            ["true", "false"]
        )

        return properties

    def _generate_cache_properties(self) -> dict[str, str]:
        """Generate Delta cache related properties"""
        properties = {}

        # Delta cache
        properties["spark.databricks.io.cache.enabled"] = random.choice(
            ["true", "false"]
        )

        if properties["spark.databricks.io.cache.enabled"] == "true":
            properties["spark.databricks.io.cache.maxDiskUsage"] = random.choice(
                ["10g", "20g", "50g", "100g"]
            )

            properties["spark.databricks.io.cache.maxMetaDataCache"] = random.choice(
                ["1g", "2g", "5g", "10g"]
            )

            properties["spark.databricks.io.cache.compression.enabled"] = random.choice(
                ["true", "false"]
            )

        return properties

    def _generate_column_mapping_properties(self) -> dict[str, str]:
        """Generate column mapping properties"""
        properties = {}

        # Column mapping mode
        mapping_mode = random.choice(["none", "name", "id"])
        properties["delta.columnMapping.mode"] = mapping_mode

        # Set minimum versions for readers and writers
        properties["delta.minReaderVersion"] = random.choice(
            [f"{i}" for i in range(1, 4)]
        )
        properties["delta.minWriterVersion"] = random.choice(
            [f"{i}" for i in range(1, 8)]
        )

        return properties

    def _generate_file_properties(self) -> dict[str, str]:
        """Generate file size and format properties"""
        properties = {}

        # Target file size
        properties["spark.databricks.delta.optimize.maxFileSize"] = random.choice(
            ["1mb", "2mb", "8mb", "64mb", "128mb", "256mb", "512mb", "1gb"]
        )

        # Parquet compression
        properties["spark.sql.parquet.compression.codec"] = random.choice(
            ["snappy", "gzip", "lzo", "lz4", "zstd", "uncompressed"]
        )

        # Parquet file size
        properties["spark.databricks.delta.parquet.blockSize"] = str(
            random.choice([134217728, 268435456, 536870912])  # 128MB  # 256MB  # 512MB
        )

        return properties

    def _generate_data_skipping_properties(self) -> dict[str, str]:
        """Generate data skipping and statistics properties"""
        properties = {}

        # Statistics columns
        properties["delta.dataSkippingNumIndexedCols"] = str(
            random.choice([1, 8, 16, 32, 64, 128, 256])
        )

        # Statistics collection
        properties["delta.checkpoint.writeStatsAsJson"] = random.choice(
            ["true", "false"]
        )

        properties["delta.checkpoint.writeStatsAsStruct"] = random.choice(
            ["true", "false"]
        )

        # Sampling for stats
        properties["spark.databricks.delta.stats.skipping"] = random.choice(
            ["true", "false"]
        )

        return properties

    def _generate_cdf_properties(self) -> dict[str, str]:
        """Generate Change Data Feed properties"""
        properties = {}

        # Enable CDF
        cdf_enabled = random.choice(["true", "false"])
        properties["delta.enableChangeDataFeed"] = cdf_enabled

        if cdf_enabled == "true":
            # CDF requires specific versions
            properties["delta.minReaderVersion"] = "1"
            properties["delta.minWriterVersion"] = "4"

        return properties

    def _generate_table_features(self) -> dict[str, str]:
        """Generate table feature properties (Delta 3.0+)"""
        properties = {}

        # Append-only table
        properties["delta.appendOnly"] = random.choice(["true", "false"])

        # Isolation level
        properties["delta.isolationLevel"] = str(
            random.choice(["Serializable"])  # , "WriteSerializable" is not supported
        )

        # Checkpoint interval
        properties["delta.checkpointInterval"] = str(
            random.choice([1, 5, 10, 20, 50, 100])
        )

        # Checkpoint retention
        properties["delta.checkpointRetentionDuration"] = random.choice(
            [
                "interval 1 second",
                "interval 1 minute",
                "interval 1 day",
            ]
        )

        # Compatibility
        properties["delta.compatibility.symlinkFormatManifest.enabled"] = random.choice(
            ["true", "false"]
        )

        # Enable deletion vectors (Delta 3.0+)
        if "delta.compatibility.symlinkFormatManifest.enabled" == "false":
            properties["delta.enableDeletionVectors"] = random.choice(["true", "false"])

        # Row tracking
        properties["delta.enableRowTracking"] = random.choice(["true", "false"])

        # Type widening
        properties["delta.enableTypeWidening"] = random.choice(["true", "false"])

        # Timestamp NTZ support
        properties["delta.feature.timestampNtz"] = random.choice(
            ["supported", "enabled"]
        )

        return properties

    def generate_extra_statement(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        next_option = random.randint(1, 4)

        if next_option == 1:
            # Vacuum
            return f"VACUUM {table.get_table_full_path()} RETAIN 0 HOURS;"
        if next_option == 2:
            # Optimize
            return f"OPTIMIZE {table.get_table_full_path()}{f" ZORDER BY ({self.random_ordered_columns(table, False)})" if random.randint(1, 2) == 1 else ""};"
        if next_option in (3, 4):
            # Restore
            result = spark.sql(
                f"DESCRIBE HISTORY {table.get_table_full_path()};"
            ).collect()
            snapshots = [r.version for r in result]
            timestamps = [r.timestamp for r in result]

            if len(snapshots) > 0 and (
                len(timestamps) == 0 or random.randint(1, 2) == 1
            ):
                return f"RESTORE TABLE {table.get_table_full_path()} TO VERSION AS OF {random.choice(snapshots)};"
            if len(timestamps) > 0:
                return f"RESTORE TABLE {table.get_table_full_path()} TO TIMESTAMP AS OF '{random.choice(timestamps)}';"
            return f"RESTORE TABLE {table.get_table_full_path()} TO VERSION AS OF 1;"
        return ""
