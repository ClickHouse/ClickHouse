import random
import typing
from abc import abstractmethod

from .clickhousetospark import ClickHouseMapping, ClickHouseTypeMapper
from .laketables import (
    LakeFormat,
    SparkTable,
    FileFormat,
    TableStorage,
    SparkColumn,
    LakeCatalogs,
)

from pyspark.sql import SparkSession
from pyiceberg.schema import Schema
import pyspark.sql.types as sp
import pyiceberg.types as it

Parameter = typing.Callable[[], int | float | str]

true_false_lambda = lambda: random.choice(["false", "true"])


def sample_from_dict(d: dict[str, Parameter], sample: int) -> dict[str, Parameter]:
    items = random.sample(list(d.items()), sample)
    return dict(items)


class LakeTableGenerator:
    def __init__(self):
        self.type_mapper = ClickHouseTypeMapper()
        self.write_format: FileFormat = FileFormat.Parquet

    @staticmethod
    def get_next_generator(lake: LakeFormat):
        return (
            IcebergTableGenerator()
            if lake == LakeFormat.Iceberg
            else DeltaLakePropertiesGenerator()
        )

    @abstractmethod
    def generate_table_properties_impl(
        self,
        table: SparkTable,
    ) -> dict[str, Parameter]:
        pass

    def generate_table_properties(
        self,
        table: SparkTable,
    ) -> dict[str, str]:
        properties = {}
        props = self.generate_table_properties_impl(table)
        selected_properties = sample_from_dict(props, random.randint(0, len(props)))
        for key, val in selected_properties.items():
            properties[key] = val()
        return properties

    @abstractmethod
    def get_format(self) -> str:
        pass

    @abstractmethod
    def set_basic_properties(self) -> dict[str, str]:
        return {}

    @abstractmethod
    def add_partition_clauses(self, table: SparkTable) -> list[str]:
        return []

    @abstractmethod
    def add_generated_col(
        self, columns: dict[str, SparkColumn], col: sp.DataType
    ) -> str:
        return ""

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
        next_catalog: LakeCatalogs,
    ) -> tuple[str, SparkTable]:
        """
        Generate a complete CREATE TABLE DDL statement with random properties

        Args:
            table_name: Name of the table
        """
        self.write_format = (
            FileFormat.file_from_str(random.choice(["parquet", "orc", "avro"]))
            if not deterministic and random.randint(1, 11) == 1
            else FileFormat.file_from_str(file_format)
        )
        first = True

        ddl = f"CREATE TABLE IF NOT EXISTS {catalog_name}.test.{table_name} ("
        columns_def = []
        columns_spark = {}
        self.type_mapper.reset()

        # Add a random column with a complex type to increase variety, but only for non-deterministic tables to avoid issues with schema inference in tests
        if not deterministic and random.randint(1, 11) == 1:
            for i in range(0, random.randint(1, 3)):
                columns.append(
                    {
                        "name": f"c4{i}",
                        "type": self.type_mapper.generate_random_clickhouse_type(
                            True, True, random.randint(1, 4), 0
                        ),
                    }
                )

        for val in columns:
            # Sometimes skip columns to create more variety in partitioning and properties
            if not first and not deterministic and random.randint(1, 11) == 1:
                continue
            # Convert columns
            next_ch_type = val["type"]
            # Sometimes use something random
            if not deterministic and random.randint(1, 5) == 1:
                next_ch_type = self.type_mapper.generate_random_clickhouse_type(
                    True, True, random.randint(1, 4), 0
                )

            self.type_mapper.increment()
            str_type, nullable, spark_type = self.type_mapper.clickhouse_to_spark(
                next_ch_type, False, ClickHouseMapping.Spark
            )
            generated = self.add_generated_col(columns_spark, spark_type)
            columns_def.append(
                f"{val['name']} {str_type}{'' if nullable else ' NOT NULL'}{generated}"
            )
            columns_spark[val["name"]] = SparkColumn(
                val["name"], spark_type, nullable, len(generated) > 0
            )
            first = False
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
            LakeFormat.lakeformat_from_str(self.get_format()),
            self.write_format,
            next_storage,
            next_catalog,
        )

        # Add Partition by, can't partition by all columns
        if random.randint(1, 5) == 1:
            partition_clauses = self.add_partition_clauses(res)
            random.shuffle(partition_clauses)
            random_subset = random.sample(
                partition_clauses, k=random.randint(1, min(3, len(partition_clauses)))
            )
            ddl += f" PARTITIONED BY ({','.join(random_subset)})"

        # ddl += self.set_table_location(next_location) no location needed yet

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

    @abstractmethod
    def create_catalog_table(
        self,
        catalog_impl,
        columns: list[dict[str, str]],
        table: SparkTable,
    ) -> str:
        return ""

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
            return f"ALTER TABLE {table.get_table_full_path()} {random.choice(['ADD', 'DROP'])} PARTITION FIELD {random.choice(list(random_subset))}"
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
            return f"ALTER TABLE {table.get_table_full_path()} WRITE{random.choice([' LOCALLY', ''])} ORDERED BY {self.random_ordered_columns(table, True)}"
        elif next_operation <= 900:
            # Set distribution
            if random.randint(1, 2) == 1:
                return f"ALTER TABLE {table.get_table_full_path()} WRITE DISTRIBUTED BY PARTITION"
            return f"ALTER TABLE {table.get_table_full_path()} WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY {self.random_ordered_columns(table, True)}"
        elif next_operation <= 1000:
            # Set identifier fields
            return f"ALTER TABLE {table.get_table_full_path()} {random.choice(['SET', 'DROP'])} IDENTIFIER FIELDS {self.random_ordered_columns(table, False)}"
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
                isinstance(val, sp.TimestampType)
                or isinstance(val, sp.DateType)
                or random.randint(0, 9) == 0
            ):
                res.append(f"year({k})")
                res.append(f"month({k})")
                res.append(f"day({k})")
                res.append(f"hour({k})")
            res.append(f"bucket({random.randint(0, 1000)}, {k})")
            res.append(f"truncate({random.randint(0, 1000)}, {k})")
        return res

    def add_generated_col(
        self, columns: dict[str, SparkColumn], col: sp.DataType
    ) -> str:
        return ""

    def create_catalog_table(
        self,
        catalog_impl,
        columns: list[dict[str, str]],
        table: SparkTable,
    ) -> str:
        nproperties = self.set_basic_properties()
        fields = []
        first = True

        # Add a random column with a complex type to increase variety, but only for non-deterministic tables to avoid issues with schema inference in tests
        if not table.deterministic and random.randint(1, 11) == 1:
            for i in range(0, random.randint(1, 3)):
                columns.append(
                    {
                        "name": f"c4{i}",
                        "type": self.type_mapper.generate_random_clickhouse_type(
                            True, True, random.randint(1, 4), 0
                        ),
                    }
                )

        self.type_mapper.reset()
        for val in columns:
            # Sometimes skip columns to create more variety in partitioning and properties
            if not first and not table.deterministic and random.randint(1, 11) == 1:
                continue
            # Convert columns
            next_field_id = self.type_mapper.field_id
            next_ch_type = val["type"]
            if not table.deterministic and random.randint(1, 5) == 1:
                next_ch_type = self.type_mapper.generate_random_clickhouse_type(
                    True, True, random.randint(1, 4), 0
                )

            _, nullable, iceberg_type = self.type_mapper.clickhouse_to_spark(
                next_ch_type, False, ClickHouseMapping.Iceberg
            )
            fields.append(
                it.NestedField(
                    field_id=next_field_id,
                    name=val["name"],
                    field_type=iceberg_type,
                    required=not nullable,
                )
            )
            self.type_mapper.increment()
            first = False
        nschema = Schema(*fields)

        if random.randint(1, 2) == 1:
            nproperties.update(self.generate_table_properties(table))
        ctable = catalog_impl.create_table(
            identifier=("test", table.table_name),
            location=f"s3{'a' if table.catalog == LakeCatalogs.Hive else ''}://warehouse-{'rest' if table.catalog == LakeCatalogs.REST else ('hms' if table.catalog == LakeCatalogs.Hive else 'glue')}/data",
            schema=nschema,
            partition_spec=self.type_mapper.generate_random_iceberg_partition_spec(
                nschema
            ),
            sort_order=self.type_mapper.generate_random_iceberg_sort_order(nschema),
            properties=nproperties,
        )

        # Return created table information for logging
        schema_summary = ", ".join(
            [
                f"{field.name}:{field.field_type}{' NOT NULL' if field.required else ''}"
                for field in ctable.schema().fields
            ]
        )
        partition_summary = (
            (
                "["
                + ",".join(
                    [
                        f"{ctable.schema().find_field(pf.source_id).name}({pf.transform})"
                        for pf in ctable.spec().fields
                    ]
                )
                + "]"
            )
            if len(ctable.spec().fields) > 0
            else "none"
        )
        sort_summary = (
            (
                "["
                + ", ".join(
                    [
                        f"{ctable.schema().find_field(sf.source_id).name}({sf.direction.name[:3]})"
                        for sf in ctable.sort_order().fields
                    ]
                )
                + "]"
            )
            if len(ctable.sort_order().fields) > 0
            else "none"
        )
        prop_summary = (
            (
                "["
                + ", ".join(
                    [f"{key} = {value}" for key, value in ctable.properties.items()]
                )
                + "]"
            )
            if ctable.properties
            else "none"
        )
        return f"{table.get_table_full_path()} | v{ctable.format_version} | Fields: [{schema_summary}] | partitions={partition_summary} | sort={sort_summary} | properties={prop_summary}"

    def generate_table_properties_impl(
        self,
        table: SparkTable,
    ) -> dict[str, Parameter]:
        """
        Generate random Iceberg table properties
        """
        next_properties = {
            # Default file format
            "format-version": lambda: random.choice(["1", "2", "3"]),
            # Target file size
            "write.target-file-size-bytes": lambda: str(
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
            ),
            "write.delete.target-file-size-bytes": lambda: str(
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
            ),
            # Compaction settings
            "commit.manifest.target-size-bytes": lambda: str(
                random.choice(
                    [1048576, 2097152, 4194304, 8388608, 16777216, 33554432]
                )  # 1MB, 2MB, 4MB, 8MB, 16MB, 32MB
            ),
            "commit.manifest.min-count-to-merge": lambda: str(
                random.choice([1, 2, 8, 50, 100, 200, 500])
            ),
            "commit.manifest-merge.enabled": true_false_lambda,
            # Snapshot retention
            "history.expire.max-snapshot-age-ms": lambda: str(
                random.choice(
                    [
                        1000,  # 1 second
                        60000,  # 1 minute
                        3600000,  # 1 hour
                    ]
                )
            ),
            "history.expire.min-snapshots-to-keep": lambda: str(
                random.choice([1, 3, 5, 10])
            ),
            # Snapshot references
            "history.expire.max-ref-age-ms": lambda: str(
                random.choice(
                    [
                        1000,  # 1 second
                        60000,  # 1 minute
                        3600000,  # 1 hour
                    ]
                )
            ),
            # Retry settings
            "commit.retry.num-retries": lambda: str(random.randint(3, 10)),
            "commit.retry.min-wait-ms": lambda: str(random.choice([100, 500, 1000])),
            "commit.retry.max-wait-ms": lambda: str(
                random.choice([1000, 10000, 30000, 60000])
            ),
            "commit.retry.total-timeout-ms": lambda: str(
                random.choice([1000, 60000, 180000, 300000])
            ),
            # Locking
            "commit.lock.enabled": true_false_lambda,
            "commit.lock.timeout-ms": lambda: str(
                random.choice([1000, 30000, 60000, 120000])
            ),
            "write.manifest.min-added-files": lambda: str(
                random.choice([1, 8, 16, 100, 500, 1000, 5000])
            ),
            "write.manifest.max-added-files": lambda: str(
                random.choice([1, 100, 1000, 10000, 50000, 100000])
            ),
            # Manifest list parallelism
            "manifest-lists.enabled": true_false_lambda,
            "manifest-lists.parallelism": lambda: str(random.choice([1, 2, 4, 8, 16])),
            # Metadata deletion
            "write.metadata.delete-after-commit.enabled": true_false_lambda,
            "write.metadata.previous-versions-max": lambda: str(
                random.choice([1, 2, 3, 5, 10, 20])
            ),
            # Metrics collection
            "write.metadata.metrics.default": lambda: random.choice(
                ["none", "counts", "truncate(16)", "full"]
            ),
            # Distribution mode
            "write.distribution-mode": lambda: random.choice(["none", "hash", "range"]),
            "write.delete.distribution-mode": lambda: random.choice(
                ["none", "hash", "range"]
            ),
            "write.object-storage.enabled": true_false_lambda,
            "write.object-storage.partitioned-paths": true_false_lambda,
            # Write parallelism
            "write.tasks.max": lambda: str(
                random.choice([1, 2, 8, 100, 500, 1000, 2000])
            ),
            "write.tasks.min": lambda: str(random.choice([1, 2, 10, 50, 100])),
            # Sorting
            "write.sort.enabled": true_false_lambda,
            "write.sort.order": lambda: self.random_ordered_columns(table, True),
            # Write modes
            "write.update.isolation-level": lambda: random.choice(
                ["serializable", "snapshot"]
            ),
            "write.update.mode": lambda: random.choice(
                ["copy-on-write", "merge-on-read"]
            ),
            "write.delete.granularity": lambda: random.choice(["partition", "file"]),
            "write.delete.isolation-level": lambda: random.choice(
                ["serializable", "snapshot"]
            ),
            "write.delete.mode": lambda: random.choice(
                ["copy-on-write", "merge-on-read"]
            ),
            "write.merge.isolation-level": lambda: random.choice(
                ["serializable", "snapshot"]
            ),
            "write.merge.mode": lambda: random.choice(
                ["copy-on-write", "merge-on-read"]
            ),
            "write.metadata.compression-codec": lambda: random.choice(
                ["gzip", "zstd", "none"]
            ),
            "write.spark.fanout.enabled": true_false_lambda,
            "write.wap.enabled": true_false_lambda,
            "read.manifest.cache.enabled": true_false_lambda,
            # Split size
            "read.split.target-size": lambda: str(
                random.choice(
                    [
                        1048576,
                        2097152,
                        4194304,
                        8388608,
                        134217728,
                        268435456,
                        536870912,
                    ]
                )  # 128MB  # 256MB  # 512MB
            ),
            "read.split.metadata-target-size": lambda: str(
                random.choice(
                    [1048576, 2097152, 4194304, 8388608, 33554432, 67108864, 134217728]
                )  # 32MB  # 64MB  # 128MB
            ),
            "read.split.planning-lookback": lambda: str(random.choice([10, 50, 100])),
            # Streaming
            "read.stream.enabled": true_false_lambda,
            "read.stream.skip-delete-snapshots": true_false_lambda,
            "read.stream.skip-overwrite-snapshots": true_false_lambda,
            # Parquet vectorization
            "read.parquet.vectorization.enabled": true_false_lambda,
            "read.parquet.vectorization.batch-size": lambda: str(
                random.choice([1, 16, 32, 128, 1024, 2048, 4096, 8192])
            ),
            # Compatibility
            "compatibility.snapshot-id-inheritance.enabled": true_false_lambda,
            # Schema evolution
            "schema.auto-evolve": true_false_lambda,
            # Data locality
            "write.data.locality.enabled": true_false_lambda,
        }
        # Column statistics
        next_properties.update(
            {
                f"write.metadata.metrics.column.{val}": lambda: random.choice(
                    ["none", "counts", "truncate(8)", "truncate(16)", "full"]
                )
                for val in list(table.columns.keys())
            }
        )
        # Parquet specific properties
        if self.write_format == FileFormat.Parquet:
            next_properties.update(
                {
                    f"write.parquet.bloom-filter-enabled.column.{val}": true_false_lambda
                    for val in list(table.columns.keys())
                }
            )
            next_properties.update(
                {
                    "write.parquet.bloom-filter-max-bytes": lambda: str(
                        random.choice(
                            [1048576, 2097152, 4194304, 8388608]
                        )  # 1MB, 2MB, 4MB, 8MB
                    ),
                    "write.parquet.compression-codec": lambda: random.choice(
                        ["zstd", "brotli", "lz4", "gzip", "snappy", "uncompressed"]
                    ),
                    "write.parquet.compression-level": lambda: str(
                        random.randint(1, 9)
                    ),
                    "write.parquet.dict-size-bytes": lambda: str(
                        random.choice(
                            [1048576, 2097152, 4194304, 8388608]
                        )  # 1MB, 2MB, 4MB, 8MB
                    ),
                    "write.parquet.page-size-bytes": lambda: str(
                        random.choice(
                            [1048576, 2097152, 4194304, 8388608]
                        )  # 1MB, 2MB, 4MB, 8MB
                    ),
                    "write.parquet.row-group-size-bytes": lambda: str(
                        random.choice(
                            [1048576, 2097152, 4194304, 8388608]
                        )  # 1MB, 2MB, 4MB, 8MB
                    ),
                }
            )
        # ORC specific properties
        elif self.write_format == FileFormat.ORC:
            next_properties.update(
                {
                    "write.orc.compression-codec": lambda: random.choice(
                        ["zstd", "lz4", "lzo", "zlib", "snappy", "none"]
                    ),
                    "write.orc.compression-strategy": lambda: random.choice(
                        ["speed", "compression"]
                    ),
                    "write.orc.stripe-size-bytes": lambda: str(
                        random.choice(
                            [1048576, 2097152, 4194304, 8388608]
                        )  # 1MB, 2MB, 4MB, 8MB
                    ),
                    "write.orc.block-size-bytes": lambda: str(
                        random.choice(
                            [1048576, 2097152, 4194304, 8388608]
                        )  # 1MB, 2MB, 4MB, 8MB
                    ),
                }
            )
        # AVRO specific properties
        elif self.write_format == FileFormat.Avro:
            next_properties.update(
                {
                    "write.avro.compression-codec": lambda: random.choice(
                        ["gzip", "zstd", "snappy", "uncompressed"]
                    )
                }
            )
        return next_properties

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

    def add_options(self, options: dict[str, Parameter]) -> str:
        selected_opts = sample_from_dict(options, random.randint(1, len(options)))
        res = ", options => map("
        res += ",".join([f"'{key}', '{val()}'" for key, val in selected_opts.items()])
        res += ")"
        return res

    def generate_extra_statement(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        next_option = random.randint(1, 15)

        if next_option == 1:
            res = f"CALL `{table.catalog_name}`.system.remove_orphan_files(table => '{table.get_namespace_path()}'"
            if random.randint(1, 2) == 1:
                res += f", dry_run => {random.choice(['true', 'false'])}"
            if random.randint(1, 2) == 1:
                res += f", max_concurrent_deletes => {random.randint(0, 20)}"
            if random.randint(1, 2) == 1:
                res += f", prefix_listing => {random.choice(['true', 'false'])}"
            timestamps = self.get_timestamps(spark, table)
            if len(timestamps) > 0 and random.randint(1, 2) == 1:
                res += f", older_than => TIMESTAMP '{random.choice(timestamps)}'"
            res += ")"
            return res
        if next_option == 2:
            res = f"CALL `{table.catalog_name}`.system.rewrite_position_delete_files('{table.get_namespace_path()}'"
            if random.randint(1, 2) == 1:
                # Add options
                options = {
                    "max-concurrent-file-group-rewrites": lambda: random.randint(0, 10),
                    "partial-progress.enabled": true_false_lambda,
                    "partial-progress.max-commits": lambda: random.randint(0, 20),
                    "use-starting-sequence-number": true_false_lambda,
                    "rewrite-job-order": lambda: random.choice(
                        ["bytes-asc", "bytes-desc", "files-asc", "files-desc", "none"]
                    ),
                    "target-file-size-bytes": lambda: random.choice(
                        [
                            1048576,  # 1MB
                            2097152,  # 2MB
                            134217728,  # 128MB
                            268435456,  # 256MB
                            536870912,  # 512MB
                            1073741824,  # 1GB
                        ]
                    ),
                    "min-input-files": lambda: random.randint(0, 20),
                    "rewrite-all": true_false_lambda,
                    "max-file-group-size-bytes": lambda: random.choice(
                        [
                            1048576,  # 1MB
                            2097152,  # 2MB
                            134217728,  # 128MB
                            268435456,  # 256MB
                            536870912,  # 512MB
                            1073741824,  # 1GB
                        ]
                    ),
                    "max-files-to-rewrite": lambda: random.randint(0, 100),
                }
                res += self.add_options(options)
            res += ")"
            return res
        if next_option == 3:
            res = f"CALL `{table.catalog_name}`.system.rewrite_manifests(table => '{table.get_namespace_path()}'"
            if random.randint(1, 2) == 1:
                res += f", use_caching => {random.choice(['true', 'false'])}"
            res += ")"
            return res
        if next_option == 4:
            res = f"CALL `{table.catalog_name}`.system.expire_snapshots(table => '{table.get_namespace_path()}'"
            timestamps = self.get_timestamps(spark, table)
            if len(timestamps) > 0 and random.randint(1, 2) == 1:
                res += f", older_than => TIMESTAMP '{random.choice(timestamps)}'"
            if random.randint(1, 2) == 1:
                res += f", stream_results => {random.choice(['true', 'false'])}"
            if random.randint(1, 2) == 1:
                res += f", retain_last => {random.randint(1, 10)}"
            if random.randint(1, 2) == 1:
                res += f", max_concurrent_deletes => {random.randint(0, 20)}"
            if random.randint(1, 2) == 1:
                res += f", clean_expired_metadata => {random.choice(['true', 'false'])}"
            res += ")"
            return res
        if next_option in (5, 6, 7, 8):
            calls = [
                "ancestors_of",
                "compute_partition_stats",
                "compute_table_stats",
                "set_current_snapshot",
            ]
            res = f"CALL `{table.catalog_name}`.system.{random.choice(calls)}(table => '{table.get_namespace_path()}'"
            snapshots = self.get_snapshots(spark, table)
            if len(snapshots) > 0 and random.randint(1, 2) == 1:
                res += f", snapshot_id => {random.choice(snapshots)}"
            res += ")"
            return res
        if next_option == 9:
            res = f"CALL `{table.catalog_name}`.system.create_changelog_view(table => '{table.get_namespace_path()}'"
            if random.randint(1, 2) == 1:
                res += f", net_changes => {random.choice(['true', 'false'])}"
            if random.randint(1, 2) == 1:
                res += f", compute_updates => {random.choice(['true', 'false'])}"
            res += ")"
            return res
        snapshots = self.get_snapshots(spark, table)
        if len(snapshots) > 0 and next_option in (10, 11):
            calls = [
                "cherrypick_snapshot",
                "rollback_to_snapshot",
            ]
            return f"CALL `{table.catalog_name}`.system.{random.choice(calls)}(table => '{table.get_namespace_path()}', snapshot_id => {random.choice(snapshots)})"
        timestamps = self.get_timestamps(spark, table)
        if len(timestamps) > 0 and next_option in (12, 13):
            return f"CALL `{table.catalog_name}`.system.rollback_to_timestamp(table => '{table.get_namespace_path()}', timestamp => TIMESTAMP '{random.choice(timestamps)}')"
        # Call rewrite_data_files when there is no other option
        zorder = False
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
        if random.randint(1, 3) == 1:
            # Add options
            options = {
                "max-concurrent-file-group-rewrites": lambda: random.randint(0, 10),
                "partial-progress.enabled": true_false_lambda,
                "partial-progress.max-commits": lambda: random.randint(0, 20),
                "use-starting-sequence-number": true_false_lambda,
                "rewrite-job-order": lambda: random.choice(
                    ["bytes-asc", "bytes-desc", "files-asc", "files-desc", "none"]
                ),
                "target-file-size-bytes": lambda: random.choice(
                    [
                        1048576,  # 1MB
                        2097152,  # 2MB
                        134217728,  # 128MB
                        268435456,  # 256MB
                        536870912,  # 512MB
                        1073741824,  # 1GB
                    ]
                ),
                "min-input-files": lambda: random.randint(0, 20),
                "rewrite-all": true_false_lambda,
                "max-file-group-size-bytes": lambda: random.choice(
                    [
                        1048576,  # 1MB
                        2097152,  # 2MB
                        134217728,  # 128MB
                        268435456,  # 256MB
                        536870912,  # 512MB
                        1073741824,  # 1GB
                    ]
                ),
                "delete-file-threshold": lambda: random.randint(0, 10000),
                "delete-ratio-threshold": lambda: random.uniform(0, 1),
                "remove-dangling-deletes": true_false_lambda,
            }
            if next_strategy == "sort":
                options.update(
                    {
                        "compression-factor": lambda: random.uniform(0, 1),
                        "shuffle-partitions-per-file": lambda: random.randint(0, 20),
                    }
                )
            if zorder:
                options.update(
                    {
                        "var-length-contribution": lambda: random.randint(0, 100),
                        "max-output-size": lambda: random.choice(
                            [
                                1048576,  # 1MB
                                2097152,  # 2MB
                                134217728,  # 128MB
                                268435456,  # 256MB
                                536870912,  # 512MB
                                1073741824,  # 1GB
                            ]
                        ),
                    }
                )
            res += self.add_options(options)
        res += ")"
        return res


class DeltaLakePropertiesGenerator(LakeTableGenerator):

    def __init__(self):
        super().__init__()

    def get_format(self) -> str:
        return "delta"

    def set_basic_properties(self) -> dict[str, str]:
        return {}

    def add_partition_clauses(self, table: SparkTable) -> list[str]:
        res = []
        # No partition by subcolumns in delta
        for k in table.columns.keys():
            res.append(k)
        return res

    def add_generated_col(
        self, columns: dict[str, SparkColumn], col: sp.DataType
    ) -> str:
        if isinstance(col, sp.LongType) and random.randint(1, 10) < 3:
            return f" GENERATED {random.choice(['ALWAYS', 'BY DEFAULT'])} AS IDENTITY"
        if len(columns) > 0 and random.randint(1, 10) < 3:
            flattened = {}
            for _, val in columns.items():
                val.flat_column(flattened)
            if (
                isinstance(
                    col, (sp.ByteType, sp.ShortType, sp.IntegerType, sp.LongType)
                )
                and random.randint(1, 2) == 1
            ):
                return f" GENERATED ALWAYS AS ({random.choice(['year', 'month', 'day', 'hour'])}({random.choice(list(flattened.keys()))}))"
            return f" GENERATED ALWAYS AS (CAST({random.choice(list(flattened.keys()))} AS {self.type_mapper.generate_random_spark_sql_type()}))"
        return ""

    def create_catalog_table(
        self,
        catalog_impl,
        columns: list[dict[str, str]],
        table: SparkTable,
    ) -> str:
        return ""

    def generate_table_properties_impl(
        self,
        table: SparkTable,
    ) -> dict[str, Parameter]:
        """
        Generate random Delta Lake table properties
        """
        return {
            # Log retention
            "delta.logRetentionDuration": lambda: random.choice(
                [
                    "interval 1 second",
                    "interval 1 minute",
                    "interval 1 hour",
                ]
            ),
            # Deleted file retention (for VACUUM)
            "delta.deletedFileRetentionDuration": lambda: random.choice(
                [
                    "interval 1 second",
                    "interval 1 minute",
                    "interval 1 hour",
                ]
            ),
            # Auto optimize
            "delta.autoOptimize.optimizeWrite": true_false_lambda,
            "delta.autoOptimize.autoCompact": true_false_lambda,
            # Column mapping mode
            "delta.columnMapping.mode": lambda: random.choice(["none", "name", "id"]),
            # Set minimum versions for readers and writers
            "delta.minReaderVersion": lambda: random.choice(
                [f"{i}" for i in range(1, 4)]
            ),
            "delta.minWriterVersion": lambda: random.choice(
                [f"{i}" for i in range(1, 8)]
            ),
            "delta.checkpointPolicy": lambda: random.choice(["classic", "v2"]),
            # Parquet compression
            "spark.sql.parquet.compression.codec": lambda: random.choice(
                ["snappy", "gzip", "lzo", "lz4", "zstd", "uncompressed"]
            ),
            # Statistics columns
            "delta.dataSkippingNumIndexedCols": lambda: str(
                random.choice([1, 8, 16, 32, 64, 128, 256])
            ),
            # Statistics collection
            "delta.checkpoint.writeStatsAsJson": true_false_lambda,
            "delta.checkpoint.writeStatsAsStruct": true_false_lambda,
            "delta.enableChangeDataFeed": true_false_lambda,  # FIXME later requires specific writer version
            "delta.enableExpiredLogCleanup": true_false_lambda,
            # Append-only table
            "delta.appendOnly": true_false_lambda,
            # Isolation level
            "delta.isolationLevel": lambda: str(
                random.choice(
                    ["Serializable"]
                )  # , "WriteSerializable" is not supported
            ),
            # Checkpoint interval
            "delta.checkpointInterval": lambda: str(
                random.choice([1, 5, 10, 20, 50, 100])
            ),
            # Checkpoint retention
            "delta.checkpointRetentionDuration": lambda: random.choice(
                [
                    "interval 1 second",
                    "interval 1 minute",
                    "interval 1 day",
                ]
            ),
            "delta.setTransactionRetentionDuration": lambda: random.choice(
                [
                    "interval 1 second",
                    "interval 1 minute",
                    "interval 1 day",
                ]
            ),
            # Compatibility
            "delta.compatibility.symlinkFormatManifest.enabled": true_false_lambda,
            # Enable deletion vectors (Delta 3.0+)
            "delta.enableDeletionVectors": true_false_lambda,
            # Row tracking
            "delta.enableRowTracking": true_false_lambda,
            # Type widening
            "delta.enableTypeWidening": true_false_lambda,
            # Timestamp NTZ support
            "delta.feature.timestampNtz": lambda: random.choice(
                ["supported", "enabled"]
            ),
            # Variant type support
            "delta.feature.variantType-preview": lambda: random.choice(
                ["supported", "enabled"]
            ),
            # Not available on OSS Spark
            # Optimize write
            "spark.databricks.delta.autoCompact.enabled": true_false_lambda,
            # Adaptive shuffle
            "spark.databricks.delta.optimizeWrite.enabled": true_false_lambda,
            # Delta cache
            "spark.databricks.io.cache.enabled": true_false_lambda,
            "spark.databricks.io.cache.maxDiskUsage": lambda: random.choice(
                ["10g", "20g", "50g", "100g"]
            ),
            "spark.databricks.io.cache.maxMetaDataCache": lambda: random.choice(
                ["1g", "2g", "5g", "10g"]
            ),
            "spark.databricks.io.cache.compression.enabled": true_false_lambda,
            # Target file size
            "spark.databricks.delta.optimize.maxFileSize": lambda: random.choice(
                ["1mb", "2mb", "8mb", "64mb", "128mb", "256mb", "512mb", "1gb"]
            ),
            # Parquet file size
            "spark.databricks.delta.parquet.blockSize": lambda: str(
                random.choice(
                    [134217728, 268435456, 536870912]
                )  # 128MB  # 256MB  # 512MB
            ),
            # Sampling for stats
            "spark.databricks.delta.stats.skipping": true_false_lambda,
        }

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
            return f"OPTIMIZE {table.get_table_full_path()}{f' ZORDER BY ({self.random_ordered_columns(table, False)})' if random.randint(1, 2) == 1 else ''};"
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
