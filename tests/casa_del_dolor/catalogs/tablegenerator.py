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


# Delta liquid clustering (CLUSTER BY) and data skipping only support scalar
# types Delta collects min/max statistics for: numeric, date, timestamp
# (including TimestampNTZ) and string. This mirrors Delta's
# `SkippingEligibleDataType`. Boolean, binary and container types
# (Array/Map/Struct) are rejected at DDL time with
# `DELTA_CLUSTERING_COLUMNS_DATATYPE_NOT_SUPPORTED`, so they must never be
# chosen as clustering keys. `CharType`/`VarcharType` are string-like and
# eligible (Delta materialises them as string). `TimestampNTZType` is absent
# from older PySpark versions, hence the feature probe.
_DELTA_SKIPPING_ELIGIBLE_TYPES = (
    sp.NumericType,
    sp.DateType,
    sp.TimestampType,
    sp.StringType,
    sp.CharType,
    sp.VarcharType,
) + ((sp.TimestampNTZType,) if hasattr(sp, "TimestampNTZType") else ())


def delta_skipping_eligible(dtype) -> bool:
    return isinstance(dtype, _DELTA_SKIPPING_ELIGIBLE_TYPES)


_TEMPORAL_TYPES = (sp.DateType, sp.TimestampType) + (
    (sp.TimestampNTZType,) if hasattr(sp, "TimestampNTZType") else ()
)
_STRINGY_TYPES = (sp.StringType, sp.CharType, sp.VarcharType)


def spark_scalar_castable(src: sp.DataType, dst: sp.DataType) -> bool:
    """Conservative analysis-time validity of ``CAST(src AS dst)`` for the scalar leaf
    types a Delta generated column may use. A generated-column expression must both match
    the declared column type AND be a legal cast from its source, otherwise CREATE fails
    (e.g. Spark rejects `boolean -> date` at analysis time)."""
    if isinstance(dst, _STRINGY_TYPES):
        return True  # any scalar casts to string
    if isinstance(dst, (sp.NumericType, sp.BooleanType)):
        return isinstance(src, (sp.NumericType, sp.BooleanType) + _STRINGY_TYPES)
    if isinstance(dst, _TEMPORAL_TYPES):
        return isinstance(src, _TEMPORAL_TYPES + _STRINGY_TYPES)
    return type(src) is type(dst)


def sample_from_dict(d: dict[str, Parameter], sample: int) -> dict[str, Parameter]:
    items = random.sample(list(d.items()), sample)
    return dict(items)


class LakeTableGenerator:
    def __init__(self):
        self.type_mapper = ClickHouseTypeMapper()
        self.write_format: FileFormat = FileFormat.Parquet

    @staticmethod
    def get_next_generator(lake: LakeFormat):
        if lake == LakeFormat.Iceberg:
            return IcebergTableGenerator()
        if lake == LakeFormat.Paimon:
            return PaimonTableGenerator()
        return DeltaLakePropertiesGenerator()

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

    # Safe type promotions supported by Iceberg, Delta, and Paimon
    _TYPE_PROMOTIONS: dict[type, list[str]] = {
        sp.ByteType: ["SMALLINT", "INT", "BIGINT"],
        sp.ShortType: ["INT", "BIGINT"],
        sp.IntegerType: ["BIGINT"],
        sp.FloatType: ["DOUBLE"],
    }

    def _pick_type_promotion(self, col_type: sp.DataType) -> typing.Optional[str]:
        candidates = self._TYPE_PROMOTIONS.get(type(col_type))
        if candidates:
            return random.choice(candidates)
        if isinstance(col_type, sp.DecimalType):
            new_prec = min(col_type.precision + random.randint(1, 5), 38)
            new_scale = col_type.scale
            if new_prec > col_type.precision:
                return f"DECIMAL({new_prec},{new_scale})"
        return None

    @staticmethod
    def _rand_comment() -> str:
        from .datagenerator import SOME_STRINGS

        # Escape backslashes BEFORE quotes: an entry like `\'` would otherwise become
        # `\\'` in the literal, where `\\` parses as a backslash and the quote then
        # terminates the string early -> PARSE_SYNTAX_ERROR on the rest of the DDL
        return (
            random.choice(SOME_STRINGS).replace("\\", "\\\\").replace("'", "\\'")
        )

    @staticmethod
    def _refresh_table_model(spark: SparkSession, table: SparkTable):
        schema = spark.table(table.get_table_full_path()).schema
        new_columns = {}
        for field in schema.fields:
            generated = (
                "delta.generationExpression" in field.metadata
                or "delta.identity.start" in field.metadata
                or (field.name in table.columns and table.columns[field.name].generated)
            )
            prev = table.columns.get(field.name)
            # Only inherit the recorded ClickHouse type while the Spark type is unchanged; an
            # ALTER COLUMN ... TYPE makes the old clickhouse_type stale, which would mislead the
            # _LOSSY_CH_INT_RE hash-comparability check. Clear it on a type change (no CH origin).
            inherited_ch_type = prev.clickhouse_type if prev and prev.spark_type == field.dataType else ""
            new_columns[field.name] = SparkColumn(
                field.name,
                field.dataType,
                field.nullable,
                generated,
                inherited_ch_type,
            )
        table.columns = new_columns
        table.check_constraints.clear()
        # RESTORE can roll back CLUSTER BY / partitioning changes, so re-sync the clustering
        # and partition state that OPTIMIZE generation relies on. DESCRIBE DETAIL exposes both
        # for Delta; guard on the column being present so an older Delta that omits
        # `clusteringColumns` leaves the create/alter-tracked value untouched rather than
        # wrongly clearing it.
        if table.lake_format == LakeFormat.DeltaLake:
            detail = spark.sql(
                f"DESCRIBE DETAIL {table.get_table_full_path()}"
            ).collect()
            if detail:
                row = detail[0].asDict()
                if "clusteringColumns" in row:
                    table.clustered = len(row["clusteringColumns"] or []) > 0
                if "partitionColumns" in row:
                    flat_cols = table.flat_columns()
                    part_cols = row["partitionColumns"] or []
                    table.partition_keys = [c for c in part_cols if c in flat_cols]
                    # Delta only has plain partition columns, so partitionColumns is an
                    # authoritative partitioned-vs-unpartitioned signal here.
                    table.partitioned = len(part_cols) > 0

    def generate_common_alter_statements(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        next_operation = random.randint(1, 1000)
        tpath = table.get_table_full_path()

        if next_operation <= 200:
            # Set random properties
            properties = self.generate_table_properties(table)
            if properties:
                key = random.choice(list(properties.keys()))
                return f"ALTER TABLE {tpath} SET TBLPROPERTIES ('{key}' = '{properties[key]}');"
        elif next_operation <= 400:
            # Unset a property
            properties = self.generate_table_properties(table)
            if properties:
                key = random.choice(list(properties.keys()))
                return f"ALTER TABLE {tpath} UNSET TBLPROPERTIES ('{key}');"
        elif next_operation <= 475:
            # Add a column
            col_name = f"c_added_{random.randint(1, 1000)}"
            col_type = self.type_mapper.generate_random_spark_sql_type()
            stmt = f"ALTER TABLE {tpath} ADD COLUMNS ({col_name} {col_type});"
            spark.sql(stmt)
            from pyspark.sql.types import _parse_datatype_string

            table.columns[col_name] = SparkColumn(
                col_name, _parse_datatype_string(col_type), True, False
            )
            return ""
        elif next_operation <= 550:
            # Drop a top-level column only; nested drops would require
            # updating the parent StructType in the in-memory model.
            cols = list(table.columns.keys())
            if len(cols) > 1:
                col = random.choice(cols)
                stmt = f"ALTER TABLE {tpath} DROP COLUMN {col};"
                spark.sql(stmt)
                table.columns.pop(col, None)
                return ""
        elif next_operation <= 625:
            # Rename a column
            cols = list(table.columns.keys())
            if cols:
                old_name = random.choice(cols)
                new_name = f"c_renamed_{random.randint(1, 1000)}"
                if new_name not in table.columns:
                    stmt = (
                        f"ALTER TABLE {tpath} RENAME COLUMN {old_name} TO {new_name};"
                    )
                    spark.sql(stmt)
                    sc = table.columns.pop(old_name)
                    sc.column_name = new_name
                    table.columns[new_name] = sc
                    return ""
        elif next_operation <= 700:
            # Alter column comment
            flat_cols = list(table.flat_columns().keys())
            if flat_cols:
                return f"ALTER TABLE {tpath} ALTER COLUMN {random.choice(flat_cols)} COMMENT '{self._rand_comment()}';"
        elif next_operation <= 775 and self.get_format() != "paimon":
            # Alter column SET/DROP NOT NULL (not supported by Paimon)
            cols = list(table.columns.keys())
            if cols:
                col_name = random.choice(cols)
                sc = table.columns[col_name]
                action = "SET" if sc.nullable else "DROP"
                spark.sql(
                    f"ALTER TABLE {tpath} ALTER COLUMN {col_name} {action} NOT NULL;"
                )
                sc.nullable = not sc.nullable
                return ""
        elif next_operation <= 850:
            # Widen column type (numeric promotion)
            cols = list(table.columns.items())
            random.shuffle(cols)
            for col_name, sc in cols:
                new_type_str = self._pick_type_promotion(sc.spark_type)
                if new_type_str:
                    stmt = f"ALTER TABLE {tpath} ALTER COLUMN {col_name} TYPE {new_type_str};"
                    spark.sql(stmt)
                    from pyspark.sql.types import _parse_datatype_string

                    sc.spark_type = _parse_datatype_string(new_type_str)
                    # The Spark type changed, so the recorded ClickHouse type is stale; clear it so
                    # _LOSSY_CH_INT_RE no longer treats a widened column as a CH-origin lossy int.
                    sc.clickhouse_type = ""
                    return ""
        return ""

    def generate_alter_table_statements(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        return self.generate_common_alter_statements(spark, table)

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

        # Spark V2 catalogs (Iceberg, Delta) support CREATE OR REPLACE TABLE: it
        # atomically replaces the table's schema and data while keeping its
        # history, and may swap the declared schema or data file format under
        # the same name - good fuzz coverage for readers reconciling a table
        # that changed shape mid-flight. Paimon's Spark catalog does not
        # implement REPLACE, so keep IF NOT EXISTS there. Restricted to
        # non-deterministic tables since, unlike IF NOT EXISTS, it is not a
        # no-op when the table already exists - it wipes and recreates it.
        use_or_replace = (
            not deterministic
            and self.get_format() in ("iceberg", "delta")
            and random.randint(1, 4) == 1
        )
        create_clause = (
            "CREATE OR REPLACE TABLE"
            if use_or_replace
            else "CREATE TABLE IF NOT EXISTS"
        )
        ddl = f"{create_clause} {catalog_name}.test.{table_name} ("
        columns_def = []
        columns_spark = {}
        # Delta IDENTITY columns cannot be partition columns (generated
        # expression columns can be); collect them to exclude below.
        identity_cols: set[str] = set()
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
            if "IDENTITY" in generated:
                identity_cols.add(val["name"])
            # Optional column COMMENT (metadata only; supported by both formats)
            col_comment = (
                f" COMMENT '{self._rand_comment()}'"
                if random.randint(1, 4) == 1
                else ""
            )
            columns_def.append(
                f"{val['name']} {str_type}{'' if nullable else ' NOT NULL'}{generated}{col_comment}"
            )
            columns_spark[val["name"]] = SparkColumn(
                val["name"], spark_type, nullable, len(generated) > 0, next_ch_type
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

        # Delta liquid clustering: CLUSTER BY is mutually exclusive with
        # PARTITIONED BY and is Delta-only. It takes up to 4 keys, each a
        # stats-eligible scalar leaf - top-level or a nested struct field by
        # dotted path (same flattened, backtick-quoted form the Iceberg
        # transforms use). Only data-skipping-eligible scalar leaves qualify
        # (see `delta_skipping_eligible`); this excludes container leaves
        # (Array/Map/Struct), Boolean and Binary, plus any column produced by a
        # generated expression.
        clustered = False
        if self.get_format() == "delta" and random.randint(1, 5) == 1:
            cluster_cols = [
                path
                for path, dtype in res.flat_columns().items()
                if delta_skipping_eligible(dtype)
                and not columns_spark[path.split(".", 1)[0]].generated
            ]
            if cluster_cols:
                random.shuffle(cluster_cols)
                chosen = cluster_cols[: random.randint(1, min(4, len(cluster_cols)))]
                ddl += f" CLUSTER BY ({','.join(chosen)})"
                clustered = True
                res.clustered = True

        # Add Partition by, can't partition by all columns. Identity columns
        # cannot be Delta partition columns; other formats have none, so the
        # filter is a no-op there.
        if not clustered and random.randint(1, 5) == 1:
            partition_clauses = [
                c for c in self.add_partition_clauses(res) if c not in identity_cols
            ]
            # Delta rejects covering every column (ALL_PARTITION_COLUMNS_NOT_ALLOWED),
            # so keep at least one non-partition column there. Iceberg (transforms) and
            # Paimon accept all-column partitioning.
            max_partitions = min(3, len(partition_clauses))
            if self.get_format() == "delta":
                max_partitions = min(max_partitions, len(columns_spark) - 1)
            if partition_clauses and max_partitions > 0:
                random.shuffle(partition_clauses)
                random_subset = random.sample(
                    partition_clauses,
                    k=random.randint(1, max_partitions),
                )
                ddl += f" PARTITIONED BY ({','.join(random_subset)})"
                res.partitioned = True
                # Iceberg: remember the active partition-transform clauses so ALTER
                # DROP/REPLACE PARTITION FIELD can target a field that actually exists.
                if self.get_format() == "iceberg":
                    res.partition_fields = list(random_subset)
                # Remember plain partition columns for Delta OPTIMIZE ... WHERE (which only
                # accepts partition predicates). Non-Delta formats emit transform expressions
                # here (e.g. bucket(n, col)), which are not usable as a WHERE column, so keep
                # only real column names.
                flat_cols = res.flat_columns()
                res.partition_keys = [c for c in random_subset if c in flat_cols]

        # ddl += self.set_table_location(next_location) no location needed yet

        # Optional table COMMENT (metadata only; supported by Iceberg and Delta)
        if random.randint(1, 3) == 1:
            ddl += f" COMMENT '{self._rand_comment()}'"

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

    def generate_alter_table_statements(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        if random.randint(1, 2) == 1:
            return self.generate_common_alter_statements(spark, table)

        next_operation = random.randint(1, 14)
        tpath = table.get_table_full_path()

        if next_operation <= 2:
            # Add or drop a partition field. Executed inline so the tracked state is updated only
            # after the ALTER succeeds (the caller swallows ALTER errors, so mutating first would
            # desync the model from Spark). DROP targets a field that exists.
            if table.partition_fields and random.randint(1, 2) == 1:
                victim = random.choice(table.partition_fields)
                spark.sql(f"ALTER TABLE {tpath} DROP PARTITION FIELD {victim}")
                table.partition_fields.remove(victim)
                table.partitioned = len(table.partition_fields) > 0
                return ""
            # ADD a field not already in the spec (exact-string match; the random bucket/truncate
            # width makes two clauses on the same column distinct, which Iceberg allows).
            candidates = [
                c
                for c in self.add_partition_clauses(table)
                if c not in table.partition_fields
            ]
            if candidates:
                added = random.choice(candidates)
                spark.sql(f"ALTER TABLE {tpath} ADD PARTITION FIELD {added}")
                table.partition_fields.append(added)
                table.partitioned = True
            return ""
        elif next_operation <= 4:
            # Replace an existing field (`old`) with a different one (`new`). Executed inline so
            # the tracked spec is updated only after the ALTER succeeds (see ADD/DROP above).
            if table.partition_fields:
                new_candidates = [
                    c
                    for c in self.add_partition_clauses(table)
                    if c not in table.partition_fields
                ]
                if new_candidates:
                    old = random.choice(table.partition_fields)
                    new = random.choice(new_candidates)
                    spark.sql(
                        f"ALTER TABLE {tpath} REPLACE PARTITION FIELD {old} WITH {new}"
                    )
                    table.partition_fields.remove(old)
                    table.partition_fields.append(new)
            return ""
        elif next_operation <= 6:
            # Set ORDER BY
            if random.randint(1, 2) == 1:
                return f"ALTER TABLE {tpath} WRITE UNORDERED"
            return f"ALTER TABLE {tpath} WRITE{random.choice([' LOCALLY', ''])} ORDERED BY {self.random_ordered_columns(table, True)}"
        elif next_operation <= 8:
            # Set distribution
            if random.randint(1, 2) == 1:
                return f"ALTER TABLE {tpath} WRITE DISTRIBUTED BY PARTITION"
            return f"ALTER TABLE {tpath} WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY {self.random_ordered_columns(table, True)}"
        elif next_operation <= 10:
            # Set identifier fields
            return f"ALTER TABLE {tpath} {random.choice(['SET', 'DROP'])} IDENTIFIER FIELDS {self.random_ordered_columns(table, False)}"
        elif next_operation <= 12:
            # Reorder columns (FIRST / AFTER)
            cols = list(table.columns.keys())
            if len(cols) > 1:
                col = random.choice(cols)
                if random.randint(1, 2) == 1:
                    return f"ALTER TABLE {tpath} ALTER COLUMN {col} FIRST;"
                other = random.choice([c for c in cols if c != col])
                return f"ALTER TABLE {tpath} ALTER COLUMN {col} AFTER {other};"
        elif next_operation <= 14:
            # Branch / tag DDL (branch names match the ones `insert_into_branch` writes to)
            kind = random.choice(["BRANCH", "TAG"])
            name = (
                f"b_{random.randint(1, 3)}"
                if kind == "BRANCH"
                else f"tag_{random.randint(1, 1000)}"
            )
            if random.randint(1, 3) == 1:
                return f"ALTER TABLE {tpath} DROP {kind} IF EXISTS {name}"
            res = f"ALTER TABLE {tpath} CREATE {kind} IF NOT EXISTS {name}"
            if random.randint(1, 3) == 1:
                res += f" RETAIN {random.randint(1, 30)} DAYS"
            if kind == "BRANCH" and random.randint(1, 3) == 1:
                res += f" WITH SNAPSHOT RETENTION {random.randint(1, 5)} SNAPSHOTS"
            return res
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
        timestamp_types = (sp.TimestampType,) + (
            (sp.TimestampNTZType,) if hasattr(sp, "TimestampNTZType") else ()
        )
        flattened_columns = table.flat_columns()
        for k, val in flattened_columns.items():
            # Iceberg cannot partition by container types
            if isinstance(val, (sp.ArrayType, sp.MapType, sp.StructType)):
                continue
            res.append(k)
            is_timestamp = isinstance(val, timestamp_types)
            if (
                is_timestamp
                or isinstance(val, sp.DateType)
                or random.randint(0, 9) == 0
            ):
                res.append(f"year({k})")
                res.append(f"month({k})")
                res.append(f"day({k})")
                # `hour` is invalid on DATE columns
                if is_timestamp or not isinstance(val, sp.DateType):
                    res.append(f"hour({k})")
            # bucket count and truncate width must be positive
            res.append(f"bucket({random.randint(1, 1000)}, {k})")
            res.append(f"truncate({random.randint(1, 1000)}, {k})")
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
            # Convert columns. Consume the top-level field's ID before converting:
            # the conversion allocates IDs for nested elements from the same counter
            next_field_id = self.type_mapper.field_id
            self.type_mapper.increment()
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
            first = False
        nschema = Schema(*fields)

        if random.randint(1, 2) == 1:
            nproperties.update(self.generate_table_properties(table))
        npartition_spec, npartition_clauses = (
            self.type_mapper.generate_random_iceberg_partition_spec(nschema)
        )
        # This catalog path never populates partition_keys (those are plain SQL DDL
        # partition columns); track the active partition fields (as Spark clauses) and the
        # partitioned flag so ALTER DROP/REPLACE and the unpartitioned-only operations gate
        # correctly.
        table.partition_fields = npartition_clauses
        table.partitioned = len(npartition_clauses) > 0
        ctable = catalog_impl.create_table(
            identifier=("test", table.table_name),
            location=f"s3{'a' if table.catalog == LakeCatalogs.Hive else ''}://warehouse-{'rest' if table.catalog == LakeCatalogs.REST else ('hms' if table.catalog == LakeCatalogs.Hive else 'glue')}/data",
            schema=nschema,
            partition_spec=npartition_spec,
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
            "write.metadata.compression-codec": lambda: random.choice(["gzip", "none"]),
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
                    # No 'brotli': it needs org.apache.hadoop.io.compress.BrotliCodec,
                    # an external jar that is not on the Spark classpath
                    "write.parquet.compression-codec": lambda: random.choice(
                        ["zstd", "lz4", "gzip", "snappy", "uncompressed"]
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
        # `write.metadata.delete-after-commit.enabled` deletes old `vN.metadata.json`
        # files after each commit. That is safe with a real metastore catalog (Glue/Hive/
        # REST/Nessie), which stores the current-metadata pointer externally, but it
        # corrupts a Hadoop catalog: `HadoopTableOperations` tracks the current version via
        # `version-hint.text` + `vN.metadata.json`, and on refresh it fails with
        # "Metadata file for version N is missing" once those files are deleted (worse on
        # object stores, which lack atomic rename). The Iceberg fallback catalog
        # (`LakeCatalogs.NoCatalog`) is exactly the Hadoop catalog, so drop the property
        # there to avoid self-corrupting the table.
        if table.catalog == LakeCatalogs.NoCatalog:
            next_properties.pop("write.metadata.delete-after-commit.enabled", None)
        return next_properties

    def generate_table_properties(
        self,
        table: SparkTable,
    ) -> dict[str, str]:
        properties = super().generate_table_properties(table)
        if (
            "write.parquet.compression-level" in properties
            and properties.get("write.parquet.compression-codec") != "zstd"
        ):
            del properties["write.parquet.compression-level"]
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
        next_option = random.randint(1, 16)

        if next_option == 1:
            res = f"CALL `{table.catalog_name}`.system.remove_orphan_files(table => '{table.get_namespace_path()}'"
            if random.randint(1, 2) == 1:
                res += f", dry_run => {random.choice(['true', 'false'])}"
            if random.randint(1, 2) == 1:
                res += f", max_concurrent_deletes => {random.randint(1, 20)}"
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
            if random.randint(1, 4) == 1:
                res += f", spec_id => {random.randint(0, 2)}"
            res += ")"
            return res
        if next_option == 4:
            res = f"CALL `{table.catalog_name}`.system.expire_snapshots(table => '{table.get_namespace_path()}'"
            timestamps = self.get_timestamps(spark, table)
            if len(timestamps) > 0 and random.randint(1, 2) == 1:
                res += f", older_than => TIMESTAMP '{random.choice(timestamps)}'"
            snapshots = self.get_snapshots(spark, table)
            if len(snapshots) > 0 and random.randint(1, 3) == 1:
                res += f", snapshot_ids => ARRAY({random.choice(snapshots)})"
            if random.randint(1, 2) == 1:
                res += f", stream_results => {random.choice(['true', 'false'])}"
            if random.randint(1, 2) == 1:
                res += f", retain_last => {random.randint(1, 10)}"
            if random.randint(1, 2) == 1:
                res += f", max_concurrent_deletes => {random.randint(1, 20)}"
            if random.randint(1, 2) == 1:
                res += f", clean_expired_metadata => {random.choice(['true', 'false'])}"
            res += ")"
            return res
        if next_option in (5, 6, 7, 8):
            snapshots = self.get_snapshots(spark, table)
            # These three take an OPTIONAL snapshot_id.
            calls = [
                "ancestors_of",
                "compute_partition_stats",
                "compute_table_stats",
            ]
            # set_current_snapshot REQUIRES exactly one of snapshot_id / ref on every
            # call, so only offer it when a concrete snapshot_id is available, and then
            # always pass one of the required arguments (`main` always exists once the
            # table has a snapshot).
            if len(snapshots) > 0:
                calls.append("set_current_snapshot")
            chosen = random.choice(calls)
            res = f"CALL `{table.catalog_name}`.system.{chosen}(table => '{table.get_namespace_path()}'"
            if chosen == "set_current_snapshot":
                if random.randint(1, 2) == 1:
                    res += f", snapshot_id => {random.choice(snapshots)}"
                else:
                    res += ", ref => 'main'"
            elif len(snapshots) > 0 and random.randint(1, 2) == 1:
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
        if next_option == 14:
            # Fast-forward `main` up to one of the branches `insert_into_branch` writes to.
            # `insert_into_branch` only ever advances `b_1..b_3` and leaves `main` untouched, so
            # `b_n` is always the descendant. `fast_forward` requires `to` to be a descendant of
            # `branch`, so the direction must stay branch => 'main', to => 'b_n'; reversing it
            # (branch => 'b_n', to => 'main') is a guaranteed failure once the branch exists.
            other = f"b_{random.randint(1, 3)}"
            return f"CALL `{table.catalog_name}`.system.fast_forward(table => '{table.get_namespace_path()}', branch => 'main', to => '{other}')"
        # `publish_changes` was intentionally dropped: it only succeeds for a wap_id staged by an
        # earlier write with `spark.wap.id` set, which this generator never does, so every call
        # failed with an unknown WAP ID. Restore it once WAP writes can be staged under a tracked id.
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
        # No partition by subcolumns in delta, and no partitioning by container types
        for k, sc in table.columns.items():
            if not isinstance(sc.spark_type, (sp.ArrayType, sp.MapType, sp.StructType)):
                res.append(k)
        return res

    # DISABLED: OSS delta-spark's `DeltaCatalog` does not declare Spark 4's
    # `SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS` / `..._IDENTITY_COLUMNS`
    # capabilities, so every SQL CREATE TABLE with a generated or identity
    # column fails analysis with `UNSUPPORTED_FEATURE.TABLE_OPERATION`
    # ("does not support generated columns"). Only the `DeltaTableBuilder`
    # API can create them; re-enable if the create path moves to that API
    # or delta-spark implements the capability.
    SUPPORTS_SQL_GENERATED_COLUMNS = False

    def add_generated_col(
        self, columns: dict[str, SparkColumn], col: sp.DataType
    ) -> str:
        if not self.SUPPORTS_SQL_GENERATED_COLUMNS:
            return ""
        # IDENTITY is valid only on BIGINT (Long) columns.
        if isinstance(col, sp.LongType) and random.randint(1, 10) < 3:
            return f" GENERATED {random.choice(['ALWAYS', 'BY DEFAULT'])} AS IDENTITY"
        if len(columns) == 0 or random.randint(1, 10) >= 3:
            return ""
        # A Delta generated column may reference only already-defined, NON-generated,
        # scalar TOP-LEVEL columns (never Array/Map/Struct, nested paths, or another
        # generated column), and its expression's result type must EXACTLY match the
        # declared column type -- otherwise CREATE TABLE fails.
        scalar = {
            name: sc.spark_type
            for name, sc in columns.items()
            if not sc.generated
            and not isinstance(sc.spark_type, (sp.ArrayType, sp.MapType, sp.StructType))
        }
        if not scalar:
            return ""
        # year/month/day/hour return INT and require a DATE/TIMESTAMP argument (hour needs
        # a TIMESTAMP). Only usable when this column is exactly INT and a temporal source
        # exists -- applying them to a non-temporal column or a non-INT target both fail.
        if isinstance(col, sp.IntegerType):
            dates = [n for n, t in scalar.items() if isinstance(t, sp.DateType)]
            stamps = [
                n
                for n, t in scalar.items()
                if isinstance(t, _TEMPORAL_TYPES) and not isinstance(t, sp.DateType)
            ]
            kinds = ([("date", dates)] if dates else []) + (
                [("ts", stamps)] if stamps else []
            )
            if kinds and random.randint(1, 2) == 1:
                kind, srcs = random.choice(kinds)
                fns = ["year", "month", "day"] + (["hour"] if kind == "ts" else [])
                return f" GENERATED ALWAYS AS ({random.choice(fns)}({random.choice(srcs)}))"
        # Otherwise CAST a scalar source to THIS column's own type: the target must equal
        # the declared type, and the source must be castable to it.
        castable = [n for n, t in scalar.items() if spark_scalar_castable(t, col)]
        if not castable:
            return ""
        return f" GENERATED ALWAYS AS (CAST({random.choice(castable)} AS {col.simpleString()}))"

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
            # Parquet compression. No 'lzo': it needs the separately-installed
            # hadoop-lzo codec, which is not on the Spark classpath
            "spark.sql.parquet.compression.codec": lambda: random.choice(
                ["snappy", "gzip", "lz4", "zstd", "uncompressed"]
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
            # Target file size
            # V2 checkpoint
            "delta.feature.v2Checkpoint": lambda: random.choice(
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

    def generate_table_properties(
        self,
        table: SparkTable,
    ) -> dict[str, str]:
        properties = super().generate_table_properties(table)
        if (
            properties.get("delta.enableDeletionVectors") == "true"
            and properties.get("delta.compatibility.symlinkFormatManifest.enabled")
            == "true"
        ):
            # Delta rejects persistent deletion vectors combined with incremental
            # symlink manifest generation, so keep only one of them enabled
            properties[
                random.choice(
                    [
                        "delta.enableDeletionVectors",
                        "delta.compatibility.symlinkFormatManifest.enabled",
                    ]
                )
            ] = "false"
        if (
            properties.get("delta.columnMapping.mode") in ("name", "id")
            and properties.get("delta.compatibility.symlinkFormatManifest.enabled")
            == "true"
        ):
            # Writes fail with DELTA_UNSUPPORTED_MANIFEST_GENERATION_WITH_COLUMN_MAPPING:
            # symlink manifests are for external readers, which cannot resolve
            # column-mapped tables. Keep only one of the two features
            if random.randint(1, 2) == 1:
                properties["delta.columnMapping.mode"] = "none"
            else:
                properties[
                    "delta.compatibility.symlinkFormatManifest.enabled"
                ] = "false"
        return properties

    def generate_alter_table_statements(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        if random.randint(1, 4) == 1:
            tpath = table.get_table_full_path()
            if table.check_constraints and random.randint(1, 2) == 1:
                cname = random.choice(list(table.check_constraints.keys()))
                del table.check_constraints[cname]
                return f"ALTER TABLE {tpath} DROP CONSTRAINT IF EXISTS {cname};"
            flat_cols = list(table.flat_columns().keys())
            if flat_cols:
                cname = f"chk_{random.randint(1, 10000)}"
                col = random.choice(flat_cols)
                expr = random.choice(
                    [
                        f"{col} IS NOT NULL",
                        f"length({col}) > 0",
                        f"{col} >= 0",
                        f"{col} <> ''",
                    ]
                )
                table.check_constraints[cname] = expr
                return f"ALTER TABLE {tpath} ADD CONSTRAINT {cname} CHECK ({expr});"
        return self.generate_common_alter_statements(spark, table)

    def generate_extra_statement(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        next_option = random.randint(1, 9)

        if next_option == 1:
            # Vacuum
            res = f"VACUUM {table.get_table_full_path()}"
            if random.randint(1, 2) == 1:
                res += f" RETAIN {random.choice([0, 1, 168])} HOURS"
            if random.randint(1, 4) == 1:
                res += " DRY RUN"
            return res + ";"
        if next_option == 2:
            # Optimize. Databricks/Delta treats liquid-clustered tables (CLUSTER BY)
            # differently from the rest, so the direction must follow the tracked state:
            #  - clustered: recluster via plain OPTIMIZE or OPTIMIZE ... FULL. ZORDER BY is
            #    rejected (mutually exclusive with clustering) and a WHERE predicate is not
            #    supported, so neither is emitted.
            #  - non-clustered: FULL is only valid for clustered tables, so it is not emitted.
            #    OPTIMIZE ... WHERE only accepts partition predicates, so a WHERE is added only
            #    when the table has partition keys and references one of them; ZORDER BY is free.
            res = f"OPTIMIZE {table.get_table_full_path()}"
            if table.clustered:
                if random.randint(1, 4) == 1:
                    res += " FULL"
                return res + ";"
            if table.partition_keys and random.randint(1, 3) == 1:
                res += f" WHERE {random.choice(table.partition_keys)} IS NOT NULL"
            if random.randint(1, 2) == 1:
                res += f" ZORDER BY ({self.random_ordered_columns(table, False)})"
            return res + ";"
        if next_option in (3, 4):
            # Restore — executed here so we can refresh the in-memory model
            # afterward (RESTORE can roll back schema-mutating alters).
            result = spark.sql(
                f"DESCRIBE HISTORY {table.get_table_full_path()};"
            ).collect()
            snapshots = [r.version for r in result]
            timestamps = [r.timestamp for r in result]

            if len(snapshots) > 0 and (
                len(timestamps) == 0 or random.randint(1, 2) == 1
            ):
                stmt = f"RESTORE TABLE {table.get_table_full_path()} TO VERSION AS OF {random.choice(snapshots)};"
            elif len(timestamps) > 0:
                stmt = f"RESTORE TABLE {table.get_table_full_path()} TO TIMESTAMP AS OF '{random.choice(timestamps)}';"
            else:
                stmt = (
                    f"RESTORE TABLE {table.get_table_full_path()} TO VERSION AS OF 1;"
                )
            spark.sql(stmt)
            self._refresh_table_model(spark, table)
            return ""
        if next_option == 5:
            # Describe history
            return f"DESCRIBE HISTORY {table.get_table_full_path()};"
        if next_option == 6:
            # Describe detail
            return f"DESCRIBE DETAIL {table.get_table_full_path()};"
        if next_option == 7:
            # Generate manifest
            return f"GENERATE symlink_format_manifest FOR TABLE {table.get_table_full_path()};"
        if next_option == 8:
            # Rewrite files with soft-deleted rows (purges deletion vectors)
            return f"REORG TABLE {table.get_table_full_path()} APPLY (PURGE);"
        if next_option == 9:
            # Change liquid clustering keys after creation. Executed inline (like RESTORE) so
            # the tracked `clustered` flag is updated only after the ALTER succeeds; setting it
            # before the statement runs would desync it whenever the ALTER fails (the caller
            # ignores such errors), which would then mis-drive OPTIMIZE generation. The new
            # value is known from the statement, so set it directly rather than via
            # `_refresh_table_model` (that helper also clears check constraints, which a
            # CLUSTER BY change must not do).
            if random.randint(1, 3) == 1:
                stmt = f"ALTER TABLE {table.get_table_full_path()} CLUSTER BY NONE"
                new_clustered = False
            else:
                cluster_cols = [
                    path
                    for path, dtype in table.flat_columns().items()
                    if delta_skipping_eligible(dtype)
                    and not table.columns[path.split(".", 1)[0]].generated
                ]
                if not cluster_cols:
                    return ""
                random.shuffle(cluster_cols)
                chosen = cluster_cols[: random.randint(1, min(4, len(cluster_cols)))]
                stmt = f"ALTER TABLE {table.get_table_full_path()} CLUSTER BY ({','.join(chosen)})"
                new_clustered = True
            spark.sql(stmt)
            table.clustered = new_clustered
            return ""
        return ""


class PaimonTableGenerator(LakeTableGenerator):

    def __init__(self):
        super().__init__()

    def get_format(self) -> str:
        return "paimon"

    def set_basic_properties(self) -> dict[str, str]:
        properties = {}
        out_format = FileFormat.file_to_str(self.write_format)
        if out_format.lower() not in ("parquet", "orc"):
            out_format = random.choice(["parquet", "orc"])
            self.write_format = FileFormat.file_from_str(out_format)
        properties["file.format"] = out_format
        return properties

    def add_partition_clauses(self, table: SparkTable) -> list[str]:
        res = []
        # No partitioning by container types
        for k, sc in table.columns.items():
            if not isinstance(sc.spark_type, (sp.ArrayType, sp.MapType, sp.StructType)):
                res.append(k)
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
        return ""

    def generate_table_properties(
        self,
        table: SparkTable,
    ) -> dict[str, str]:
        properties = super().generate_table_properties(table)
        flat_cols = list(table.flat_columns().keys())
        has_pk = "primary-key" in properties
        if not has_pk:
            for key in (
                "merge-engine",
                "changelog-producer",
                "sequence.field",
                "deletion-vectors.enabled",
                "deletion-vectors.bitmap64",
            ):
                properties.pop(key, None)
        if properties.get("merge-engine") == "first-row":
            # Paimon rejects a sequence field and deletion vectors on the 'first-row' merge
            # engine, and only 'none' and 'lookup' changelog producers are supported with it
            properties.pop("sequence.field", None)
            properties.pop("deletion-vectors.enabled", None)
            if properties.get("changelog-producer") not in (None, "none", "lookup"):
                properties["changelog-producer"] = random.choice(["none", "lookup"])
        if (
            properties.get("deletion-vectors.enabled") == "true"
            and properties.get("changelog-producer") == "full-compaction"
        ):
            # Deletion vectors mode is only supported for none/input/lookup changelog producers
            properties["changelog-producer"] = random.choice(
                ["none", "input", "lookup"]
            )
        if (
            "deletion-vectors.bitmap64" in properties
            and properties.get("deletion-vectors.enabled") != "true"
        ):
            del properties["deletion-vectors.bitmap64"]
        if "bucket-key" in properties and "bucket" not in properties:
            del properties["bucket-key"]
        elif "bucket" in properties and "bucket-key" not in properties:
            properties["bucket-key"] = random.choice(flat_cols)
        # full-compaction.delta-commits is only valid for fixed-bucket tables. Paimon rejects it
        # for unaware bucket (append-only without a 'bucket') and dynamic bucket (primary key
        # without a fixed 'bucket') alike, so require a fixed bucket here.
        if "full-compaction.delta-commits" in properties and "bucket" not in properties:
            del properties["full-compaction.delta-commits"]
        # write-only installs NoopCompactManager, but full-compaction.delta-commits makes the
        # writer trigger a guaranteed compaction on commit -> every INSERT fails
        if properties.get("write-only") == "true":
            properties.pop("full-compaction.delta-commits", None)
        for min_key, max_key in (
            ("snapshot.num-retained.min", "snapshot.num-retained.max"),
            ("compaction.min.file-num", "compaction.max.file-num"),
            ("num-sorted-run.compaction-trigger", "num-sorted-run.stop-trigger"),
        ):
            has_min, has_max = min_key in properties, max_key in properties
            if has_min and has_max:
                lo, hi = int(properties[min_key]), int(properties[max_key])
                if lo > hi:
                    properties[max_key] = str(lo)
            elif has_max:
                # Only the max is set: Paimon's default min can exceed it (e.g.
                # snapshot.num-retained.min defaults to 10 > a chosen max of 5), which
                # fails validation, so add an explicit min no larger than the chosen max.
                properties[min_key] = str(random.randint(1, int(properties[max_key])))
        return properties

    def generate_table_properties_impl(
        self,
        table: SparkTable,
    ) -> dict[str, Parameter]:
        flat_cols = list(table.flat_columns().keys())
        next_properties = {
            "primary-key": lambda: ",".join(
                random.sample(flat_cols, k=random.randint(1, min(3, len(flat_cols))))
            ),
            "bucket": lambda: str(random.choice([1, 2, 4, 8, 16])),
            "bucket-key": lambda: random.choice(flat_cols),
            "changelog-producer": lambda: random.choice(
                ["none", "input", "full-compaction", "lookup"]
            ),
            "merge-engine": lambda: random.choice(
                ["deduplicate", "partial-update", "first-row", "aggregation"]
            ),
            "sequence.field": lambda: random.choice(flat_cols),
            "metadata.iceberg.storage": lambda: "hadoop-catalog",
            "metadata.iceberg.format-version": lambda: random.choice(["1", "2", "3"]),
            "deletion-vectors.enabled": true_false_lambda,
            "deletion-vectors.bitmap64": true_false_lambda,
            "compaction.optimization-interval": lambda: random.choice(
                ["1ms", "1s", "1min", "1h"]
            ),
            "snapshot.time-retained": lambda: random.choice(["1s", "1min", "1h", "1d"]),
            "snapshot.num-retained.min": lambda: str(random.choice([1, 2, 5, 10])),
            "snapshot.num-retained.max": lambda: str(random.choice([5, 10, 50, 100])),
            "write-buffer-size": lambda: random.choice(
                ["64kb", "256kb", "1mb", "64mb", "256mb"]
            ),
            "page-size": lambda: random.choice(["4kb", "64kb", "1mb"]),
            "target-file-size": lambda: random.choice(
                ["1mb", "32mb", "128mb", "256mb"]
            ),
            "compaction.min.file-num": lambda: str(random.choice([3, 5, 10])),
            "compaction.max.file-num": lambda: str(random.choice([5, 10, 50])),
            "compaction.early-max.file-num": lambda: str(random.choice([4, 8, 16])),
            "num-sorted-run.compaction-trigger": lambda: str(random.choice([3, 5, 10])),
            "num-sorted-run.stop-trigger": lambda: str(random.choice([10, 20, 50])),
            "write-only": true_false_lambda,
            "scan.mode": lambda: random.choice(
                ["default", "latest-full", "latest", "compacted-full"]
            ),
            "full-compaction.delta-commits": lambda: str(random.choice([1, 3, 5, 10])),
            "tag.automatic-creation": lambda: random.choice(
                ["none", "process-time", "watermark"]
            ),
            "tag.creation-period": lambda: random.choice(
                ["daily", "hourly", "two-hours"]
            ),
            "tag.num-retained-max": lambda: str(random.choice([1, 3, 5, 10, 50])),
            "read-batch-size": lambda: str(random.choice([1024, 4096, 8192, 16384])),
            "local-sort.max-num-file-handles": lambda: str(
                random.choice([50, 128, 256, 512])
            ),
            "lookup.cache-file-retention": lambda: random.choice(["1h", "6h", "1d"]),
            "lookup.cache-max-disk-size": lambda: random.choice(
                ["64mb", "256mb", "1gb", "10gb"]
            ),
            "lookup.cache-max-memory-size": lambda: random.choice(
                ["64mb", "256mb", "1gb"]
            ),
        }
        if self.write_format == FileFormat.ORC:
            next_properties.update(
                {
                    "orc.compress": lambda: random.choice(
                        ["zlib", "snappy", "zstd", "lz4", "none"]
                    ),
                }
            )
        elif self.write_format == FileFormat.Parquet:
            next_properties.update(
                {
                    "parquet.compression": lambda: random.choice(
                        ["snappy", "gzip", "zstd", "lz4", "uncompressed"]
                    ),
                }
            )
        return next_properties

    def generate_extra_statement(
        self,
        spark: SparkSession,
        table: SparkTable,
    ) -> str:
        next_option = random.randint(1, 9)

        if next_option == 1:
            return f"CALL `{table.catalog_name}`.sys.compact(table => '{table.get_namespace_path()}');"
        if next_option == 2:
            return f"CALL `{table.catalog_name}`.sys.expire_snapshots(table => '{table.get_namespace_path()}', retain_max => {random.choice([1, 2, 5, 10])});"
        if next_option == 3:
            tag_name = f"tag_{random.randint(1, 1000)}"
            return f"CALL `{table.catalog_name}`.sys.create_tag(table => '{table.get_namespace_path()}', tag => '{tag_name}');"
        if next_option == 4:
            return f"CALL `{table.catalog_name}`.sys.repair(table => '{table.get_namespace_path()}');"
        if next_option == 5:
            return f"CALL `{table.catalog_name}`.sys.remove_orphan_files(table => '{table.get_namespace_path()}');"
        if next_option == 6:
            # Rollback — executed here so the in-memory model can be refreshed
            # afterward (rollback can undo schema-mutating alters)
            version = random.choice(
                [str(random.randint(1, 10)), f"tag_{random.randint(1, 1000)}"]
            )
            spark.sql(
                f"CALL `{table.catalog_name}`.sys.rollback(table => '{table.get_namespace_path()}', version => '{version}')"
            )
            self._refresh_table_model(spark, table)
            return ""
        if next_option == 7:
            return f"CALL `{table.catalog_name}`.sys.delete_tag(table => '{table.get_namespace_path()}', tag => 'tag_{random.randint(1, 1000)}');"
        if next_option == 8:
            branch = f"b_{random.randint(1, 3)}"
            if random.randint(1, 2) == 1:
                res = f"CALL `{table.catalog_name}`.sys.create_branch(table => '{table.get_namespace_path()}', branch => '{branch}'"
                if random.randint(1, 2) == 1:
                    res += f", tag => 'tag_{random.randint(1, 1000)}'"
                return res + ");"
            return f"CALL `{table.catalog_name}`.sys.delete_branch(table => '{table.get_namespace_path()}', branch => '{branch}');"
        if next_option == 9:
            return f"CALL `{table.catalog_name}`.sys.expire_partitions(table => '{table.get_namespace_path()}', expiration_time => '{random.choice(['1 h', '1 d', '7 d'])}');"
        return ""
