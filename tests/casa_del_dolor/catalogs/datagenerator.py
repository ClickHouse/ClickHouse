import random
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, date
import json
import logging
import math
import string
import sys
import threading
import traceback
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
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
    CharType,
    VarcharType,
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
    DataType,
)

try:
    from pyspark.sql.types import VariantType, VariantVal  # noqa: F401  (import probe)

    HAS_VARIANT_TYPE = True
except ImportError:
    HAS_VARIANT_TYPE = False

try:
    from pyspark.sql.types import TimestampNTZType

    HAS_TIMESTAMP_NTZ = True
except ImportError:
    HAS_TIMESTAMP_NTZ = False

from .tablegenerator import LakeTableGenerator
from .clickhousetospark import ClickHouseTypeMapper

from .laketables import SparkTable, LakeFormat

SOME_STRINGS = [
    "",
    "0",
    "1",
    "-1",
    "/",
    "_",
    "%",
    "*",
    '"',
    "\\'",
    "\\0",
    "\\'",
    "\\t",
    "\\n",
    "null",
    "NULL",
    "is",
    "was",
    "are",
    "be",
    "have",
    "had",
    "were",
    "can",
    "said",
    "use",
    "do",
    "will",
    "would",
    "make",
    "like",
    "has",
    "look",
    "write",
    "go",
    "see",
    "could",
    "been",
    "call",
    "am",
    "find",
    "did",
    "get",
    "come",
    "made",
    "may",
    "take",
    "know",
    "live",
    "give",
    "think",
    "say",
    "help",
    "tell",
    "follow",
    "came",
    "want",
    "show",
    "set",
    "put",
    "does",
    "must",
    "ask",
    "went",
    "read",
    "need",
    "move",
    "try",
    "change",
    "play",
    "spell",
    "found",
    "study",
    "learn",
    "should",
    "add",
    "keep",
    "start",
    "thought",
    "saw",
    "turn",
    "might",
    "close",
    "seem",
    "open",
    "begin",
    "got",
    "run",
    "walk",
    "began",
    "grow",
    "took",
    "carry",
    "hear",
    "stop",
    "miss",
    "eat",
    "watch",
    "let",
    "cut",
    "talk",
    "being",
    "leave",
    "water",
    "day",
    "part",
    "sound",
    "work",
    "place",
    "year",
    "back",
    "thing",
    "name",
    "sentence",
    "man",
    "line",
    "boy",
    "认识你很高兴",
    "美国",
    "叫",
    "名字",
    "你们",
    "日本",
    "哪国人",
    "爸爸",
    "兄弟姐妹",
    "漂亮",
    "照片",
    "😉",
    "😊😊",
    "😛😛😛😛",
]


def _to_json_safe(obj):
    """Recursively convert a value to JSON-safe types."""
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, float) and not math.isfinite(obj):
        # `json.dumps` emits bare NaN/Infinity tokens, which are not valid JSON
        # and Spark's `parse_json` rejects them, so keep them as strings
        return str(obj)
    if isinstance(obj, (int, float, str)):
        return obj
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, Row):
        return {k: _to_json_safe(v) for k, v in obj.asDict().items()}
    if isinstance(obj, dict):
        return {str(_to_json_safe(k)): _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_safe(v) for v in obj]
    return str(obj)


def _to_hashable(value):
    """Deep-convert lists to tuples so a value can be used as a dict key (map keys
    of ARRAY type come back as Python lists). PySpark accepts tuples for ArrayType."""
    if isinstance(value, list):
        return tuple(_to_hashable(v) for v in value)
    return value


class LakeDataGenerator:
    def __init__(self, query_logger):
        self._thread_local = threading.local()
        self._thread_local._min_nested = 0
        self._thread_local._max_nested = 100
        self._thread_local._min_str_len = 0
        self._thread_local._max_str_len = 100
        self.logger = logging.getLogger(__name__)
        self.spark_query_logger = query_logger
        self.type_generator = ClickHouseTypeMapper()

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

    # Control characters, NUL, quotes, escapes, RTL override, ZWJ, combining accent,
    # multi-byte and surrogate-pair characters
    NASTY_ALPHABET = "\x00\x01\x07\x08\x0b\x0c\x1b\x7f\t\n\r'\"`\\́‍‮漂亮😉"

    def _rand_string(self, nlen):
        r = random.randint(1, 100)
        if r <= 50:
            next_str = random.choice(SOME_STRINGS)
            if len(next_str) <= nlen:
                return next_str
        alphabet = (
            LakeDataGenerator.NASTY_ALPHABET
            if r > 90
            else string.ascii_letters + string.digits + " _-"
        )
        return "".join(random.choice(alphabet) for _ in range(nlen))

    def _rand_binary(self, nlen):
        return bytes(random.getrandbits(8) for _ in range(nlen))

    def _rand_date(self):
        if random.randint(1, 100) < 16:
            # Today's date
            return date.today()
        reduced_limit = random.randint(1, 2) == 1
        start = date(2000 if reduced_limit else 1, 1, 1).toordinal()
        end = date(2000 if reduced_limit else 9999, 12, 31).toordinal()
        return date.fromordinal(self._rand_int(start, end))

    def _rand_timestamp(self):
        if random.randint(1, 100) < 21:
            # Timestamp related to now
            return datetime.now() + timedelta(seconds=random.randint(-60, 60))
        reduced_limit = random.randint(1, 2) == 1
        start = datetime(2000 if reduced_limit else 1, 1, 1)
        end = datetime(2000 if reduced_limit else 9999, 12, 31)
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
        if random.randint(1, 100) <= 10:
            # All-9s extreme at full precision
            int_part = max(0, max_int)
            frac_part = 10**scale - 1 if scale > 0 else 0
        else:
            int_part = self._rand_int(0, max(0, max_int))
            frac_part = self._rand_int(0, 10**scale - 1) if scale > 0 else 0
        if scale > 0:
            s = f"{int_part}.{frac_part:0{scale}d}"
        else:
            s = f"{int_part}"
        # Apply the sign to the assembled string: `sign * int_part` loses it whenever
        # int_part is 0, so e.g. DECIMAL(p, p) values were never negative
        if random.random() < 0.5:
            s = "-" + s
        return Decimal(s)

    INT_LIMITS = {
        ByteType: [-128, 127],
        ShortType: [-32768, 32767],
        IntegerType: [-2_147_483_648, 2_147_483_647],
        LongType: [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807],
    }

    def _take_nested_budget(self, n: int) -> int:
        # Nested containers multiply per-level counts (map of arrays of maps...), so an
        # unlucky min/max_nested draw explodes combinatorially. Cap the TOTAL container
        # entries per row; once exhausted, deeper containers come out empty.
        budget = getattr(self._thread_local, "_nested_budget", None)
        if budget is None:
            return n
        n = min(n, budget)
        self._thread_local._nested_budget = budget - n
        return n

    def _random_value_for_type(self, dtype: DataType, null_rate: float):
        """Return a random Python value that conforms to the given Spark DataType."""
        if random.random() < null_rate:
            return None
        if isinstance(dtype, BooleanType):
            return self._rand_bool()
        if isinstance(dtype, (ByteType, ShortType, IntegerType, LongType)):
            next_limits = LakeDataGenerator.INT_LIMITS[type(dtype)]
            r = random.randint(1, 100)
            if r <= 10:
                # Exact boundaries are almost never hit by uniform draws, but they are
                # what stresses readers and narrowing casts
                return random.choice(
                    [
                        next_limits[0],
                        next_limits[0] + 1,
                        -1,
                        0,
                        1,
                        next_limits[1] - 1,
                        next_limits[1],
                    ]
                )
            # Try reduced limits
            if r <= 55:
                return self._rand_int(-100, 100)
            return self._rand_int(next_limits[0], next_limits[1])
        if isinstance(dtype, FloatType):
            if random.randint(1, 100) <= 5:
                # float32 boundaries and denormals; also values that overflow on
                # Float64 -> Float32 narrowing
                return random.choice(
                    [3.4028235e38, -3.4028235e38, 1.17549435e-38, 1.4e-45, -1.4e-45]
                )
            return float(self._rand_float(-1e5, 1e5))
        if isinstance(dtype, DoubleType):
            if random.randint(1, 100) <= 5:
                # float64 boundaries and denormals
                return random.choice(
                    [
                        sys.float_info.max,
                        -sys.float_info.max,
                        sys.float_info.min,
                        5e-324,
                        -5e-324,
                    ]
                )
            return float(self._rand_float(-1e9, 1e9))
        if isinstance(dtype, DecimalType):
            return self._rand_decimal(dtype.precision, dtype.scale)
        if isinstance(dtype, StringType):
            return self._rand_string(
                random.randint(
                    self._thread_local._min_str_len, self._thread_local._max_str_len
                )
            )
        if isinstance(dtype, (CharType, VarcharType)):
            return self._rand_string(
                random.randint(
                    min(dtype.length, self._thread_local._min_str_len),
                    min(dtype.length, self._thread_local._max_str_len),
                )
            )
        if isinstance(dtype, BinaryType):
            return self._rand_binary(
                random.randint(
                    self._thread_local._min_str_len, self._thread_local._max_str_len
                )
            )
        if isinstance(dtype, DateType):
            return self._rand_date()
        if isinstance(dtype, TimestampType):
            return self._rand_timestamp()
        if HAS_TIMESTAMP_NTZ and isinstance(dtype, TimestampNTZType):
            return self._rand_timestamp()
        if HAS_VARIANT_TYPE and isinstance(dtype, VariantType):
            # Spark stores variants as self-describing values, so any type works.
            inner_type = self.type_generator.generate_random_spark_type(
                allow_variant=False, max_depth=random.randint(1, 5)
            )
            val = self._random_value_for_type(inner_type, null_rate)
            return None if val is None else json.dumps(_to_json_safe(val))
        if isinstance(dtype, ArrayType):
            # Arrays of variable length
            elem_null_rate = null_rate if dtype.containsNull else 0.0
            n = self._take_nested_budget(
                random.randint(
                    self._thread_local._min_nested, self._thread_local._max_nested
                )
            )
            return [
                self._random_value_for_type(dtype.elementType, elem_null_rate)
                for _ in range(n)
            ]
        if isinstance(dtype, MapType):
            # pyspark's MapType.toInternal rebuilds the dict converting each key; an ArrayType
            # key that needs conversion (e.g. ARRAY<DATE>) comes back as an unhashable list,
            # so such maps can only be produced empty.
            if isinstance(dtype.keyType, ArrayType) and dtype.keyType.needConversion():
                return {}
            # Keys: must be non-null and hashable; values may be null only if allowed
            value_null_rate = null_rate if dtype.valueContainsNull else 0.0
            n = self._take_nested_budget(
                random.randint(
                    self._thread_local._min_nested, self._thread_local._max_nested
                )
            )
            out = {}
            attempts = 0
            # Keep drawing until we have n unique, non-null keys (cap attempts)
            while len(out) < n and attempts < n * 5:
                attempts += 1
                k = self._random_value_for_type(
                    dtype.keyType, 0.0
                )  # NEVER null for keys
                if k is None:
                    continue
                # ClickHouse/Spark allow ARRAY map keys (e.g. Map(Array(Date32), ...)),
                # which come back as Python lists - unhashable as dict keys. Deep-convert
                # lists to tuples (PySpark accepts any sequence for ArrayType). Skip keys
                # that remain unhashable (e.g. bytearray for BINARY, dict for MAP keys)
                # rather than fail - the map just ends up with fewer entries.
                k = _to_hashable(k)
                try:
                    hash(k)
                except TypeError:
                    continue
                v = self._random_value_for_type(dtype.valueType, value_null_rate)
                out[k] = v
            return out
        if isinstance(dtype, StructType):
            obj = {}
            for f in dtype.fields:
                nr = null_rate if f.nullable else 0.0
                obj[f.name] = self._random_value_for_type(f.dataType, nr)
            return Row(**obj)
        # Surface missing branches (e.g. a new Spark version adding a type) instead
        # of silently degrading to strings
        self.logger.warning(
            f"No value generator for Spark type {dtype}, falling back to a random string"
        )
        return self._rand_string(
            random.randint(
                self._thread_local._min_str_len, self._thread_local._max_str_len
            )
        )

    def _map_type_to_insert(self, dtype):
        # Char and Varchar have to be Strings
        if isinstance(dtype, (CharType, VarcharType)) or (
            HAS_VARIANT_TYPE and isinstance(dtype, VariantType)
        ):
            return StringType()
        if isinstance(dtype, ArrayType):
            return ArrayType(
                self._map_type_to_insert(dtype.elementType),
                containsNull=dtype.containsNull,
            )
        if isinstance(dtype, MapType):
            return MapType(
                self._map_type_to_insert(dtype.keyType),
                self._map_type_to_insert(dtype.valueType),
                valueContainsNull=dtype.valueContainsNull,
            )
        if isinstance(dtype, StructType):
            return StructType(
                [
                    StructField(
                        name=f.name,
                        dataType=self._map_type_to_insert(f.dataType),
                        nullable=f.nullable,
                    )
                    for f in dtype.fields
                ]
            )
        return dtype

    def _contains_variant(self, dtype):
        """Check if a type contains VariantType anywhere in its tree."""
        if HAS_VARIANT_TYPE and isinstance(dtype, VariantType):
            return True
        if isinstance(dtype, StructType):
            return any(self._contains_variant(f.dataType) for f in dtype.fields)
        if isinstance(dtype, ArrayType):
            return self._contains_variant(dtype.elementType)
        if isinstance(dtype, MapType):
            return self._contains_variant(dtype.keyType) or self._contains_variant(
                dtype.valueType
            )
        return False

    def _build_variant_conversion(self, col_expr, original_dtype):
        """Build a Column expression that recursively converts string placeholders to VariantType."""
        if HAS_VARIANT_TYPE and isinstance(original_dtype, VariantType):
            return F.parse_json(col_expr)

        if isinstance(original_dtype, StructType):
            if not self._contains_variant(original_dtype):
                return col_expr
            fields = []
            for field in original_dtype.fields:
                converted = self._build_variant_conversion(
                    col_expr[field.name], field.dataType
                )
                fields.append(converted.alias(field.name))
            return F.struct(*fields)

        if isinstance(original_dtype, ArrayType):
            if not self._contains_variant(original_dtype.elementType):
                return col_expr
            return F.transform(
                col_expr,
                lambda x: self._build_variant_conversion(x, original_dtype.elementType),
            )

        if isinstance(original_dtype, MapType):
            # Keys shouldn't be variant, but handle values
            if not self._contains_variant(original_dtype.valueType):
                return col_expr
            return F.transform_values(
                col_expr,
                lambda k, v: self._build_variant_conversion(
                    v, original_dtype.valueType
                ),
            )

        return col_expr

    def _create_random_df(self, spark: SparkSession, table: SparkTable, n_rows: int):
        """
        Build a DataFrame of random rows for the given schema (types as strings are fine).
        """
        # Set limits
        self._thread_local._min_nested = random.randint(0, 100)
        self._thread_local._max_nested = max(
            self._thread_local._min_nested, random.randint(0, 100)
        )
        self._thread_local._min_str_len = random.randint(0, 100)
        self._thread_local._max_str_len = max(
            self._thread_local._min_str_len, random.randint(0, 100)
        )
        null_rate: float = 0.05 if random.randint(1, 2) == 1 else 0.0

        struct1 = StructType(
            [
                StructField(
                    name=cname,
                    dataType=val.spark_type,
                    nullable=val.nullable,
                )
                for cname, val in table.columns.items()
                if not val.generated
            ]
        )
        struct2 = StructType(
            [
                StructField(
                    name=cname,
                    dataType=self._map_type_to_insert(val.spark_type),
                    nullable=val.nullable,
                )
                for cname, val in table.columns.items()
                if not val.generated
            ]
        )
        rows = []
        for _ in range(n_rows):
            self._thread_local._nested_budget = 10_000
            rec = {}
            for f in struct1.fields:
                nr = null_rate if f.nullable else 0.0
                rec[f.name] = self._random_value_for_type(f.dataType, nr)
            rows.append(Row(**rec))
        # Use explicit schema so types match exactly
        df = spark.createDataFrame(rows, schema=struct2)
        if HAS_VARIANT_TYPE:
            for f in struct1.fields:
                if self._contains_variant(f.dataType):
                    df = df.withColumn(
                        f.name,
                        self._build_variant_conversion(F.col(f.name), f.dataType),
                    )
        return df

    def _random_predicate(self, table: SparkTable) -> str:
        # Simple, always-valid filter for WHERE / overwrite conditions.
        key = random.choice(list(table.flat_columns().keys()))
        return f"{key} IS{random.choice(['', ' NOT'])} NULL"

    @staticmethod
    def _float_literal(v: float, sql_type: str) -> str:
        """Spark parses bare `nan`/`inf` tokens as column references, so special
        values must go through a quoted-string cast."""
        if math.isnan(v):
            return f"CAST('NaN' AS {sql_type})"
        if math.isinf(v):
            return f"CAST('{'Infinity' if v > 0 else '-Infinity'}' AS {sql_type})"
        return f"CAST({v!r} AS {sql_type})"

    def _sql_scalar_literal(self, dtype: DataType):
        """A SQL literal for a scalar type, or None for container/variant types
        whose literals are impractical to spell out inline."""
        if isinstance(dtype, BooleanType):
            return "true" if self._rand_bool() else "false"
        if isinstance(dtype, (ByteType, ShortType, IntegerType, LongType)):
            return str(self._rand_int(-100, 100))
        if isinstance(dtype, FloatType):
            return self._float_literal(float(self._rand_float(-1e5, 1e5)), "FLOAT")
        if isinstance(dtype, DoubleType):
            return self._float_literal(float(self._rand_float(-1e9, 1e9)), "DOUBLE")
        if isinstance(dtype, DecimalType):
            return (
                f"CAST('{self._rand_decimal(dtype.precision, dtype.scale)}'"
                f" AS DECIMAL({dtype.precision}, {dtype.scale}))"
            )
        if isinstance(dtype, (StringType, CharType, VarcharType)):
            s = (
                self._rand_string(random.randint(0, 16))
                .replace("\\", "\\\\")
                .replace("'", "\\'")
            )
            return f"'{s}'"
        if isinstance(dtype, DateType):
            return f"DATE '{self._rand_date().isoformat()}'"
        if HAS_TIMESTAMP_NTZ and isinstance(dtype, TimestampNTZType):
            return f"TIMESTAMP_NTZ '{self._rand_timestamp().strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        if isinstance(dtype, TimestampType):
            return (
                f"TIMESTAMP '{self._rand_timestamp().strftime('%Y-%m-%d %H:%M:%S.%f')}'"
            )
        if isinstance(dtype, BinaryType):
            return f"X'{self._rand_binary(random.randint(0, 8)).hex()}'"
        return None

    def insert_random_data(self, spark: SparkSession, table: SparkTable):
        nrows: int = random.randint(0, 100)
        tpath = table.get_table_full_path()
        self.logger.info(f"Inserting {nrows} row(s) into {tpath}")
        df = self._create_random_df(spark, table, nrows)
        df.writeTo(tpath).append()

    def run_query(self, session, query: str):
        self.logger.info(f"Running query: {query}")
        # Ignore spark_query_logger at the moment because this is multithreaded
        # with open(self.spark_query_logger, "a") as f:
        #    f.write(query + "\n")
        # session.sql takes a single statement; the Iceberg/Delta SQL-extension grammars are
        # stricter than Spark's base parser and reject a trailing ';' with "expecting <EOF>".
        session.sql(query.strip().rstrip(";"))

    def merge_into_table(self, spark: SparkSession, table: SparkTable):
        nrows: int = random.randint(0, 100)
        tpath = table.get_table_full_path()
        self.logger.info(f"Merging {nrows} row(s) into {tpath}")
        df = self._create_random_df(spark, table, nrows)
        df.createOrReplaceTempView("updates")

        to_update = list(table.flat_columns().keys())
        random.shuffle(to_update)
        next_pick = random.choice(to_update)
        if random.randint(1, 100) <= 70:
            to_update = random.sample(to_update, random.randint(1, len(to_update)))

        match_options = [
            "DELETE",
            "UPDATE SET *",
            f"UPDATE SET {','.join([f't.{cname} = s.{cname}' for cname in to_update])}",
        ]
        self.run_query(
            spark,
            f"MERGE INTO {tpath} AS t USING updates AS s ON t.{next_pick} = s.{next_pick}\
 WHEN MATCHED THEN {random.choice(match_options)}{' WHEN NOT MATCHED BY TARGET THEN INSERT *' if random.randint(1, 4) == 1 else ''}\
{' WHEN NOT MATCHED BY SOURCE THEN DELETE' if random.randint(1, 4) == 1 else ''};",
        )

    def delete_table(self, spark: SparkSession, table: SparkTable):
        tpath = table.get_table_full_path()
        self.logger.info(f"Delete from table {tpath}")
        self.run_query(
            spark,
            f"DELETE FROM {tpath} WHERE {self._random_predicate(table)};",
        )

    # Top-level Spark column types eligible for an equality-delete key. Each maps
    # to a primitive Iceberg type whose value `_to_iceberg_value` can build.
    _EQ_DELETE_TYPES = (
        StringType,
        CharType,
        VarcharType,
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType,
        DateType,
        BinaryType,
    )

    def _to_iceberg_value(self, jvm, spark_type: DataType, value):
        # Build the exact Java object Iceberg's `GenericRecord` expects for this
        # column type. py4j maps Python str/int/bool straight through, but sends
        # `float` as Java double (wrong for a FLOAT field) and `int` as Integer
        # (wrong for a LONG field), and does not convert date / Decimal / bytes at
        # all -- so those are constructed explicitly. Returns None for a type we
        # do not handle, so the caller skips it.
        if isinstance(spark_type, (StringType, CharType, VarcharType)):
            return str(value)
        if isinstance(spark_type, BooleanType):
            return jvm.java.lang.Boolean.valueOf(bool(value))
        if isinstance(spark_type, (ByteType, ShortType, IntegerType)):
            return jvm.java.lang.Integer.valueOf(int(value))
        if isinstance(spark_type, LongType):
            return jvm.java.lang.Long.valueOf(int(value))
        if isinstance(spark_type, FloatType):
            return jvm.java.lang.Float.valueOf(float(value))
        if isinstance(spark_type, DoubleType):
            return jvm.java.lang.Double.valueOf(float(value))
        if isinstance(spark_type, DateType):
            return jvm.java.time.LocalDate.of(
                int(value.year), int(value.month), int(value.day)
            )
        if isinstance(spark_type, DecimalType):
            # Iceberg validates that the BigDecimal scale equals the field scale.
            return jvm.java.math.BigDecimal(str(value)).setScale(
                spark_type.scale, jvm.java.math.RoundingMode.HALF_UP
            )
        if isinstance(spark_type, BinaryType):
            return jvm.java.nio.ByteBuffer.wrap(bytes(value))
        return None

    def _synthetic_py_value(self, spark_type: DataType):
        # A Python value used only when the table has no rows to reuse, just so
        # the delete file is not empty; it need not match any row.
        if isinstance(spark_type, (StringType, CharType, VarcharType)):
            return self._rand_string(random.randint(1, 8))
        if isinstance(spark_type, BooleanType):
            return random.randint(0, 1) == 1
        if isinstance(spark_type, (ByteType, ShortType, IntegerType, LongType)):
            return random.randint(-1000, 1000)
        if isinstance(spark_type, (FloatType, DoubleType)):
            return random.uniform(-1000.0, 1000.0)
        if isinstance(spark_type, DateType):
            return self._rand_date()
        if isinstance(spark_type, DecimalType):
            return Decimal(0)  # always fits any precision/scale
        if isinstance(spark_type, BinaryType):
            return self._rand_binary(random.randint(1, 8))
        return None

    def write_equality_delete(self, spark: SparkSession, table: SparkTable):
        # Spark SQL only ever writes *position* deletes; *equality* deletes must
        # be produced through the Iceberg Java API (reached here via the PySpark
        # JVM gateway). This exercises the equality-delete read path in ClickHouse
        # and, on a coin flip, makes the delete file's column nullability differ
        # from the table schema -- the shape that crashed the server in
        # https://github.com/ClickHouse/ClickHouse/pull/109551 (a required column
        # in the table declared optional in the delete file, or vice versa).
        tpath = table.get_table_full_path()

        # Equality deletes match on top-level primitive columns.
        candidates = [
            name
            for name, col in table.columns.items()
            if not col.generated and isinstance(col.spark_type, self._EQ_DELETE_TYPES)
        ]
        if not candidates:
            self.insert_random_data(spark, table)
            return
        col_name = random.choice(candidates)
        col_type = table.columns[col_name].spark_type

        # Reuse real values when the table has any, otherwise a synthetic one so
        # the delete file is never empty. Matching real rows is not required to
        # exercise the read path (the set is built from the delete file
        # regardless), but it keeps the delete meaningful for result checking.
        try:
            rows = spark.sql(
                f"SELECT DISTINCT `{col_name}` FROM {tpath} "
                f"WHERE `{col_name}` IS NOT NULL LIMIT {random.randint(1, 10)}"
            ).collect()
        except Exception:
            rows = []
        py_values = [r[0] for r in rows if r[0] is not None]
        if not py_values:
            synthetic = self._synthetic_py_value(col_type)
            if synthetic is None:
                self.insert_random_data(spark, table)
                return
            py_values = [synthetic]

        jvm = spark.sparkContext._jvm
        gateway = spark.sparkContext._gateway

        java_values = [
            v
            for v in (self._to_iceberg_value(jvm, col_type, pv) for pv in py_values)
            if v is not None
        ]
        if not java_values:
            self.insert_random_data(spark, table)
            return

        # Make the delete-file column nullability match (False) or deliberately
        # differ from (True) the table schema.
        flip_nullability = random.randint(1, 2) == 1
        self.logger.info(
            "Writing Iceberg equality delete on %s.`%s` (%d value(s), flip_nullability=%s)",
            tpath,
            col_name,
            len(java_values),
            flip_nullability,
        )

        # Resolve the Iceberg table through Spark's catalog so this works for any
        # registered Iceberg catalog (hadoop / hive / rest / nessie / glue),
        # instead of assuming a hadoop warehouse path.
        iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, tpath
        )
        schema = iceberg_table.schema()
        field = schema.findField(col_name)
        field_id = field.fieldId()

        types = jvm.org.apache.iceberg.types.Types
        make_required = field.isRequired() != flip_nullability
        if make_required:
            delete_field = types.NestedField.required(field_id, col_name, field.type())
        else:
            delete_field = types.NestedField.optional(field_id, col_name, field.type())
        delete_schema = jvm.org.apache.iceberg.Schema([delete_field])

        equality_ids = gateway.new_array(jvm.int, 1)
        equality_ids[0] = field_id

        appender_factory = jvm.org.apache.iceberg.data.GenericAppenderFactory(
            schema, iceberg_table.spec(), equality_ids, delete_schema, None
        )
        # Delete files may be Parquet, ORC or Avro (recorded per-file in the
        # manifest), independently of the data file format -- vary it so all three
        # delete-file reader paths get exercised.
        delete_format = random.choice(["parquet", "orc", "avro"])
        file_format = getattr(jvm.org.apache.iceberg.FileFormat, delete_format.upper())
        file_name = (
            "eq-delete-"
            + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
            + f".{delete_format}"
        )
        out = iceberg_table.io().newOutputFile(
            iceberg_table.locationProvider().newDataLocation(file_name)
        )
        writer = appender_factory.newEqDeleteWriter(
            jvm.org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(
                out
            ),
            file_format,
            None,
        )
        try:
            for value in java_values:
                record = jvm.org.apache.iceberg.data.GenericRecord.create(delete_schema)
                record.setField(col_name, value)
                writer.write(record)
        finally:
            writer.close()
        iceberg_table.newRowDelta().addDeletes(writer.toDeleteFile()).commit()

    def shallow_clone_table(self, spark: SparkSession, table: SparkTable):
        # Delta SHALLOW CLONE makes a table's log reference data files by ABSOLUTE
        # path from another location instead of copying them. To exercise
        # ClickHouse reading such foreign-path references on a table it already
        # knows -- without registering a new table on the ClickHouse side:
        #   1. DEEP CLONE the table into a scratch table, so the scratch owns a
        #      physical copy of the files at a different location;
        #   2. CREATE OR REPLACE the table as a SHALLOW CLONE of the scratch.
        # The table keeps its identity, schema and rows (so both sides stay in
        # sync and the oracle stays valid), but its log now points at the
        # scratch table's directory.
        #
        # The scratch table's files become load-bearing for the table after
        # step 2, so it must not be dropped; give it a unique name and leave it.
        # It is never registered, so the rest of dolor (tablecheck, BuzzHouse)
        # never touches it, and the run's teardown removes the whole warehouse.
        tpath = table.get_table_full_path()
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        scratch = f"{table.catalog_name}.test.`{table.table_name}_shallow_src_{suffix}`"
        self.logger.info(
            "Delta SHALLOW CLONE round-trip on %s (via %s)", tpath, scratch
        )
        self.run_query(spark, f"CREATE TABLE {scratch} DEEP CLONE {tpath};")
        self.run_query(
            spark, f"CREATE OR REPLACE TABLE {tpath} SHALLOW CLONE {scratch};"
        )

    def add_files_to_table(self, spark: SparkSession, table: SparkTable):
        # Iceberg `add_files` imports EXTERNALLY-authored data files (files with no
        # Iceberg field-IDs) by reference, mapping columns by name -- a distinct
        # read path from files written by Iceberg's own writer. Write a small batch
        # of random rows as plain Parquet/ORC/Avro to a sibling path, then CALL
        # add_files to pull them in.
        #
        # add_files references the imported files in place, so the scratch path is
        # load-bearing afterwards and is left in place (unique per call, tiny, and
        # invisible to the rest of dolor); the run's teardown removes the warehouse.
        tpath = table.get_table_full_path()
        nrows = random.randint(1, 100)
        src_format = random.choice(["parquet", "orc", "avro"])
        df = self._create_random_df(spark, table, nrows)

        jvm = spark.sparkContext._jvm
        # Base the scratch path on the table's own location so it lands on the same
        # filesystem / URI scheme (local, s3, azure) with no extra configuration.
        iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, tpath
        )
        location = str(iceberg_table.location()).rstrip("/")
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        src_path = f"{location}_add_files_src_{suffix}"

        self.logger.info(
            "Iceberg add_files into %s from %d %s row(s) at %s",
            tpath,
            nrows,
            src_format,
            src_path,
        )
        df.write.format(src_format).mode("overwrite").save(src_path)
        self.run_query(
            spark,
            f"CALL `{table.catalog_name}`.system.add_files("
            f"table => '{table.get_namespace_path()}', "
            f"source_table => '`{src_format}`.`{src_path}`')",
        )

    def update_rows(self, spark: SparkSession, table: SparkTable):
        # Standalone UPDATE ... SET ... WHERE (distinct from the UPDATE inside
        # MERGE). SET targets are top-level, non-generated columns; scalar
        # columns get a typed literal, complex-typed columns are nulled when
        # nullable (their literals are impractical to spell inline).
        tpath = table.get_table_full_path()
        self.logger.info(f"Update rows in {tpath}")
        targets = [c for c, sc in table.columns.items() if not sc.generated]
        random.shuffle(targets)
        assignments = []
        for c in targets[: random.randint(1, max(1, len(targets)))]:
            lit = self._sql_scalar_literal(table.columns[c].spark_type)
            if lit is not None:
                assignments.append(f"{c} = {lit}")
            elif table.columns[c].nullable:
                assignments.append(f"{c} = NULL")
        if not assignments:
            return
        self.run_query(
            spark,
            f"UPDATE {tpath} SET {', '.join(assignments)} WHERE {self._random_predicate(table)};",
        )

    def insert_replace_where(self, spark: SparkSession, table: SparkTable):
        # Delta conditional overwrite. The newly written rows need not satisfy
        # the predicate for fuzzing, so relax Delta's constraint check (the
        # session is per-operation and stopped right after, so this conf does
        # not leak to other work).
        nrows: int = random.randint(0, 100)
        tpath = table.get_table_full_path()
        self.logger.info(f"INSERT REPLACE WHERE {nrows} row(s) into {tpath}")
        df = self._create_random_df(spark, table, nrows)
        view_name = f"replace_src_{table.table_name}"
        df.createOrReplaceTempView(view_name)
        spark.conf.set(
            "spark.databricks.delta.replaceWhere.constraintCheck.enabled", "false"
        )
        self.run_query(
            spark,
            f"INSERT INTO {tpath} REPLACE WHERE {self._random_predicate(table)} SELECT * FROM {view_name};",
        )

    def overwrite_partitions_data(self, spark: SparkSession, table: SparkTable):
        # Dynamic partition overwrite via DataFrameWriterV2: replaces only the
        # partitions present in the new data (full overwrite when unpartitioned).
        nrows: int = random.randint(0, 100)
        tpath = table.get_table_full_path()
        self.logger.info(f"Overwrite partitions of {tpath} with {nrows} row(s)")
        df = self._create_random_df(spark, table, nrows)
        df.writeTo(tpath).overwritePartitions()

    def overwrite_where_data(self, spark: SparkSession, table: SparkTable):
        # Conditional overwrite via DataFrameWriterV2: atomically deletes rows
        # matching the condition and appends the new data.
        nrows: int = random.randint(0, 100)
        tpath = table.get_table_full_path()
        self.logger.info(f"Overwrite rows of {tpath} with {nrows} row(s)")
        df = self._create_random_df(spark, table, nrows)
        df.writeTo(tpath).overwrite(F.expr(self._random_predicate(table)))

    def insert_into_branch(self, spark: SparkSession, table: SparkTable):
        # Iceberg branch write: create a branch and insert into it. The main
        # branch (what readers / tablecheck see) is left unchanged.
        branch = f"b_{random.randint(1, 3)}"
        tpath = table.get_table_full_path()
        nrows: int = random.randint(0, 100)
        self.logger.info(f"INSERT {nrows} row(s) into branch {branch} of {tpath}")
        self.run_query(
            spark, f"ALTER TABLE {tpath} CREATE BRANCH IF NOT EXISTS {branch};"
        )
        df = self._create_random_df(spark, table, nrows)
        view_name = f"branch_src_{table.table_name}"
        df.createOrReplaceTempView(view_name)
        self.run_query(
            spark,
            f"INSERT INTO {tpath}.branch_{branch} SELECT * FROM {view_name};",
        )

    def truncate_table(self, spark: SparkSession, table: SparkTable):
        tpath = table.get_table_full_path()
        self.logger.info(f"Truncate table {tpath}")
        self.run_query(spark, f"DELETE FROM {tpath};")

    def insert_overwrite_data(self, spark: SparkSession, table: SparkTable):
        nrows: int = random.randint(0, 100)
        tpath = table.get_table_full_path()
        self.logger.info(f"INSERT OVERWRITE {nrows} row(s) into {tpath}")
        df = self._create_random_df(spark, table, nrows)
        view_name = f"overwrite_src_{table.table_name}"
        df.createOrReplaceTempView(view_name)
        self.run_query(
            spark,
            f"INSERT OVERWRITE {tpath} SELECT * FROM {view_name};",
        )

    def update_table(self, spark: SparkSession, table: SparkTable) -> bool:
        next_operation = random.randint(1, 1100)

        is_iceberg = table.lake_format == LakeFormat.Iceberg
        is_delta = table.lake_format == LakeFormat.DeltaLake
        try:
            if next_operation <= 300:
                # Insert
                self.insert_random_data(spark, table)
            elif next_operation <= 340:
                # Standalone UPDATE ... SET ... WHERE
                self.update_rows(spark, table)
            elif next_operation <= 510:
                # Update and delete
                self.merge_into_table(spark, table)
            elif next_operation <= 550:
                # Delete
                self.delete_table(spark, table)
            elif next_operation <= 580:
                # Truncate
                self.truncate_table(spark, table)
            elif next_operation <= 610:
                # INSERT OVERWRITE (replaces all data)
                self.insert_overwrite_data(spark, table)
            elif next_operation <= 650:
                # Dynamic partition overwrite (DataFrameWriterV2)
                if is_iceberg or is_delta:
                    self.overwrite_partitions_data(spark, table)
                else:
                    self.insert_random_data(spark, table)
            elif next_operation <= 690:
                # Conditional overwrite (DataFrameWriterV2)
                if is_iceberg or is_delta:
                    self.overwrite_where_data(spark, table)
                else:
                    self.insert_random_data(spark, table)
            elif next_operation <= 710:
                # Delta INSERT INTO ... REPLACE WHERE
                if is_delta:
                    self.insert_replace_where(spark, table)
                else:
                    self.insert_random_data(spark, table)
            elif next_operation <= 730:
                # Iceberg branch write
                if is_iceberg:
                    self.insert_into_branch(spark, table)
                else:
                    self.insert_random_data(spark, table)
            elif next_operation <= 745:
                # Iceberg equality-delete file via the Java API (Spark SQL only
                # writes position deletes). Restricted to unpartitioned tables so
                # the writer can use a null partition tuple.
                if is_iceberg and not table.partitioned:
                    self.write_equality_delete(spark, table)
                else:
                    self.insert_random_data(spark, table)
            elif next_operation <= 755:
                # Delta SHALLOW CLONE round-trip: rewrites the table's log to
                # reference its data files by absolute foreign path. Only on
                # non-deterministic tables.
                if is_delta and not table.deterministic:
                    self.shallow_clone_table(spark, table)
                else:
                    self.insert_random_data(spark, table)
            elif next_operation <= 780:
                # Iceberg add_files: import externally-authored (no field-ID) data
                # files by reference, mapped by name. Unpartitioned only (a
                # partitioned target needs a Hive-layout source or partition_filter).
                if is_iceberg and not table.partitioned:
                    self.add_files_to_table(spark, table)
                else:
                    self.insert_random_data(spark, table)
            elif next_operation <= 970:
                # SQL Procedures or other statements specific for the lake
                next_table_generator = LakeTableGenerator.get_next_generator(
                    table.lake_format
                )
                self.logger.info(
                    f"Running SQL procedure on {table.get_table_full_path()}"
                )
                stmt = next_table_generator.generate_extra_statement(spark, table)
                if stmt:
                    self.run_query(spark, stmt)
            else:
                # Alter statements
                next_table_generator = LakeTableGenerator.get_next_generator(
                    table.lake_format
                )
                self.logger.info(
                    f"Running ALTER statement on {table.get_table_full_path()}"
                )
                stmt = next_table_generator.generate_alter_table_statements(
                    spark, table
                )
                if stmt:
                    self.run_query(spark, stmt)
        except Exception as e:
            # If an error happens, ignore it, but log it
            traceback.print_exc()
            self.logger.exception(e)
        return True
