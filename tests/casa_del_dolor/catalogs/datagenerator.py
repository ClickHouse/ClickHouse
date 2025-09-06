import random
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, date
import math
import logging
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
from .tablegenerator import LakeTableGenerator

from .laketables import SparkTable


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


class LakeDataGenerator:
    def __init__(self, query_logger):
        self._min_nested = 0
        self._max_nested = 100
        self._min_str_len = 0
        self._max_str_len = 100
        self.logger = logging.getLogger(__name__)
        self.spark_query_logger = query_logger

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

    def _rand_string(self, nlen):
        if random.randint(1, 2) == 1:
            next_str = random.choice(SOME_STRINGS)
            if len(next_str) <= nlen:
                return next_str
        alphabet = string.ascii_letters + string.digits + " _-"
        return "".join(random.choice(alphabet) for _ in range(nlen))

    def _rand_binary(self, nlen):
        return bytes(random.getrandbits(8) for _ in range(nlen))

    def _rand_date(self):
        reduced_limit = random.randint(1, 2) == 1
        start = date(2000 if reduced_limit else 1, 1, 1).toordinal()
        end = date(2000 if reduced_limit else 9999, 12, 31).toordinal()
        return date.fromordinal(self._rand_int(start, end))

    def _rand_timestamp(self):
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
        int_part = self._rand_int(0, max(0, max_int))
        frac_part = self._rand_int(0, 10**scale - 1) if scale > 0 else 0
        sign = -1 if random.random() < 0.5 else 1
        if scale > 0:
            s = f"{sign*int_part}.{frac_part:0{scale}d}"
        else:
            s = f"{sign*int_part}"
        return Decimal(s)

    INT_LIMITS = {
        ByteType: [-128, 127],
        ShortType: [-32768, 32767],
        IntegerType: [-2_147_483_648, 2_147_483_647],
        LongType: [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807],
    }

    def _random_value_for_type(self, dtype: DataType, null_rate: float):
        """Return a random Python value that conforms to the given Spark DataType."""
        if random.random() < null_rate:
            return None
        if isinstance(dtype, BooleanType):
            return self._rand_bool()
        if isinstance(dtype, (ByteType, ShortType, IntegerType, LongType)):
            # Try reduced limits
            if random.randint(1, 2) == 1:
                return self._rand_int(-100, 100)
            next_limits = LakeDataGenerator.INT_LIMITS[type(dtype)]
            return self._rand_int(next_limits[0], next_limits[1])
        if isinstance(dtype, FloatType):
            return float(self._rand_float(-1e5, 1e5))
        if isinstance(dtype, DoubleType):
            return float(self._rand_float(-1e9, 1e9))
        if isinstance(dtype, DecimalType):
            return self._rand_decimal(dtype.precision, dtype.scale)
        if isinstance(dtype, StringType):
            return self._rand_string(
                random.randint(self._min_str_len, self._max_str_len)
            )
        if isinstance(dtype, (CharType, VarcharType)):
            return self._rand_string(
                random.randint(
                    min(dtype.length, self._min_str_len),
                    min(dtype.length, self._max_str_len),
                )
            )
        if isinstance(dtype, BinaryType):
            return self._rand_binary(
                random.randint(self._min_str_len, self._max_str_len)
            )
        if isinstance(dtype, DateType):
            return self._rand_date()
        if isinstance(dtype, TimestampType):
            return self._rand_timestamp()
        if isinstance(dtype, ArrayType):
            # Arrays of variable length
            elem_null_rate = null_rate if dtype.containsNull else 0.0
            n = random.randint(self._min_nested, self._max_nested)
            return [
                self._random_value_for_type(dtype.elementType, elem_null_rate)
                for _ in range(n)
            ]
        if isinstance(dtype, MapType):
            # Keys: must be non-null and hashable; values may be null only if allowed
            value_null_rate = null_rate if dtype.valueContainsNull else 0.0
            n = random.randint(self._min_nested, self._max_nested)
            out = {}
            attempts = 0
            # Keep drawing until we have n unique, non-null keys (cap attempts)
            while len(out) < n and attempts < n * 5:
                k = self._random_value_for_type(
                    dtype.keyType, 0.0
                )  # NEVER null for keys
                if k is None:
                    attempts += 1
                    continue
                v = self._random_value_for_type(dtype.valueType, value_null_rate)
                out[k] = v
                attempts += 1
            return out
        if isinstance(dtype, StructType):
            obj = {}
            for f in dtype.fields:
                nr = null_rate if f.nullable else 0.0
                obj[f.name] = self._random_value_for_type(f.dataType, nr)
            return Row(**obj)
        return self._rand_string(random.randint(self._min_str_len, self._max_str_len))

    def _map_type_to_insert(self, dtype):
        # Char and Varchar have to be Strings
        if isinstance(dtype, (CharType, VarcharType)):
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

    def _create_random_df(self, spark: SparkSession, table: SparkTable, n_rows: int):
        """
        Build a DataFrame of random rows for the given schema (types as strings are fine).
        """
        # Set limits
        self._min_nested = random.randint(0, 100)
        self._max_nested = max(self._min_nested, random.randint(0, 100))
        self._min_str_len = random.randint(0, 100)
        self._max_str_len = max(self._min_str_len, random.randint(0, 100))
        null_rate: float = 0.05 if random.randint(1, 2) == 1 else 0.0

        struct1 = StructType(
            [
                StructField(
                    name=cname,
                    dataType=val.spark_type,
                    nullable=val.nullable,
                )
                for cname, val in table.columns.items()
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
            ]
        )
        rows = []
        for _ in range(n_rows):
            rec = {}
            for f in struct1.fields:
                nr = null_rate if f.nullable else 0.0
                rec[f.name] = self._random_value_for_type(f.dataType, nr)
            rows.append(Row(**rec))
        # Use explicit schema so types match exactly
        return spark.createDataFrame(rows, schema=struct2)

    def insert_random_data(self, spark: SparkSession, table: SparkTable):
        nrows: int = random.randint(0, 100)
        df = self._create_random_df(spark, table, nrows)
        self.logger.info(f"Inserting {nrows} row(s) into {table.get_table_full_path()}")
        df.writeTo(table.get_table_full_path()).append()

    def run_query(self, session, query: str):
        self.logger.info(f"Running query: {query}")
        with open(self.spark_query_logger, "a") as f:
            f.write(query + "\n")
        session.sql(query)

    def merge_into_table(self, spark: SparkSession, table: SparkTable):
        nrows: int = random.randint(0, 100)
        df = self._create_random_df(spark, table, nrows)
        df.createOrReplaceTempView("updates")

        to_update = list(table.columns.keys())
        random.shuffle(to_update)
        next_pick = random.choice(to_update)
        if random.randint(1, 100) <= 70:
            to_update = random.sample(to_update, random.randint(1, len(to_update)))

        match_options = [
            "DELETE",
            "UPDATE SET *",
            f"UPDATE SET {",".join([f"t.{cname} = s.{cname}" for cname in to_update])}",
        ]

        self.logger.info(f"Merging {nrows} row(s) into {table.get_table_full_path()}")
        self.run_query(
            spark,
            f"MERGE INTO {table.get_table_full_path()} AS t USING updates AS s ON t.{next_pick} = s.{next_pick}\
 WHEN MATCHED THEN {random.choice(match_options)}{" WHEN NOT MATCHED BY TARGET THEN INSERT *" if random.randint(1, 4) == 1 else ""}\
{f" WHEN NOT MATCHED BY SOURCE THEN DELETE" if random.randint(1, 4) == 1 else ""};",
        )

    def delete_table(self, spark: SparkSession, table: SparkTable):
        delete_key = random.choice(list(table.columns.keys()))
        predicate = f"{delete_key} IS{random.choice([""," NOT"])} NULL"

        self.logger.info(f"Delete from table {table.get_table_full_path()}")
        self.run_query(
            spark, f"DELETE FROM {table.get_table_full_path()} WHERE {predicate};"
        )

    def truncate_table(self, spark: SparkSession, table: SparkTable):
        self.logger.info(f"Truncate table {table.get_table_full_path()}")
        self.run_query(spark, f"DELETE FROM {table.get_table_full_path()};")

    def update_table(self, spark: SparkSession, table: SparkTable) -> bool:
        next_operation = random.randint(1, 1000)

        if next_operation <= 400:
            # Insert
            self.insert_random_data(spark, table)
        elif next_operation <= 600:
            # Update and delete
            self.merge_into_table(spark, table)
        elif next_operation <= 650:
            # Delete
            self.delete_table(spark, table)
        elif next_operation <= 700:
            # Truncate
            self.truncate_table(spark, table)
        elif next_operation <= 850:
            # SQL Procedures or other statements specific for the lake
            next_table_generator = LakeTableGenerator.get_next_generator(
                table.lake_format
            )
            self.run_query(spark, next_table_generator.generate_extra_statement(table))
        elif next_operation <= 1000:
            # Alter statements
            next_table_generator = LakeTableGenerator.get_next_generator(
                table.lake_format
            )
            self.run_query(
                spark, next_table_generator.generate_alter_table_statements(table)
            )
        return True
