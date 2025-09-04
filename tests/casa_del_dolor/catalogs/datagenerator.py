import random
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

from .laketables import SparkTable


class LakeDataGenerator:
    def __init__(self):
        self._min_nested = 0
        self._max_nested = 100
        self._min_str_len = 0
        self._max_str_len = 100

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
        alphabet = string.ascii_letters + string.digits + " _-"
        return "".join(random.choice(alphabet) for _ in range(nlen))

    def _rand_binary(self, nlen):
        return bytes(random.getrandbits(8) for _ in range(nlen))

    def _rand_date(self):
        start = date(1900, 1, 1).toordinal()
        end = date(2300, 12, 31).toordinal()
        return date.fromordinal(self._rand_int(start, end))

    def _rand_timestamp(self):
        start = datetime(1, 1, 1)
        end = datetime(9999, 12, 31)
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

    def _random_value_for_type(self, dtype: DataType, null_rate: float):
        """Return a random Python value that conforms to the given Spark DataType."""
        if random.random() < null_rate:
            return None
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
            return self._rand_string(
                random.randint(self._min_str_len, self._max_str_len)
            )
        if isinstance(dtype, CharType) or isinstance(dtype, VarcharType):
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
        if isinstance(dtype, CharType) or isinstance(dtype, VarcharType):
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

    def insert_random_data(
        self, spark: SparkSession, catalog_name: str, table: SparkTable
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
        n_rows = random.randint(0, 100)
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
        df = spark.createDataFrame(rows, schema=struct2)
        df.writeTo(f"{catalog_name}.test.{table.table_name}").append()
