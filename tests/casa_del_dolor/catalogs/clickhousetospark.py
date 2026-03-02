from enum import Enum
from typing import Dict, List, Tuple, Optional
import random
import re
import pyspark.sql.types as sp
import pyiceberg.types as it
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
from pyiceberg.transforms import (
    IdentityTransform,
    BucketTransform,
    TruncateTransform,
    YearTransform,
    MonthTransform,
    DayTransform,
    HourTransform,
    VoidTransform,
)

try:
    from pyspark.sql.types import VariantType

    HAS_VARIANT_TYPE = True
except ImportError:
    HAS_VARIANT_TYPE = False


class ClickHouseMapping(Enum):
    Unkown = 0
    Spark = 1
    Iceberg = 2


class ClickHouseTypeMapper:
    """Maps between ClickHouse and Spark and Iceberg data types with string representations."""

    def __init__(self):
        # Basic type mappings from ClickHouse to Spark SQL strings
        self.field_id = 1
        self.clickhouse_to_spark_map: Dict[
            str, tuple[str, sp.DataType, it.PrimitiveType]
        ] = {
            # Numeric types
            "UInt8": ("SMALLINT", sp.ShortType(), it.IntegerType()),  # or 'SHORT'
            "UInt16": ("INT", sp.IntegerType(), it.IntegerType()),  # or 'INTEGER'
            "UInt32": ("BIGINT", sp.LongType(), it.LongType()),  # or 'LONG'
            "UInt64": (
                "BIGINT",
                sp.LongType(),
                it.LongType(),
            ),  # May overflow - consider DECIMAL(20,0)
            "UInt128": (
                "BIGINT",
                sp.LongType(),
                it.LongType(),
            ),  # May overflow - consider DECIMAL(20,0)
            "UInt256": (
                "BIGINT",
                sp.LongType(),
                it.LongType(),
            ),  # May overflow - consider DECIMAL(20,0)
            "Int8": ("TINYINT", sp.ByteType(), it.IntegerType()),  # or 'BYTE'
            "Int16": ("SMALLINT", sp.ShortType(), it.IntegerType()),  # or 'SHORT'
            "Int32": ("INT", sp.IntegerType(), it.IntegerType()),  # or 'INTEGER'
            "Int64": ("BIGINT", sp.LongType(), it.LongType()),  # or 'LONG'
            "Int128": (
                "BIGINT",
                sp.LongType(),
                it.LongType(),
            ),  # May overflow - consider DECIMAL(20,0)
            "Int256": (
                "BIGINT",
                sp.LongType(),
                it.LongType(),
            ),  # May overflow - consider DECIMAL(20,0)
            "Float32": ("FLOAT", sp.FloatType(), it.FloatType()),  # or 'REAL'
            "Float64": (
                "DOUBLE",
                sp.DoubleType(),
                it.DoubleType(),
            ),  # or 'DOUBLE PRECISION'
            "BFloat16": ("FLOAT", sp.FloatType(), it.FloatType()),
            # String types
            "String": ("STRING", sp.StringType(), it.StringType()),
            "FixedString": ("STRING", sp.StringType(), it.StringType()),
            # Date and Time types
            "Date": ("STRING", sp.StringType(), it.StringType()),  # it doesn't fit
            "Date32": ("DATE", sp.DateType(), it.DateType()),
            "Time": ("STRING", sp.StringType(), it.TimeType()),
            "Time64": ("STRING", sp.StringType(), it.TimeType()),
            "DateTime": ("STRING", sp.StringType(), it.StringType()),  # it doesn't fit
            "DateTime64": ("TIMESTAMP", sp.TimestampType(), it.TimestampType()),
            # Boolean
            "Bool": ("BOOLEAN", sp.BooleanType(), it.BooleanType()),
            "Boolean": ("BOOLEAN", sp.BooleanType(), it.BooleanType()),
            # UUID
            "UUID": ("STRING", sp.StringType(), it.UUIDType()),
            # IP addresses
            "IPv4": ("STRING", sp.StringType(), it.StringType()),
            "IPv6": ("STRING", sp.StringType(), it.StringType()),
        }

    def reset(self):
        self.field_id = 1

    def increment(self):
        self.field_id += 1

    def clickhouse_to_spark(
        self, ch_type: str, inside_nullable: bool, mapping: ClickHouseMapping
    ) -> Tuple[str, bool, sp.DataType | it.IcebergType]:
        """
        Convert ClickHouse type to Spark or Iceberg type with string representation.

        Args:
            ch_type: ClickHouse type string (e.g., 'Int32', 'Nullable(String)', 'Array(Int32)')
            inside_nullable: Whether we're inside a Nullable wrapper

        Returns:
            A tuple of (spark_type_string, is_nullable)
        """
        ch_type = ch_type.strip()
        module = sp if mapping == ClickHouseMapping.Spark else it

        # Handle Nullable types
        nullable_match = re.match(r"Nullable\((.*)\)$", ch_type)
        if nullable_match:
            inner_type = nullable_match.group(1)
            # In Spark SQL, nullable is handled at column level, not type level
            return self.clickhouse_to_spark(inner_type, True, mapping)

        # Handle Array types
        if ch_type.startswith("Array("):
            element_type = self._extract_nested_content(ch_type, "Array")
            current_id = self.field_id

            self.increment()
            inner_str, inner_null, inner_tp = self.clickhouse_to_spark(
                element_type, False, mapping
            )
            return (
                f"ARRAY<{inner_str}>",
                False,
                (
                    sp.ArrayType(inner_tp, containsNull=inner_null)
                    if mapping == ClickHouseMapping.Spark
                    else it.ListType(
                        element_id=current_id,
                        element_type=inner_tp,
                        element_required=not inner_null,
                    )
                ),
            )

        # Handle Nested (similar to Array of Structs)
        if ch_type.startswith("Nested("):
            fields_str = self._extract_nested_content(ch_type, "Nested")
            fields = self._parse_nested_fields(fields_str)
            struct_fields = []
            current_id = self.field_id

            if fields:
                parsed_fields = {}
                for name, ch_field_type in fields.items():
                    self.increment()
                    parsed_fields[name] = self.clickhouse_to_spark(
                        ch_field_type, False, mapping
                    )
                str_fields = ", ".join(
                    [f"{cname}: {val[0]}" for cname, val in parsed_fields.items()]
                )
                struct_fields = [
                    (
                        sp.StructField(name=cname, dataType=val[2], nullable=val[1])
                        if mapping == ClickHouseMapping.Spark
                        else it.NestedField(
                            field_id=current_id,
                            name=cname,
                            field_type=val[2],
                            required=not val[1],
                        )
                    )
                    for cname, val in parsed_fields.items()
                ]
                inner_fields = (
                    module.StructType(struct_fields)
                    if mapping == ClickHouseMapping.Spark
                    else module.StructType(*struct_fields)
                )

                return (
                    f"ARRAY<STRUCT<{str_fields}>>",
                    False,
                    (
                        sp.ArrayType(inner_fields)
                        if mapping == ClickHouseMapping.Spark
                        else it.ListType(
                            element_id=current_id,
                            element_type=inner_fields,
                            element_required=True,
                        )
                    ),
                )
            return ("STRING", False, module.StringType())

        # Handle Tuple types (map to Struct)
        if ch_type.startswith("Tuple("):
            content = self._extract_nested_content(ch_type, "Tuple")
            elements = self._parse_tuple_elements(content)
            spark_elements = []
            struct_fields = []
            current_id = self.field_id

            for i, elem in enumerate(elements):
                # Check if it's a named tuple element
                # Named tuples can have format: "name Type" or "name Nested(Type)"
                # We need to find the last space that's not inside parentheses
                cname, elem_type = self._split_named_element(elem)

                self.increment()
                if cname:
                    next_tp, next_null, next_spark = self.clickhouse_to_spark(
                        elem_type, False, mapping
                    )
                    spark_elements.append(f"{cname}: {next_tp}")
                else:
                    cname = f"{i + 1}"
                    next_tp, next_null, next_spark = self.clickhouse_to_spark(
                        elem, False, mapping
                    )
                    spark_elements.append(f"`{cname}`: {next_tp}")

                struct_fields.append(
                    sp.StructField(name=cname, dataType=next_spark, nullable=next_null)
                    if mapping == ClickHouseMapping.Spark
                    else it.NestedField(
                        field_id=current_id,
                        name=cname,
                        field_type=next_spark,
                        required=not next_null,
                    )
                )

            if spark_elements:
                return (
                    f'STRUCT<{", ".join(spark_elements)}>',
                    False,
                    (
                        module.StructType(struct_fields)
                        if mapping == ClickHouseMapping.Spark
                        else module.StructType(*struct_fields)
                    ),
                )
            return ("STRING", False, module.StringType())

        # Handle Map types
        if ch_type.startswith("Map("):
            content = self._extract_nested_content(ch_type, "Map")
            # Parse map key and value types
            key_value = self._parse_map_types(content)
            if key_value:
                current_id = self.field_id
                key_type, value_type = key_value

                self.increment()
                spark_key_str, _, spark_key = self.clickhouse_to_spark(
                    key_type, False, mapping
                )
                self.increment()
                spark_value_str, value_nullable, spark_val = self.clickhouse_to_spark(
                    value_type, False, mapping
                )
                return (
                    f"MAP<{spark_key_str}, {spark_value_str}>",
                    False,
                    (
                        sp.MapType(
                            spark_key, spark_val, valueContainsNull=value_nullable
                        )
                        if mapping == ClickHouseMapping.Spark
                        else it.MapType(
                            key_id=current_id,
                            key_type=spark_key,
                            value_id=current_id,
                            value_type=spark_val,
                            value_required=True,
                        )
                    ),
                )
            return ("STRING", False, module.StringType())

        # Handle Decimal types
        decimal_match = re.match(r"Decimal(?:\d+)?\((\d+)(?:,\s*(\d+))?\)", ch_type)
        if decimal_match:
            nprecision = min(38, int(decimal_match.group(1)))
            nscale = min(
                nprecision,
                int(decimal_match.group(2) if decimal_match.group(2) else "0"),
            )
            return (
                f"DECIMAL({nprecision}, {nscale})",
                inside_nullable,
                module.DecimalType(precision=nprecision, scale=nscale),
            )

        # Handle FixedString with length
        fixed_string_match = re.match(r"FixedString\((\d+)\)", ch_type)
        if fixed_string_match:
            nlength = fixed_string_match.group(1)
            if mapping == ClickHouseMapping.Iceberg:
                return ("STRING", False, module.StringType())
            return (
                (
                    f"CHAR({nlength})",
                    inside_nullable,
                    sp.CharType(length=int(nlength)),
                )
                if random.randint(1, 2) == 1
                else (
                    f"VARCHAR({nlength})",
                    inside_nullable,
                    sp.VarcharType(length=int(nlength)),
                )
            )

        # Handle DateTime and Time
        for val in ["DateTime", "Time"]:
            if ch_type.startswith(val):
                return ("TIMESTAMP", inside_nullable, module.TimestampType())

        # Handle LowCardinality wrapper
        if ch_type.startswith("LowCardinality("):
            inner_type = self._extract_nested_content(ch_type, "LowCardinality")
            return self.clickhouse_to_spark(inner_type, inside_nullable, mapping)

        # Handle Variant(T1, T2, ...) → Spark VariantType / Iceberg Struct with nullable fields
        if ch_type.startswith("Variant("):
            if HAS_VARIANT_TYPE and mapping == ClickHouseMapping.Spark:
                return ("VARIANT", inside_nullable, sp.VariantType())

            # Iceberg has no Variant type; expand into a Struct with nullable fields
            content = self._extract_nested_content(ch_type, "Variant")
            elements = self._parse_tuple_elements(content)
            current_id = self.field_id

            if elements:
                spark_elements = []
                struct_fields = []
                for i, elem in enumerate(elements):
                    self.increment()
                    elem = elem.strip()
                    inner_str, _, inner_tp = self.clickhouse_to_spark(
                        elem, True, mapping
                    )
                    field_name = f"v{i}_{re.split(r'[^a-zA-Z0-9_]', elem)[0]}"
                    spark_elements.append(f"{field_name}: {inner_str}")
                    struct_fields.append(
                        it.NestedField(
                            field_id=current_id + i,
                            name=field_name,
                            field_type=inner_tp,
                            required=False,
                        )
                    )

                return (
                    f'STRUCT<{", ".join(spark_elements)}>',
                    inside_nullable,
                    it.StructType(*struct_fields),
                )
            return ("STRING", inside_nullable, it.StringType())

        # Handle JSON, Dynamic → Spark 4.0 VariantType (untyped semi-structured)
        for val in ["JSON", "Dynamic", "Enum"]:
            if ch_type.startswith(val):
                if (
                    HAS_VARIANT_TYPE
                    and val != "Enum"
                    and mapping == ClickHouseMapping.Spark
                ):
                    return ("VARIANT", inside_nullable, sp.VariantType())
                else:
                    is_text = random.randint(1, 2) == 1
                    return (
                        "STRING" if is_text else "BINARY",
                        inside_nullable,
                        (module.StringType() if is_text else module.BinaryType()),
                    )

        # Handle AggregateFunction
        for val in ["AggregateFunction", "SimpleAggregateFunction"]:
            if ch_type.startswith(val):
                return ("BINARY", inside_nullable, module.BinaryType())

        # Basic type lookup
        str_type, spark_type, iceberg_type = self.clickhouse_to_spark_map.get(
            ch_type, ("STRING", sp.StringType(), it.StringType())
        )
        return (
            str_type,
            inside_nullable,
            spark_type if mapping == ClickHouseMapping.Spark else iceberg_type,
        )

    def _extract_nested_content(self, type_str: str, prefix: str) -> str:
        """
        Extract content from nested type like Array(...), Tuple(...), etc.
        Handles nested parentheses correctly.
        """
        if not type_str.startswith(f"{prefix}("):
            return ""

        start = len(prefix) + 1
        depth = 1
        i = start

        while i < len(type_str) and depth > 0:
            if type_str[i] == "(":
                depth += 1
            elif type_str[i] == ")":
                depth -= 1
            i += 1

        if depth == 0:
            return type_str[start : i - 1]
        return ""

    def _parse_tuple_elements(self, elements_str: str) -> List[str]:
        """
        Parse comma-separated tuple elements, handling nested types properly.
        This correctly handles nested tuples, arrays, maps, etc.
        """
        elements = []
        current = []
        depth = 0
        in_quotes = False
        quote_char = None

        for char in elements_str:
            if not in_quotes:
                if char in "\"'":
                    in_quotes = True
                    quote_char = char
                    current.append(char)
                elif char in "(<":
                    depth += 1
                    current.append(char)
                elif char in ")>":
                    depth -= 1
                    current.append(char)
                elif char == "," and depth == 0:
                    elements.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
            else:
                current.append(char)
                if char == quote_char:
                    in_quotes = False
                    quote_char = None

        if current:
            elements.append("".join(current).strip())
        return elements

    def _split_named_element(self, element: str) -> Tuple[Optional[str], str]:
        """
        Split a potentially named tuple element into name and type.
        Handles cases like:
        - "name Type"
        - "name Tuple(Type1, Type2)"
        - "name Array(Nested(Type))"
        """
        # Find the last space that's not inside parentheses
        depth = 0
        last_space_pos = -1
        in_quotes = False
        quote_char = None

        for i, char in enumerate(element):
            if not in_quotes:
                if char in "\"'":
                    in_quotes = True
                    quote_char = char
                elif char in "(<":
                    depth += 1
                elif char in ")>":
                    depth -= 1
                elif char == " " and depth == 0:
                    last_space_pos = i
            else:
                if char == quote_char:
                    in_quotes = False
                    quote_char = None

        if last_space_pos > 0:
            name = element[:last_space_pos].strip()
            elem_type = element[last_space_pos + 1 :].strip()
            # Validate that name is a valid identifier
            if name and name[0].isalpha() or name[0] == "_":
                return name, elem_type
        return None, element

    def _parse_map_types(self, content: str) -> Optional[Tuple[str, str]]:
        """
        Parse Map content to extract key and value types.
        Handles nested types in both key and value.
        """
        # Find the comma that separates key and value types
        depth = 0
        in_quotes = False
        quote_char = None

        for i, char in enumerate(content):
            if not in_quotes:
                if char in "\"'":
                    in_quotes = True
                    quote_char = char
                elif char in "(<":
                    depth += 1
                elif char in ")>":
                    depth -= 1
                elif char == "," and depth == 0:
                    # Found the separator
                    key_type = content[:i].strip()
                    value_type = content[i + 1 :].strip()
                    return (key_type, value_type)
            else:
                if char == quote_char:
                    in_quotes = False
                    quote_char = None
        return None

    def _parse_nested_fields(self, fields_str: str) -> Dict[str, str]:
        """Parse Nested type field definitions."""
        fields = {}
        elements = self._parse_tuple_elements(fields_str)

        for element in elements:
            name, ch_type = self._split_named_element(element)
            if name:
                fields[name] = ch_type
        return fields

    def generate_random_spark_sql_type(
        self, max_depth=3, current_depth=0, allow_complex=True
    ):
        """
        Generate a random Spark SQL data type as a SQL string.

        Args:
            max_depth: Maximum nesting depth for complex types (STRUCT, ARRAY, MAP)
            current_depth: Current depth in the type hierarchy
            allow_complex: Whether to allow complex types at this level

        Returns:
            A SQL type string (e.g., "INT", "ARRAY<STRING>", "STRUCT<a:INT,b:STRING>")
        """

        # Primitive types
        primitive_types = [
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            f"DECIMAL({random.randint(1, 38)},{random.randint(0, 10)})",
            "STRING",
            "BINARY",
            "BOOLEAN",
            "DATE",
            "TIMESTAMP",
        ]

        # If we've reached max depth or complex types not allowed, return primitive
        if current_depth >= max_depth or not allow_complex:
            return random.choice(primitive_types)

        # Choose between primitive and complex types
        type_choice = random.choice(["primitive", "array", "map", "struct", "variant"])

        if type_choice == "primitive":
            return random.choice(primitive_types)
        elif type_choice == "array":
            # Generate random element type (can be nested)
            element_type = self.generate_random_spark_sql_type(
                max_depth=max_depth, current_depth=current_depth + 1, allow_complex=True
            )
            return f"ARRAY<{element_type}>"
        elif type_choice == "map":
            # Generate random key and value types
            # Keys are typically primitive types
            key_type = random.choice(primitive_types)
            value_type = self.generate_random_spark_sql_type(
                max_depth=max_depth, current_depth=current_depth + 1, allow_complex=True
            )
            return f"MAP<{key_type},{value_type}>"
        elif type_choice == "struct":
            # Generate random number of fields (1-3)
            num_fields = random.randint(1, 3)
            fields = []
            for i in range(num_fields):
                field_name = f"`{i}`"
                field_type = self.generate_random_spark_sql_type(
                    max_depth=max_depth,
                    current_depth=current_depth + 1,
                    allow_complex=True,
                )
                fields.append(f"{field_name}:{field_type}")
            return f"STRUCT<{','.join(fields)}>"
        elif type_choice == "variant":
            return "VARIANT"

    def generate_random_spark_type(
        self, allow_variant=True, max_depth=3, current_depth=0
    ) -> sp.DataType:
        """Return a random Spark DataType for use inside a Variant column."""
        primitive_factories = [
            sp.BooleanType,
            sp.ShortType,
            sp.IntegerType,
            sp.ByteType,
            sp.LongType,
            sp.FloatType,
            sp.DoubleType,
            sp.StringType,
            sp.BinaryType,
            sp.DateType,
            sp.TimestampType,
            lambda: sp.DecimalType(
                precision=random.randint(1, 38), scale=random.randint(0, 10)
            ),
            lambda: sp.CharType(length=random.randint(1, 100)),
            lambda: sp.VarcharType(length=random.randint(1, 100)),
        ]
        roll = random.randint(1, 100)

        # At max depth only emit primitives
        if roll <= 60 or current_depth >= max_depth:
            factory = random.choice(primitive_factories)
            return factory() if callable(factory) else factory
        elif HAS_VARIANT_TYPE and roll <= 70 and allow_variant:
            return sp.VariantType()
        elif roll <= 80:
            elem = self.generate_random_spark_type(
                allow_variant, max_depth, current_depth + 1
            )
            return sp.ArrayType(elem, containsNull=random.choice([True, False]))
        elif roll <= 90:
            factory = random.choice(primitive_factories)
            key = factory() if callable(factory) else factory
            val = self.generate_random_spark_type(
                allow_variant, max_depth, current_depth + 1
            )
            return sp.MapType(key, val, valueContainsNull=random.choice([True, False]))
        else:
            n_fields = random.randint(1, 4)
            fields = []
            for i in range(n_fields):
                ft = self.generate_random_spark_type(
                    allow_variant, max_depth, current_depth + 1
                )
                fields.append(
                    sp.StructField(
                        name=f"f{i}", dataType=ft, nullable=random.choice([True, False])
                    )
                )
            return sp.StructType(fields)

    def generate_random_clickhouse_type(
        self, allow_complex=True, allow_variant=True, max_depth=3, current_depth=0
    ) -> str:
        """Generate a random ClickHouse SQL type string."""

        primitive_types = [
            "Bool",
            "Boolean",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
            "UInt256",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Int256",
            "BFloat16",
            "Float32",
            "Float64",
            f"Decimal({random.randint(1, 38)}, {random.randint(0, 10)})",
            "String",
            f"FixedString({random.randint(1, 100)})",
            "Date",
            "Date32",
            "DateTime",
            "DateTime64",
            "Time",
            "Time64",
            "UUID",
            "IPv4",
            "IPv6",
            "JSON",
            "Dynamic",
            "Enum",
            "Enum8",
            "Enum16",
        ]
        roll = random.randint(1, 100)

        if roll <= 60 or current_depth >= max_depth or not allow_complex:
            base = random.choice(primitive_types)
        elif roll <= 70:
            inner = self.generate_random_clickhouse_type(
                allow_complex, allow_variant, max_depth, current_depth + 1
            )
            base = f"Array({inner})"
        elif roll <= 80:
            # Map keys must be primitive in ClickHouse
            key = self.generate_random_clickhouse_type(
                False, allow_variant, max_depth, current_depth + 1
            )
            val = self.generate_random_clickhouse_type(
                allow_complex, allow_variant, max_depth, current_depth + 1
            )
            base = f"Map({key}, {val})"
        elif roll <= 90:
            n = random.randint(1, 4)
            named = random.choice([True, False])
            elems = []
            for i in range(n):
                t = self.generate_random_clickhouse_type(
                    allow_complex, allow_variant, max_depth, current_depth + 1
                )
                elems.append(f"f{i} {t}" if named else t)
            base = f"Tuple({','.join(elems)})"
        elif roll <= 95:
            n = random.randint(2, 5)
            members = set()
            while len(members) < n:
                members.add(
                    self.generate_random_clickhouse_type(
                        allow_complex, False, max_depth, current_depth + 1
                    )
                )
            base = f"Variant({','.join(members)})"
        else:
            n = random.randint(1, 3)
            fields = [
                f"f{i} {self.generate_random_clickhouse_type(allow_complex, allow_variant, max_depth, current_depth + 1)}"
                for i in range(n)
            ]
            base = f"Nested({','.join(fields)})"

        # Optionally wrap in Nullable or LowCardinality
        if random.randint(1, 5) == 1 and not base.startswith(
            ("Dynamic", "Array", "Map", "Nested", "Variant")
        ):
            base = f"Nullable({base})"
        if random.randint(1, 8) == 1 and not base.startswith(
            (
                "Decimal",
                "Enum",
                "JSON",
                "Dynamic",
                "Array",
                "Map",
                "Tuple",
                "Nested",
                "Variant",
            )
        ):
            base = f"LowCardinality({base})"
        return base

    def _get_random_iceberg_transform_for_type(self, field_type):
        """Get a random appropriate transform for a field type."""
        transforms = [IdentityTransform()]

        if isinstance(field_type, it.StringType) or random.randint(1, 100) < 6:
            transforms.extend(
                [
                    BucketTransform(num_buckets=random.choice([4, 8, 16, 32, 64])),
                    TruncateTransform(width=random.choice([1, 2, 4, 8, 16])),
                ]
            )
        if (
            isinstance(
                field_type, (it.IntegerType, it.LongType, it.FloatType, it.DoubleType)
            )
            or random.randint(1, 100) < 6
        ):
            transforms.extend(
                [
                    BucketTransform(num_buckets=random.choice([4, 8, 16, 32, 64, 128])),
                ]
            )
        if (
            isinstance(
                field_type,
                (
                    it.DateType,
                    it.TimestampType,
                    it.TimestamptzType,
                ),
            )
            or random.randint(1, 100) < 6
        ):
            transforms.extend([YearTransform(), MonthTransform(), DayTransform()])
        if (
            isinstance(
                field_type,
                (
                    it.TimeType,
                    it.TimestampType,
                    it.TimestamptzType,
                ),
            )
            or random.randint(1, 100) < 6
        ):
            transforms.extend([HourTransform()])
        if isinstance(field_type, it.DecimalType) or random.randint(1, 100) < 6:
            transforms.extend(
                [
                    BucketTransform(num_buckets=random.choice([8, 16, 32])),
                ]
            )
        if random.randint(1, 100) < 11:
            transforms.extend([VoidTransform()])
        return random.choice(transforms)

    def generate_random_iceberg_partition_spec(
        self, schema: Schema, max_partitions: int = 3
    ) -> PartitionSpec:
        """
        Generate a random PartitionSpec from a schema.

        Args:
            schema: The Iceberg schema
            max_partitions: Maximum number of partition fields to create

        Returns:
            A random PartitionSpec
        """
        # Get all fields from schema
        available_fields = list(schema.fields)
        if not available_fields or random.randint(1, 5) != 5:
            return PartitionSpec()
        # Randomly decide how many partitions to create (0 to max_partitions)
        num_partitions = random.randint(0, min(max_partitions, len(available_fields)))
        if num_partitions == 0:
            return PartitionSpec()  # Unpartitioned table

        # Randomly select fields to partition on
        partition_fields_list = random.sample(available_fields, num_partitions)
        partition_fields = []
        partition_field_id = 1000  # Start partition field IDs at 1000
        for field in partition_fields_list:
            # Choose appropriate transform based on field type
            transform = self._get_random_iceberg_transform_for_type(field.field_type)

            partition_field = PartitionField(
                source_id=field.field_id,
                field_id=partition_field_id,
                transform=transform,
                name=f"{field.name}_{transform}",
            )
            partition_fields.append(partition_field)
            partition_field_id += 1
        return PartitionSpec(*partition_fields)

    def generate_random_iceberg_sort_order(
        self, schema: Schema, max_sort_fields: int = 3
    ) -> SortOrder:
        """
        Generate a random SortOrder from a schema.

        Args:
            schema: The Iceberg schema
            max_sort_fields: Maximum number of sort fields to create

        Returns:
            A random SortOrder
        """
        # Get all fields from schema
        available_fields = list(schema.fields)
        if not available_fields or random.randint(1, 5) != 5:
            return SortOrder()
        # Randomly decide how many sort fields to create (0 to max_sort_fields)
        num_sort_fields = random.randint(0, min(max_sort_fields, len(available_fields)))
        if num_sort_fields == 0:
            return SortOrder()  # Unsorted table

        # Randomly select fields to sort on (without replacement)
        sort_fields_list = random.sample(available_fields, num_sort_fields)
        sort_fields = []
        for field in sort_fields_list:
            # Choose transform (or identity)
            transform = self._get_random_iceberg_transform_for_type(field.field_type)
            # Choose sort direction
            direction = random.choice([SortDirection.ASC, SortDirection.DESC])
            # Choose null order
            null_order = random.choice([NullOrder.NULLS_FIRST, NullOrder.NULLS_LAST])

            sort_field = SortField(
                source_id=field.field_id,
                transform=transform,
                direction=direction,
                null_order=null_order,
            )
            sort_fields.append(sort_field)

        return SortOrder(*sort_fields)
