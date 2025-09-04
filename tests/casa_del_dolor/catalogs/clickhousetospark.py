from typing import Dict, List, Tuple, Optional
import random
import re
from typing import Any

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


class ClickHouseSparkTypeMapper:
    """Maps between ClickHouse and Spark SQL data types using string representations."""

    def __init__(self):
        # Basic type mappings from ClickHouse to Spark SQL strings
        self.clickhouse_to_spark_map: Dict[str, tuple[str, DataType]] = {
            # Numeric types
            "UInt8": ("SMALLINT", ShortType()),  # or 'SHORT'
            "UInt16": ("INT", IntegerType()),  # or 'INTEGER'
            "UInt32": ("BIGINT", LongType()),  # or 'LONG'
            "UInt64": ("BIGINT", LongType()),  # May overflow - consider DECIMAL(20,0)
            "UInt128": ("BIGINT", LongType()),  # May overflow - consider DECIMAL(20,0)
            "UInt256": ("BIGINT", LongType()),  # May overflow - consider DECIMAL(20,0)
            "Int8": ("TINYINT", ByteType()),  # or 'BYTE'
            "Int16": ("SMALLINT", ShortType()),  # or 'SHORT'
            "Int32": ("INT", IntegerType()),  # or 'INTEGER'
            "Int64": ("BIGINT", LongType()),  # or 'LONG'
            "Int128": ("BIGINT", LongType()),  # May overflow - consider DECIMAL(20,0)
            "Int256": ("BIGINT", LongType()),  # May overflow - consider DECIMAL(20,0)
            "Float32": ("FLOAT", FloatType()),  # or 'REAL'
            "Float64": ("DOUBLE", DoubleType()),  # or 'DOUBLE PRECISION'
            "BFloat16": ("FLOAT", FloatType()),
            # String types
            "String": ("STRING", StringType()),
            "FixedString": ("STRING", StringType()),
            # Date and Time types
            "Date": ("STRING", StringType()),  # it doesn't fit
            "Date32": ("DATE", DateType()),
            "Time": ("STRING", StringType()),
            "Time64": ("STRING", StringType()),
            "DateTime": ("STRING", StringType()),  # it doesn't fit
            "DateTime64": ("TIMESTAMP", TimestampType()),
            # Boolean
            "Bool": ("BOOLEAN", BooleanType()),
            "Boolean": ("BOOLEAN", BooleanType()),
            # UUID
            "UUID": ("STRING", StringType()),
            # IP addresses
            "IPv4": ("STRING", StringType()),
            "IPv6": ("STRING", StringType()),
            # JSON and Dynamic
            "JSON": ("STRING", StringType()),
            "Dynamic": ("STRING", StringType()),
        }

    def clickhouse_to_spark(
        self, ch_type: str, inside_nullable: bool
    ) -> Tuple[str, bool, Any]:
        """
        Convert ClickHouse type to Spark SQL type string.

        Args:
            ch_type: ClickHouse type string (e.g., 'Int32', 'Nullable(String)', 'Array(Int32)')
            inside_nullable: Whether we're inside a Nullable wrapper

        Returns:
            A tuple of (spark_type_string, is_nullable)
        """
        ch_type = ch_type.strip()

        # Handle Nullable types
        nullable_match = re.match(r"Nullable\((.*)\)$", ch_type)
        if nullable_match:
            inner_type = nullable_match.group(1)
            # In Spark SQL, nullable is handled at column level, not type level
            return self.clickhouse_to_spark(inner_type, True)

        # Handle Array types
        if ch_type.startswith("Array("):
            element_type = self._extract_nested_content(ch_type, "Array")
            inner_str, inner_null, inner_tp = self.clickhouse_to_spark(
                element_type, False
            )
            return (
                f"ARRAY<{inner_str}>",
                False,
                ArrayType(inner_tp, containsNull=inner_null),
            )

        # Handle Nested (similar to Array of Structs)
        if ch_type.startswith("Nested("):
            fields_str = self._extract_nested_content(ch_type, "Nested")
            fields = self._parse_nested_fields(fields_str)
            struct_fields = []
            if fields:
                parsed_fields = {
                    name: self.clickhouse_to_spark(ch_field_type, False)
                    for name, ch_field_type in fields.items()
                }
                str_fields = ", ".join(
                    [f"{cname}: {val[0]}" for cname, val in parsed_fields.items()]
                )
                struct_fields = [
                    StructField(name=cname, dataType=val[2], nullable=val[1])
                    for cname, val in parsed_fields.items()
                ]
                return (
                    f"ARRAY<STRUCT<{str_fields}>>",
                    False,
                    ArrayType(StructType(struct_fields), containsNull=False),
                )
            return ("STRING", False, StringType())

        # Handle Tuple types (map to Struct)
        if ch_type.startswith("Tuple("):
            content = self._extract_nested_content(ch_type, "Tuple")
            elements = self._parse_tuple_elements(content)
            spark_elements = []
            struct_fields = []

            for i, elem in enumerate(elements):
                # Check if it's a named tuple element
                # Named tuples can have format: "name Type" or "name Nested(Type)"
                # We need to find the last space that's not inside parentheses
                cname, elem_type = self._split_named_element(elem)

                if cname:
                    next_tp, next_null, next_spark = self.clickhouse_to_spark(
                        elem_type, False
                    )
                    spark_elements.append(f"{cname}: {next_tp}")
                    struct_fields.append(
                        StructField(name=cname, dataType=next_spark, nullable=next_null)
                    )
                else:
                    next_tp, next_null, next_spark = self.clickhouse_to_spark(
                        elem, False
                    )
                    spark_elements.append(f"_{i}: {next_tp}")
                    struct_fields.append(
                        StructField(
                            name=f"_{i}", dataType=next_spark, nullable=next_null
                        )
                    )

            if spark_elements:
                return (
                    f'STRUCT<{", ".join(spark_elements)}>',
                    False,
                    StructType(struct_fields),
                )
            return ("STRING", False, StringType())

        # Handle Map types
        if ch_type.startswith("Map("):
            content = self._extract_nested_content(ch_type, "Map")
            # Parse map key and value types
            key_value = self._parse_map_types(content)
            if key_value:
                key_type, value_type = key_value
                spark_key_str, _, spark_key = self.clickhouse_to_spark(key_type, False)
                spark_value_str, value_nullable, spark_val = self.clickhouse_to_spark(
                    value_type, False
                )
                return (
                    f"MAP<{spark_key_str}, {spark_value_str}>",
                    False,
                    MapType(spark_key, spark_val, valueContainsNull=value_nullable),
                )
            return ("STRING", False, StringType())

        # Handle Decimal types
        decimal_match = re.match(r"Decimal(?:\d+)?\((\d+)(?:,\s*(\d+))?\)", ch_type)
        if decimal_match:
            nprecision = min(38, int(decimal_match.group(1)))
            nscale = int(decimal_match.group(2) if decimal_match.group(2) else "0")
            return (
                f"DECIMAL({nprecision}, {nscale})",
                inside_nullable,
                DecimalType(precision=nprecision, scale=nscale),
            )

        # Handle FixedString with length
        fixed_string_match = re.match(r"FixedString\((\d+)\)", ch_type)
        if fixed_string_match:
            nlength = fixed_string_match.group(1)
            return (
                (f"CHAR({nlength})", inside_nullable, CharType(length=int(nlength)))
                if random.randint(1, 2) == 1
                else (
                    f"VARCHAR({nlength})",
                    inside_nullable,
                    VarcharType(length=int(nlength)),
                )
            )

        # Handle DateTime and Time
        for val in ["DateTime", "Time"]:
            if ch_type.startswith(val):
                return ("TIMESTAMP", inside_nullable, TimestampType())

        # Handle LowCardinality wrapper
        if ch_type.startswith("LowCardinality("):
            inner_type = self._extract_nested_content(ch_type, "LowCardinality")
            return self.clickhouse_to_spark(inner_type, inside_nullable)

        # Handle types not covered by Spark
        # Spark 4.0.0 has Variant type, maybe worth to try
        for val in ["Enum", "Variant", "JSON", "Dynamic"]:
            if ch_type.startswith(val):
                is_text = random.randint(1, 2) == 1
                return (
                    "STRING" if is_text else "BINARY",
                    inside_nullable,
                    StringType() if is_text else BinaryType(),
                )

        # Handle AggregateFunction
        for val in ["AggregateFunction", "SimpleAggregateFunction"]:
            if ch_type.startswith(val):
                return ("BINARY", inside_nullable, BinaryType())

        # Basic type lookup
        str_type, spark_type = self.clickhouse_to_spark_map.get(
            ch_type, ("STRING", StringType())
        )
        return (str_type, inside_nullable, spark_type)

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
