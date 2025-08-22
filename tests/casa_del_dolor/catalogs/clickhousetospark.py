from typing import Dict, List, Tuple, Optional
import re


class ClickHouseSparkTypeMapper:
    """Maps between ClickHouse and Spark SQL data types using string representations."""

    def __init__(self):
        # Basic type mappings from ClickHouse to Spark SQL strings
        self.clickhouse_to_spark_map: Dict[str, str] = {
            # Numeric types
            # "UInt8": "SMALLINT",  # or 'SHORT'
            # "UInt16": "INT",  # or 'INTEGER'
            # "UInt32": "BIGINT",  # or 'LONG'
            # "UInt64": "BIGINT",  # May overflow - consider DECIMAL(20,0)
            "Int8": "TINYINT",  # or 'BYTE'
            "Int16": "SMALLINT",  # or 'SHORT'
            "Int32": "INT",  # or 'INTEGER'
            "Int64": "BIGINT",  # or 'LONG'
            "Float32": "FLOAT",  # or 'REAL'
            "Float64": "DOUBLE",  # or 'DOUBLE PRECISION'
            # String types
            "String": "STRING",  # or 'VARCHAR' for some Spark distributions
            "FixedString": "STRING",
            # Date and Time types
            "Date": "DATE",
            "Date32": "DATE",
            "Time": "TIMESTAMP",
            "Time64": "TIMESTAMP",
            "DateTime": "TIMESTAMP",
            "DateTime64": "TIMESTAMP",
            # Boolean
            "Bool": "BOOLEAN",
            "Boolean": "BOOLEAN",
            # UUID
            "UUID": "STRING",
            # IP addresses
            "IPv4": "STRING",
            "IPv6": "STRING",
            # JSON
            "JSON": "STRING",
            "Object('json')": "STRING",
        }

    def clickhouse_to_spark(
        self, ch_type: str, inside_nullable: bool
    ) -> Tuple[str, bool]:
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
            spark_element_type, _ = self.clickhouse_to_spark(element_type, False)
            return (f"ARRAY<{spark_element_type}>", False)

        # Handle Nested (similar to Array of Structs)
        if ch_type.startswith("Nested("):
            fields_str = self._extract_nested_content(ch_type, "Nested")
            fields = self._parse_nested_fields(fields_str)
            if fields:
                struct_fields = ", ".join(
                    [
                        f"{name}: {self.clickhouse_to_spark(ch_field_type, False)[0]}"
                        for name, ch_field_type in fields.items()
                    ]
                )
                return (f"ARRAY<STRUCT<{struct_fields}>>", False)
            return ("STRING", False)

        # Handle Tuple types (map to Struct)
        if ch_type.startswith("Tuple("):
            content = self._extract_nested_content(ch_type, "Tuple")
            elements = self._parse_tuple_elements(content)
            spark_elements = []

            for i, elem in enumerate(elements):
                # Check if it's a named tuple element
                # Named tuples can have format: "name Type" or "name Nested(Type)"
                # We need to find the last space that's not inside parentheses
                name, elem_type = self._split_named_element(elem)

                if name:
                    spark_type, _ = self.clickhouse_to_spark(elem_type, False)
                    spark_elements.append(f"{name}: {spark_type}")
                else:
                    spark_type, _ = self.clickhouse_to_spark(elem, False)
                    spark_elements.append(f"_{i}: {spark_type}")

            if spark_elements:
                return (f'STRUCT<{", ".join(spark_elements)}>', False)
            return ("STRING", False)

        # Handle Map types
        if ch_type.startswith("Map("):
            content = self._extract_nested_content(ch_type, "Map")
            # Parse map key and value types
            key_value = self._parse_map_types(content)
            if key_value:
                key_type, value_type = key_value
                spark_key, _ = self.clickhouse_to_spark(key_type, False)
                spark_value, _ = self.clickhouse_to_spark(value_type, False)
                return (f"MAP<{spark_key}, {spark_value}>", False)
            return ("STRING", False)

        # Handle Decimal types
        decimal_match = re.match(r"Decimal(?:\d+)?\((\d+)(?:,\s*(\d+))?\)", ch_type)
        if decimal_match:
            precision = decimal_match.group(1)
            scale = decimal_match.group(2) if decimal_match.group(2) else "0"
            return (f"DECIMAL({precision}, {scale})", inside_nullable)

        # Handle FixedString with length
        fixed_string_match = re.match(r"FixedString\((\d+)\)", ch_type)
        if fixed_string_match:
            length = fixed_string_match.group(1)
            return (f"VARCHAR({length})", inside_nullable)

        # Handle DateTime with timezone
        if ch_type.startswith("DateTime(") or ch_type.startswith("DateTime64("):
            return ("TIMESTAMP", inside_nullable)

        # Handle Time with timezone
        if ch_type.startswith("Time(") or ch_type.startswith("Time64("):
            return ("TIMESTAMP", inside_nullable)

        # Handle Enum types
        if (
            ch_type.startswith("Enum(")
            or ch_type.startswith("Enum8(")
            or ch_type.startswith("Enum16(")
        ):
            return ("STRING", inside_nullable)

        # Handle LowCardinality wrapper
        if ch_type.startswith("LowCardinality("):
            inner_type = self._extract_nested_content(ch_type, "LowCardinality")
            return self.clickhouse_to_spark(inner_type, inside_nullable)

        # Handle AggregateFunction
        if ch_type.startswith("AggregateFunction("):
            return ("BINARY", inside_nullable)

        # Basic type lookup
        return (self.clickhouse_to_spark_map.get(ch_type, "STRING"), inside_nullable)

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
