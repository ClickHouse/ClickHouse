from typing import Dict, Optional
import re


class ClickHouseSparkTypeMapper:
    """Maps between ClickHouse and Spark SQL data types using string representations."""

    def __init__(self):
        # Basic type mappings from ClickHouse to Spark SQL strings
        self.clickhouse_to_spark_map: Dict[str, str] = {
            # Numeric types
            #"UInt8": "SMALLINT",  # or 'SHORT'
            #"UInt16": "INT",  # or 'INTEGER'
            #"UInt32": "BIGINT",  # or 'LONG'
            #"UInt64": "BIGINT",  # May overflow - consider DECIMAL(20,0)
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
        self, inside_nullable: bool, ch_type: str
    ) -> tuple[str, bool]:
        """
        Convert ClickHouse type to Spark SQL type string.

        Args:
            ch_type: ClickHouse type string (e.g., 'Int32', 'Nullable(String)', 'Array(Int32)')

        Returns:
            A tuple of type as str and if is nullable
        """
        ch_type = ch_type.strip()

        # Handle Nullable types
        nullable_match = re.match(r"Nullable\((.*)\)", ch_type)
        if nullable_match:
            inner_type = nullable_match.group(1)
            # In Spark SQL, nullable is handled at column level, not type level
            return self.clickhouse_to_spark(True, inner_type)

        # Handle Array types
        array_match = re.match(r"Array\((.*)\)", ch_type)
        if array_match:
            element_type = array_match.group(1)
            spark_element_type = self.clickhouse_to_spark(False, element_type)
            if spark_element_type:
                return (f"ARRAY<{spark_element_type}>", False)
            return ("STRING", False)

        # Handle Nested (similar to Array of Structs)
        nested_match = re.match(r"Nested\((.*)\)", ch_type)
        if nested_match:
            # Parse nested fields
            fields_str = nested_match.group(1)
            fields = self._parse_nested_fields(fields_str)
            if fields:
                struct_fields = ", ".join(
                    [
                        f"{name}: {self.clickhouse_to_spark(False, ch_type)}"
                        for name, ch_type in fields.items()
                    ]
                )
                return (f"ARRAY<STRUCT<{struct_fields}>>", False)
            return ("STRING", False)

        # Handle Tuple types (map to Struct)
        tuple_match = re.match(r"Tuple\((.*)\)", ch_type)
        if tuple_match:
            elements = self._parse_tuple_elements(tuple_match.group(1))
            spark_elements = []
            for i, elem in enumerate(elements):
                # Check if it's a named tuple
                if (
                    " " in elem
                    and not elem.startswith("Array")
                    and not elem.startswith("Map")
                ):
                    parts = elem.rsplit(" ", 1)
                    if len(parts) == 2:
                        name, elem_type = parts
                        spark_elem_type = self.clickhouse_to_spark(False, elem_type)
                        if spark_elem_type:
                            spark_elements.append(f"{name}: {spark_elem_type}")
                    else:
                        spark_elem_type = self.clickhouse_to_spark(False, elem)
                        if spark_elem_type:
                            spark_elements.append(f"_{i}: {spark_elem_type}")
                else:
                    spark_elem_type = self.clickhouse_to_spark(False, elem)
                    if spark_elem_type:
                        spark_elements.append(f"_{i}: {spark_elem_type}")
            if spark_elements:
                return (f'STRUCT<{", ".join(spark_elements)}>', False)
            return ("STRING", False)

        # Handle Map types
        map_match = re.match(r"Map\((.*?),\s*(.*)\)", ch_type)
        if map_match:
            key_type = map_match.group(1)
            value_type = map_match.group(2)
            spark_key_type = self.clickhouse_to_spark(False, key_type)
            spark_value_type = self.clickhouse_to_spark(False, value_type)
            if spark_key_type and spark_value_type:
                return (f"MAP<{spark_key_type}, {spark_value_type}>", False)
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
        datetime_tz_match = re.match(r"DateTime\((.*)\)", ch_type)
        if datetime_tz_match:
            return ("TIMESTAMP", inside_nullable)

        # Handle DateTime64 with precision
        datetime64_match = re.match(r"DateTime64\((\d+)(?:,\s*(.*))?\)", ch_type)
        if datetime64_match:
            return ("TIMESTAMP", inside_nullable)

        # Handle Time with timezone
        datetime_tz_match = re.match(r"Time\((.*)\)", ch_type)
        if datetime_tz_match:
            return ("TIMESTAMP", inside_nullable)

        # Handle Time64 with precision
        datetime64_match = re.match(r"Time64\((\d+)(?:,\s*(.*))?\)", ch_type)
        if datetime64_match:
            return ("TIMESTAMP", inside_nullable)

        # Handle Enum types
        enum_match = re.match(r"Enum(?:8|16)\((.*)\)", ch_type)
        if enum_match:
            return ("STRING", inside_nullable)

        # Handle LowCardinality wrapper
        low_cardinality_match = re.match(r"LowCardinality\((.*)\)", ch_type)
        if low_cardinality_match:
            inner_type = low_cardinality_match.group(1)
            return self.clickhouse_to_spark(inside_nullable, inner_type)

        # Handle AggregateFunction
        agg_func_match = re.match(r"AggregateFunction\((.*?),\s*(.*)\)", ch_type)
        if agg_func_match:
            return ("BINARY", inside_nullable)  # Store as binary data

        # Basic type lookup
        return (self.clickhouse_to_spark_map.get(ch_type, "STRING"), inside_nullable)

    def _parse_tuple_elements(self, elements_str: str) -> list:
        """Parse comma-separated tuple elements, handling nested types."""
        elements = []
        current = []
        depth = 0

        for char in elements_str:
            if char in "(<":
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

        if current:
            elements.append("".join(current).strip())

        return elements

    def _parse_nested_fields(self, fields_str: str) -> dict:
        """Parse Nested type field definitions."""
        fields = {}
        parts = fields_str.split(",")
        for part in parts:
            part = part.strip()
            if " " in part:
                components = part.rsplit(" ", 1)
                if len(components) == 2:
                    name, ch_type = components
                    fields[name] = ch_type
        return fields
