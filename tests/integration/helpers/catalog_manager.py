import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
)


def arrow_type_to_iceberg(arrow_type):
    """Convert a PyArrow type to a PyIceberg type."""
    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    if (
        pa.types.is_int8(arrow_type)
        or pa.types.is_int16(arrow_type)
        or pa.types.is_int32(arrow_type)
    ):
        return IntegerType()
    if pa.types.is_int64(arrow_type):
        return LongType()
    if pa.types.is_float32(arrow_type):
        return FloatType()
    if pa.types.is_float64(arrow_type):
        return DoubleType()
    if pa.types.is_decimal(arrow_type):
        return DecimalType(precision=arrow_type.precision, scale=arrow_type.scale)
    if pa.types.is_date(arrow_type):
        return DateType()
    if pa.types.is_timestamp(arrow_type):
        if arrow_type.tz:
            return TimestamptzType()
        return TimestampType()
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return StringType()
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return BinaryType()
    if pa.types.is_list(arrow_type):
        return ListType(
            element_id=1,
            element=arrow_type_to_iceberg(arrow_type.value_type),
            element_required=not arrow_type.value_field.nullable,
        )
    if pa.types.is_map(arrow_type):
        return MapType(
            key_id=1,
            key_type=arrow_type_to_iceberg(arrow_type.key_type),
            value_id=2,
            value_type=arrow_type_to_iceberg(arrow_type.item_type),
            value_required=not arrow_type.item_field.nullable,
        )
    if pa.types.is_struct(arrow_type):
        fields = []
        for i, field in enumerate(arrow_type):
            fields.append(
                NestedField(
                    field_id=i + 1,
                    name=field.name,
                    field_type=arrow_type_to_iceberg(field.type),
                    required=not field.nullable,
                )
            )
        return StructType(*fields)
    raise ValueError(f"Unsupported PyArrow type: {arrow_type}")


def arrow_to_iceberg_schema(data: pa.Table) -> Schema:
    """Convert a PyArrow table's schema to a PyIceberg Schema."""
    return Schema(
        *[
            NestedField(
                field_id=i + 1,
                name=field.name,
                field_type=arrow_type_to_iceberg(field.type),
                required=not field.nullable,
            )
            for i, field in enumerate(data.schema)
        ]
    )


class CatalogManager(ABC):
    """Unified interface for DataLakeCatalog e2e test backends."""

    @classmethod
    @abstractmethod
    def from_env(cls) -> "CatalogManager":
        ...

    @staticmethod
    @abstractmethod
    def make_database_name() -> str:
        ...

    @abstractmethod
    def create_catalog(self, node, database_name: str) -> None:
        """Create a DataLakeCatalog database in ClickHouse."""
        ...

    @abstractmethod
    def create_table(
        self, data: pa.Table, table_name: Optional[str] = None
    ) -> str:
        """Create a table from Arrow data. Returns a short table name."""
        ...

    def resolve_table_name(
        self, node, database_name: str, short_name: str, retries: int = 5, delay: float = 3.0
    ) -> str:
        """Resolve a short table name to its fully-qualified form
        as returned by SHOW TABLES (e.g. ``dbo.X`` or ``ns.X``).

        Retries several times with a short sleep to tolerate eventual
        consistency in cloud catalog backends (e.g. AWS Glue)."""
        raw = ""
        for attempt in range(retries):
            raw = node.query(f"SHOW TABLES FROM {database_name}").strip()
            for line in raw.splitlines():
                if short_name in line:
                    return line.strip()
            if attempt < retries - 1:
                time.sleep(delay)
        raise AssertionError(
            f"Table '{short_name}' not found in SHOW TABLES output:\n{raw}"
        )

    @abstractmethod
    def wait_for_table_ready(self, table_name: str) -> None:
        """Wait until a specific table is queryable."""
        ...

    @abstractmethod
    def wait_for_table_gone(self, table_name: str) -> None:
        """Wait until a specific table is no longer visible in the catalog."""
        ...

    @abstractmethod
    def cleanup_table(self, table_name: str) -> None:
        ...

    @abstractmethod
    def cleanup_all(self) -> None:
        ...

    def clickhouse_env_variables(self) -> Dict[str, str]:
        return {}

    @property
    def main_configs(self) -> List[str]:
        return []

    @property
    def user_configs(self) -> List[str]:
        return []
