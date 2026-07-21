#include <Core/PostgreSQLProtocol.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/assert_cast.h>

namespace DB::PostgreSQLProtocol::Messaging
{

namespace
{

/// PostgreSQL encodes the precision and scale of a `numeric` in its type modifier as
/// `((precision << 16) | scale) + VARHDRSZ`, where `VARHDRSZ` is 4. This is decoded by `format_type` and
/// by schema inference in `fetchPostgreSQLTableStructure`, so both sides round-trip. The same encoding is
/// produced by the table-name path in the `pg_attribute` emulation (see PostgreSQLHandler).
Int32 encodeNumericTypeModifier(UInt32 precision, UInt32 scale)
{
    return static_cast<Int32>(((precision << 16) | scale) + 4);
}

}

ColumnTypeSpec convertDataTypeToPostgresColumnTypeSpec(const DataTypePtr & data_type_)
{
    /// Unwrap LowCardinality and Nullable so that e.g. `Nullable(UInt64)` is described by the same OID and
    /// type modifier as `UInt64` (otherwise it would fall through to the `VARCHAR` default below).
    DataTypePtr data_type = removeNullable(recursiveRemoveLowCardinality(data_type_));

    // Check for Bool type first
    if (isBool(data_type))
        return {ColumnType::BOOL, 1};

    // Otherwise use TypeIndex
    TypeIndex type_index = data_type->getTypeId();
    switch (type_index)
    {
        case TypeIndex::Int8:
            return {ColumnType::CHAR, 1};

        case TypeIndex::UInt8:
        case TypeIndex::Int16:
            return {ColumnType::INT2, 2};

        case TypeIndex::UInt16:
        case TypeIndex::Int32:
            return {ColumnType::INT4, 4};

        case TypeIndex::UInt32:
        case TypeIndex::Int64:
            return {ColumnType::INT8, 8};

        /// PostgreSQL has neither unsigned integers nor integers wider than a signed 64-bit `bigint`, so the
        /// integer types that do not fit into `bigint` are advertised as `numeric` with a scale of 0 and a
        /// precision large enough to hold every value. The counterpart mapping in `convertPostgreSQLDataType`
        /// turns such a `numeric(p, 0)` back into a Decimal (for the types that fit, e.g. `UInt64` -> the
        /// smallest Decimal) or into `Int256` (for a precision above the Decimal256 range). PostgreSQL
        /// `numeric` is signed, so a `UInt256` value above the `Int256` maximum cannot be recovered as an
        /// unsigned type; it is rejected on the reading side (fail-closed) instead of being silently wrapped
        /// around. This mirrors the table-name path in the `pg_attribute` emulation (see PostgreSQLHandler).
        case TypeIndex::UInt64:
            return {ColumnType::NUMERIC, -1, encodeNumericTypeModifier(20, 0)};
        case TypeIndex::Int128:
        case TypeIndex::UInt128:
            return {ColumnType::NUMERIC, -1, encodeNumericTypeModifier(39, 0)};
        case TypeIndex::Int256:
        case TypeIndex::UInt256:
            return {ColumnType::NUMERIC, -1, encodeNumericTypeModifier(78, 0)};

        case TypeIndex::Float32:
            return {ColumnType::FLOAT4, 4};
        case TypeIndex::Float64:
            return {ColumnType::FLOAT8, 8};

        case TypeIndex::FixedString:
        case TypeIndex::String:
            return {ColumnType::VARCHAR, -1};

        case TypeIndex::Date:
        case TypeIndex::Date32:
            return {ColumnType::DATE, 4};

        /// `DateTime` and `DateTime64` map to PostgreSQL `timestamp` (without time zone), consistent with the
        /// table-name path in the `pg_attribute` emulation (see PostgreSQLHandler). The value is rendered as
        /// text - ClickHouse's `YYYY-MM-DD hh:mm:ss[.ffffff]` form is exactly PostgreSQL's timestamp text
        /// format. PostgreSQL stores the fractional-second precision (0..6) directly in the type modifier
        /// for the time types, so carry the scale there: a `DateTime` has second precision (0), and a
        /// `DateTime64` scale above 6 does not fit PostgreSQL's `timestamp` at all and falls back to the
        /// text type below, preserving the full value. The same text fallback applies to a type with an
        /// explicit time zone (e.g. `DateTime('UTC')`): `timestamp without time zone` cannot carry the
        /// zone, and a reader reconstructing a plain `DateTime`/`DateTime64(p)` would reinterpret the
        /// values in its default time zone, silently shifting the stored epochs.
        case TypeIndex::DateTime:
        {
            if (assert_cast<const DataTypeDateTime &>(*data_type).hasExplicitTimeZone())
                return {ColumnType::VARCHAR, -1};
            return {ColumnType::TIMESTAMP, 8, 0};
        }
        case TypeIndex::DateTime64:
        {
            const auto & date_time64 = assert_cast<const DataTypeDateTime64 &>(*data_type);
            if (date_time64.getScale() <= 6 && !date_time64.hasExplicitTimeZone())
                return {ColumnType::TIMESTAMP, 8, static_cast<Int32>(date_time64.getScale())};
            return {ColumnType::VARCHAR, -1};
        }

        /// Carry the actual precision and scale so that a self-connected `Decimal(p, s)` round-trips through
        /// schema inference instead of collapsing to a bare `numeric` (which `convertPostgreSQLDataType`
        /// would map to `Decimal128`).
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            return {ColumnType::NUMERIC, -1, encodeNumericTypeModifier(getDecimalPrecision(*data_type), getDecimalScale(*data_type))};

        case TypeIndex::UUID:
            return {ColumnType::UUID, 16};

        default:
            return {ColumnType::VARCHAR, -1};
    }
}

}
