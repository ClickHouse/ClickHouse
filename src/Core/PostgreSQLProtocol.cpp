#include <Core/PostgreSQLProtocol.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

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

ColumnTypeSpec convertArrayTypeToPostgresColumnTypeSpec(const DataTypePtr & array_type)
{
    /// Find the innermost non-array element type. LowCardinality was already removed recursively by the
    /// caller; Nullable can wrap the innermost element (`Array(Nullable(T))`) and does not change the OID.
    DataTypePtr element = array_type;
    while (const auto * array = typeid_cast<const DataTypeArray *>(element.get()))
        element = removeNullable(array->getNestedType());

    /// PostgreSQL arrays are variable-length, so `len` is always -1. The element mapping matches the
    /// scalar branches of `convertDataTypeToPostgresColumnTypeSpec` and the `pg_attribute` emulation.
    if (isBool(element))
        return {ColumnType::BOOL_ARRAY, -1};

    switch (element->getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::UInt8:
        case TypeIndex::Int16:
            return {ColumnType::INT2_ARRAY, -1};

        case TypeIndex::UInt16:
        case TypeIndex::Int32:
            return {ColumnType::INT4_ARRAY, -1};

        case TypeIndex::UInt32:
        case TypeIndex::Int64:
            return {ColumnType::INT8_ARRAY, -1};

        case TypeIndex::UInt64:
            return {ColumnType::NUMERIC_ARRAY, -1, encodeNumericTypeModifier(20, 0)};
        case TypeIndex::Int128:
        case TypeIndex::UInt128:
            return {ColumnType::NUMERIC_ARRAY, -1, encodeNumericTypeModifier(39, 0)};
        case TypeIndex::Int256:
        case TypeIndex::UInt256:
            return {ColumnType::NUMERIC_ARRAY, -1, encodeNumericTypeModifier(78, 0)};

        case TypeIndex::Float32:
            return {ColumnType::FLOAT4_ARRAY, -1};
        case TypeIndex::Float64:
            return {ColumnType::FLOAT8_ARRAY, -1};

        case TypeIndex::Date:
        case TypeIndex::Date32:
            return {ColumnType::DATE_ARRAY, -1};

        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            return {ColumnType::NUMERIC_ARRAY, -1, encodeNumericTypeModifier(getDecimalPrecision(*element), getDecimalScale(*element))};

        case TypeIndex::UUID:
            return {ColumnType::UUID_ARRAY, -1};

        /// `String`, `FixedString`, `DateTime`, `DateTime64` and everything else: `text[]`.
        default:
            return {ColumnType::TEXT_ARRAY, -1};
    }
}

}

ColumnTypeSpec convertDataTypeToPostgresColumnTypeSpec(const DataTypePtr & data_type_)
{
    /// Unwrap LowCardinality and Nullable so that e.g. `Nullable(UInt64)` is described by the same OID and
    /// type modifier as `UInt64` (otherwise it would fall through to the `VARCHAR` default below).
    DataTypePtr data_type = removeNullable(recursiveRemoveLowCardinality(data_type_));

    /// An `Array(...)` value is sent as a PostgreSQL array literal (`{...}`, see PostgreSQLOutputFormat),
    /// so describe it with the array OID of its element type, mirroring the `pg_attribute` emulation in
    /// PostgreSQLHandler: the OID is that of the array of the innermost non-array type (PostgreSQL has one
    /// array type per element type regardless of dimensions), and for elements advertised as `numeric` the
    /// type modifier carries the element's precision and scale. Elements without a PostgreSQL counterpart,
    /// including `DateTime`/`DateTime64` (see the comment on the scalar branch below), fall back to `text[]`.
    if (isArray(data_type))
        return convertArrayTypeToPostgresColumnTypeSpec(data_type);

    // Check for Bool type first
    if (isBool(data_type))
        return {ColumnType::BOOL, 1};

    // Otherwise use TypeIndex
    TypeIndex type_index = data_type->getTypeId();
    switch (type_index)
    {
        case TypeIndex::Int8:
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

        /// `DateTime` and `DateTime64` stay on the text fallback (consistent with the table-name path in
        /// the `pg_attribute` emulation, see PostgreSQLHandler), because PostgreSQL's `timestamp without
        /// time zone` cannot carry the time zone the value's wall-clock text is rendered in. For a type
        /// with an explicit zone (e.g. `DateTime('UTC')`) that is obvious; but a `DateTime` without one is
        /// no safer: its text is rendered in the *source* server's default time zone, while a reader that
        /// reconstructs a plain `DateTime`/`DateTime64(p)` reinterprets that text in its *own* default
        /// time zone - whenever the two zones differ, the same wire text becomes a different epoch and the
        /// values are silently shifted. As text the full value round-trips losslessly as `String`.
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
            return {ColumnType::VARCHAR, -1};

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
