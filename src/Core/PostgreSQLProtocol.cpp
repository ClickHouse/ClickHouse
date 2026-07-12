#include <Core/PostgreSQLProtocol.h>
#include <DataTypes/IDataType.h>

namespace DB::PostgreSQLProtocol::Messaging
{

ColumnTypeSpec convertDataTypeToPostgresColumnTypeSpec(const DataTypePtr & data_type)
{
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

        case TypeIndex::Float32:
            return {ColumnType::FLOAT4, 4};
        case TypeIndex::Float64:
            return {ColumnType::FLOAT8, 8};

        case TypeIndex::FixedString:
        case TypeIndex::String:
            return {ColumnType::VARCHAR, -1};

        case TypeIndex::Date:
            return {ColumnType::DATE, 4};

        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            return {ColumnType::NUMERIC, -1};

        case TypeIndex::UUID:
        /// `UUID2` is exposed to PostgreSQL clients as the same `uuid` type: the wire values are serialized as text
        /// and `UUID2` produces the canonical UUID string, so existing clients keep decoding these columns as UUIDs
        /// even when a bare `UUID` column is materialized to `UUID2` under `uuid_type_version = 2`.
        case TypeIndex::UUID2:
            return {ColumnType::UUID, 16};

        default:
            return {ColumnType::VARCHAR, -1};
    }
}

}
