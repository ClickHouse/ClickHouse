#include <Core/PostgreSQLProtocol.h>

namespace DB::PostgreSQLProtocol::Messaging
{

ColumnTypeSpec convertTypeIndexToPostgresColumnTypeSpec(TypeIndex type_index)
{
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
            return {ColumnType::UUID, 16};

        default:
            return {ColumnType::VARCHAR, -1};
    }
}

}
