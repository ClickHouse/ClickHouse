#include <Dictionaries/CassandraSource.h>

#if USE_CASSANDRA

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/UUID.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
    extern const int UNKNOWN_TYPE;
}

using Cassandra::ColumnType;

CassandraSource::CassandraSource(
    Cassandra::ConnectionPoolPtr pool_,
    const String & query_,
    const SharedHeader & sample_block,
    size_t max_block_size_)
    : ISource(std::make_shared<const Block>(sample_block->cloneEmpty()))
    , pool(std::move(pool_))
    , query(query_)
    , max_block_size(max_block_size_)
{
    description.init(*sample_block);
}

namespace
{

/// Cassandra has no unsigned integers, so the unsigned types are read from the signed ones of the same size.
VectorWithMemoryTracking<ColumnType> compatibleTypes(ExternalResultDescription::ValueType type)
{
    using ValueType = ExternalResultDescription::ValueType;
    switch (type)
    {
        case ValueType::vtUInt8:
            return {ColumnType::Tinyint, ColumnType::Boolean};
        case ValueType::vtInt8:
            return {ColumnType::Tinyint};
        case ValueType::vtUInt16:
        case ValueType::vtInt16:
            return {ColumnType::Smallint};
        case ValueType::vtUInt32:
        case ValueType::vtInt32:
            return {ColumnType::Int};
        case ValueType::vtUInt64:
        case ValueType::vtInt64:
            return {ColumnType::Bigint, ColumnType::Counter};
        case ValueType::vtFloat32:
            return {ColumnType::Float};
        case ValueType::vtFloat64:
            return {ColumnType::Double};
        case ValueType::vtEnum8:
        case ValueType::vtEnum16:
        case ValueType::vtString:
            return {ColumnType::Varchar, ColumnType::Ascii, ColumnType::Blob};
        case ValueType::vtDate:
            return {ColumnType::Date};
        case ValueType::vtDateTime:
            return {ColumnType::Timestamp};
        case ValueType::vtUUID:
            return {ColumnType::Uuid, ColumnType::Timeuuid};
        default:
            return {};
    }
}

template <typename T>
T readFixed(std::string_view value)
{
    if (value.size() != sizeof(T))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size {} of a value from Cassandra, expected {}", value.size(), sizeof(T));
    return unalignedLoadBigEndian<T>(value.data());
}

}

void CassandraSource::insertValue(IColumn & column, ValueType type, std::string_view value)
{
    switch (type)
    {
        case ValueType::vtUInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(readFixed<UInt8>(value));
            break;
        case ValueType::vtUInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(readFixed<UInt16>(value));
            break;
        case ValueType::vtUInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(readFixed<UInt32>(value));
            break;
        case ValueType::vtUInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(readFixed<UInt64>(value));
            break;
        case ValueType::vtInt8:
            assert_cast<ColumnInt8 &>(column).insertValue(static_cast<Int8>(readFixed<UInt8>(value)));
            break;
        case ValueType::vtInt16:
            assert_cast<ColumnInt16 &>(column).insertValue(static_cast<Int16>(readFixed<UInt16>(value)));
            break;
        case ValueType::vtInt32:
            assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(readFixed<UInt32>(value)));
            break;
        case ValueType::vtInt64:
            assert_cast<ColumnInt64 &>(column).insertValue(static_cast<Int64>(readFixed<UInt64>(value)));
            break;
        case ValueType::vtFloat32:
            assert_cast<ColumnFloat32 &>(column).insertValue(std::bit_cast<Float32>(readFixed<UInt32>(value)));
            break;
        case ValueType::vtFloat64:
            assert_cast<ColumnFloat64 &>(column).insertValue(std::bit_cast<Float64>(readFixed<UInt64>(value)));
            break;
        case ValueType::vtEnum8:
        case ValueType::vtEnum16:
        case ValueType::vtString:
            assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
            break;
        case ValueType::vtDate:
        {
            /// The number of days since the epoch, plus 2^31. Out of range values are saturated.
            Int64 days = static_cast<Int64>(readFixed<UInt32>(value)) - (Int64(1) << 31);
            days = std::clamp<Int64>(days, 0, std::numeric_limits<UInt16>::max());
            assert_cast<ColumnUInt16 &>(column).insertValue(static_cast<UInt16>(days));
            break;
        }
        case ValueType::vtDateTime:
        {
            /// Milliseconds since the epoch. Out of range values are saturated.
            Int64 seconds = static_cast<Int64>(readFixed<UInt64>(value)) / 1000;
            seconds = std::clamp<Int64>(seconds, 0, std::numeric_limits<UInt32>::max());
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(seconds));
            break;
        }
        case ValueType::vtUUID:
        {
            if (value.size() != 16)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size {} of a UUID value from Cassandra, expected 16", value.size());
            UUID uuid;
            UUIDHelpers::getHighBytes(uuid) = unalignedLoadBigEndian<UInt64>(value.data());
            UUIDHelpers::getLowBytes(uuid) = unalignedLoadBigEndian<UInt64>(value.data() + 8);
            assert_cast<ColumnUUID &>(column).insertValue(uuid);
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unsupported type {} for reading from Cassandra", static_cast<int>(type));
    }
}

void CassandraSource::checkTypes(const Cassandra::ResultPage & page) const
{
    if (page.columns() != description.sample_block.columns())
        throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
            "The query to Cassandra returned {} columns, expected {}", page.columns(), description.sample_block.columns());

    for (size_t i = 0; i < page.columns(); ++i)
    {
        const auto & name = description.sample_block.getByPosition(i).name;
        const auto compatible = compatibleTypes(description.types[i].first);
        if (compatible.empty())
            throw Exception(ErrorCodes::UNKNOWN_TYPE,
                "Type {} of column {} is not supported for reading from Cassandra",
                description.sample_block.getByPosition(i).type->getName(), name);

        const ColumnType got = page.columnSpecs()[i].type;
        if (std::find(compatible.begin(), compatible.end(), got) == compatible.end())
        {
            String expected;
            for (ColumnType type : compatible)
                expected += (expected.empty() ? "" : " or ") + Cassandra::columnTypeToString(type);

            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Type mismatch for column {}: expected Cassandra type {}, got {}",
                name, expected, Cassandra::columnTypeToString(got));
        }
    }
}

Chunk CassandraSource::generate()
{
    if (is_finished)
        return {};

    Cassandra::ResultPage page = pool->query(query, static_cast<Int32>(max_block_size), paging_state);

    if (!types_checked)
    {
        checkTypes(page);
        types_checked = true;
    }

    if (page.hasMorePages())
        paging_state = page.pagingState();
    else
        is_finished = true;

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    for (auto & column : columns)
        column->reserve(page.rows());

    for (size_t row = 0; row < page.rows(); ++row)
    {
        for (size_t col = 0; col < columns.size(); ++col)
        {
            auto value = page.value(row, col);
            if (!value)
                columns[col]->insertDefault();
            else if (description.types[col].second)
            {
                ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[col]);
                insertValue(column_nullable.getNestedColumn(), description.types[col].first, *value);
                column_nullable.getNullMapData().emplace_back(false);
            }
            else
                insertValue(*columns[col], description.types[col].first, *value);
        }
    }

    /// A page may be empty, while there are more pages.
    size_t num_rows = page.rows();
    return Chunk(std::move(columns), num_rows);
}

}

#endif
