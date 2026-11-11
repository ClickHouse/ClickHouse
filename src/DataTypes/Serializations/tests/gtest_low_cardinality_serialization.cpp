#include <Columns/IColumn.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <base/unaligned.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

using namespace DB;

namespace
{

/// The MergeTree layout: the dictionary in one stream, the indexes and the additional keys in another.
template <typename BufferPtr, typename Buffer>
auto makeGetter(Buffer & keys, Buffer & indexes)
{
    return [&keys, &indexes](const ISerialization::SubstreamPath & path) -> BufferPtr
    {
        if (!path.empty() && path.back().type == ISerialization::Substream::DictionaryKeys)
            return &keys;
        return &indexes;
    };
}

struct Streams
{
    std::string keys;
    std::string indexes;
};

Streams serialize(const DataTypePtr & type, const IColumn & column, size_t max_dictionary_size)
{
    WriteBufferFromOwnString keys;
    WriteBufferFromOwnString indexes;
    auto serialization = type->getDefaultSerialization();
    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = makeGetter<WriteBuffer *>(keys, indexes);
    settings.low_cardinality_max_dictionary_size = max_dictionary_size;
    settings.low_cardinality_use_single_dictionary_for_part = false;
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, column.size(), settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
    keys.finalize();
    indexes.finalize();
    return {keys.str(), indexes.str()};
}

ColumnPtr deserialize(const DataTypePtr & type, const Streams & streams, size_t rows)
{
    ReadBufferFromString keys(streams.keys);
    ReadBufferFromString indexes(streams.indexes);
    auto serialization = type->getDefaultSerialization();
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = makeGetter<ReadBuffer *>(keys, indexes);
    settings.native_format = false;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    auto column = type->createColumn();
    serialization->deserializeBinaryBulkWithMultipleStreams(*column, rows, settings, state, nullptr);
    return column;
}

void expectIncorrectData(const DataTypePtr & type, const Streams & streams, size_t rows)
{
    try
    {
        deserialize(type, streams, rows);
        FAIL() << "accepted an index past the keys";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.message();
    }
}

/// Calls `check` for `LowCardinality(String)` and `LowCardinality(Nullable(UInt64))`, each serialized with a dictionary
/// of 2 keys (so most values become additional keys) and with one large enough for all values (no additional keys).
template <typename Check>
void forEachCase(Check && check)
{
    for (bool nullable : {false, true})
    {
        DataTypePtr inner = nullable ? makeNullable(std::make_shared<DataTypeUInt64>()) : std::make_shared<DataTypeString>();
        auto type = std::make_shared<DataTypeLowCardinality>(inner);
        auto column = type->createColumn();
        for (size_t i = 0; i < 20; ++i)
        {
            if (nullable)
                column->insert(i % 6 == 0 ? Field() : Field(UInt64(i % 6)));
            else
                column->insert(String(1, static_cast<char>('a' + i % 5)));
        }

        for (size_t max_dictionary_size : {2, 8192})
        {
            SCOPED_TRACE(type->getName() + " with max dictionary size " + std::to_string(max_dictionary_size));
            auto streams = serialize(type, *column, max_dictionary_size);

            constexpr UInt64 need_global_dictionary_bit = 1ULL << 8;
            constexpr UInt64 has_additional_keys_bit = 1ULL << 9;
            const UInt64 index_type = unalignedLoadLittleEndian<UInt64>(streams.indexes.data());
            const bool has_additional_keys = (index_type & has_additional_keys_bit) != 0;
            ASSERT_NE(index_type & need_global_dictionary_bit, 0u);
            ASSERT_EQ(has_additional_keys, max_dictionary_size == 2);
            ASSERT_LE(index_type & 0xff, 3u);

            /// The keys stream starts with the version, then the number of keys; the indexes stream starts with the index type,
            /// then the number of additional keys if there are any.
            const UInt64 num_keys = unalignedLoadLittleEndian<UInt64>(streams.keys.data() + 8)
                + (has_additional_keys ? unalignedLoadLittleEndian<UInt64>(streams.indexes.data() + 8) : 0);
            const size_t index_width = 1ULL << (index_type & 0xff);

            check(type, *column, std::move(streams), num_keys, index_width);
        }
    }
}

}

TEST(SerializationLowCardinality, ValidStreamsRoundTrip)
{
    MainThreadStatus::getInstance();
    forEachCase([](const DataTypePtr & type, const IColumn & column, Streams streams, UInt64, size_t)
    {
        auto result = deserialize(type, streams, column.size());
        ASSERT_EQ(result->size(), column.size());
        for (size_t i = 0; i < column.size(); ++i)
            ASSERT_EQ((*result)[i], column[i]) << "row " << i;
    });
}

/// The indexes come from the data, so they have to be checked against the keys of the dictionary and the additional keys.
TEST(SerializationLowCardinality, IndexPastTheKeysIsRejected)
{
    MainThreadStatus::getInstance();
    forEachCase([](const DataTypePtr & type, const IColumn & column, Streams streams, UInt64 num_keys, size_t index_width)
    {
        const size_t rows = column.size();
        const size_t indexes_begin = streams.indexes.size() - rows * index_width;

        /// The same indexes as `UInt64` (index type 3, the low byte of the index type) read back the same values...
        Streams wide{streams.keys, streams.indexes.substr(0, indexes_begin)};
        wide.indexes[0] = 3;
        for (size_t row = 0; row < rows; ++row)
        {
            UInt64 index = 0;
            for (size_t i = 0; i < index_width; ++i)
                index |= UInt64(static_cast<UInt8>(streams.indexes[indexes_begin + row * index_width + i])) << (8 * i);
            for (size_t i = 0; i < 8; ++i)
                wide.indexes.push_back(static_cast<char>(index >> (8 * i)));
        }
        auto result = deserialize(type, wide, rows);
        ASSERT_EQ(result->size(), rows);
        for (size_t row = 0; row < rows; ++row)
            ASSERT_EQ((*result)[row], column[row]) << "row " << row;

        /// ...and the largest possible last index is rejected before `mapIndexWithAdditionalKeys` sizes its overflow map by it.
        std::fill(wide.indexes.end() - 8, wide.indexes.end(), '\xff');
        expectIncorrectData(type, wide, rows);

        /// The last index of the original stream becomes the first value past the keys.
        ASSERT_LE(num_keys, std::numeric_limits<UInt64>::max() >> (64 - 8 * index_width));
        for (size_t i = 0; i < index_width; ++i)
            streams.indexes[streams.indexes.size() - index_width + i] = static_cast<char>(num_keys >> (8 * i));
        expectIncorrectData(type, streams, rows);
    });
}
