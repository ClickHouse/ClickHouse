// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <Storages/PartitionedSink.h>

#include <Common/ArenaUtils.h>
#include <Core/Settings.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <Parsers/ASTFunction.h>

#include <Processors/ISource.h>

#include <boost/algorithm/string/replace.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int INCORRECT_DATA;
}

PartitionedSink::PartitionedSink(
    std::shared_ptr<IPartitionStrategy> partition_strategy_,
    ContextPtr context_,
    SharedHeader source_header_)
    : SinkToStorage(source_header_)
    , partition_strategy(partition_strategy_)
    , context(context_)
    , source_header(source_header_)
{
}


SinkPtr PartitionedSink::getSinkForPartitionKey(StringRef partition_key)
{
    auto it = partition_id_to_sink.find(partition_key);
    if (it == partition_id_to_sink.end())
    {
        auto sink = createSinkForPartition(partition_key.toString());
        std::tie(it, std::ignore) = partition_id_to_sink.emplace(partition_key, sink);
    }

    return it->second;
}

void PartitionedSink::consume(Chunk & source_chunk)
{
    const ColumnPtr partition_by_result_column = partition_strategy->computePartitionKey(source_chunk);

    /// Not all columns are serialized using the format writer (e.g, hive partitioning stores partition columns in the file path)
    const auto columns_to_consume = partition_strategy->getFormatChunkColumns(source_chunk);

    if (columns_to_consume.empty())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
                        "No column to write as all columns are specified as partition columns. "
                        "Consider setting `partition_columns_in_data_file=1`");
    }

    size_t chunk_rows = source_chunk.getNumRows();
    chunk_row_index_to_partition_index.resize(chunk_rows);

    partition_id_to_chunk_index.clear();

    for (size_t row = 0; row < chunk_rows; ++row)
    {
        auto partition_key = partition_by_result_column->getDataAt(row);
        auto [it, inserted] = partition_id_to_chunk_index.insert(makePairNoInit(partition_key, partition_id_to_chunk_index.size()));
        if (inserted)
            it->value.first = copyStringInArena(partition_keys_arena, partition_key);

        chunk_row_index_to_partition_index[row] = it->getMapped();
    }

    size_t columns_size = columns_to_consume.size();
    size_t partitions_size = partition_id_to_chunk_index.size();

    Chunks partition_index_to_chunk;
    partition_index_to_chunk.reserve(partitions_size);

    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        MutableColumns partition_index_to_column_split = columns_to_consume[column_index]->scatter(partitions_size, chunk_row_index_to_partition_index);

        /// Add chunks into partition_index_to_chunk with sizes of result columns
        if (column_index == 0)
        {
            for (const auto & partition_column : partition_index_to_column_split)
            {
                partition_index_to_chunk.emplace_back(Columns(), partition_column->size());
            }
        }

        for (size_t partition_index = 0; partition_index  < partitions_size; ++partition_index)
        {
            partition_index_to_chunk[partition_index].addColumn(std::move(partition_index_to_column_split[partition_index]));
        }
    }

    for (const auto & [partition_key, partition_index] : partition_id_to_chunk_index)
    {
        auto sink = getSinkForPartitionKey(partition_key);
        sink->consume(partition_index_to_chunk[partition_index]);
    }
}

void PartitionedSink::onException(std::exception_ptr exception)
{
    for (auto & [_, sink] : partition_id_to_sink)
    {
        sink->onException(exception);
    }
}

void PartitionedSink::onFinish()
{
    for (auto & [_, sink] : partition_id_to_sink)
    {
        sink->onFinish();
    }
}


void PartitionedSink::validatePartitionKey(const String & str, bool allow_slash)
{
    for (const char * i = str.data(); i != str.data() + str.size(); ++i)
    {
        if (static_cast<UInt8>(*i) < 0x20 || *i == '{' || *i == '}' || *i == '*' || *i == '?' || (!allow_slash && *i == '/'))
        {
            /// Need to convert to UInt32 because UInt8 can't be passed to format due to "mixing character types is disallowed".
            UInt32 invalid_char_byte = static_cast<UInt32>(static_cast<UInt8>(*i));
            throw DB::Exception(
                ErrorCodes::CANNOT_PARSE_TEXT, "Illegal character '\\x{:02x}' in partition id starting with '{}'",
                invalid_char_byte, std::string(str.data(), i - str.data()));
        }
    }
}


String PartitionedSink::replaceWildcards(const String & haystack, const String & partition_id)
{
    return boost::replace_all_copy(haystack, PartitionedSink::PARTITION_ID_WILDCARD, partition_id);
}

PartitionedSink::~PartitionedSink()
{
    if (isCancelled())
        for (auto & item : partition_id_to_sink)
            item.second->cancel();
}
}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
