#include <DataStreams/ColumnGathererStream.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/formatReadable.h>
#include <IO/WriteHelpers.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int EMPTY_DATA_PASSED;
    extern const int RECEIVED_EMPTY_DATA;
}

ColumnGathererStream::ColumnGathererStream(
        const String & column_name_, const BlockInputStreams & source_streams, ReadBuffer & row_sources_buf_,
        bool enable_low_cardinality_merge_new_algo_, size_t block_preferred_size_)
    : column_name(column_name_), sources(source_streams.size()), row_sources_buf(row_sources_buf_)
    , enable_low_cardinality_merge_new_algo(enable_low_cardinality_merge_new_algo_)
    , block_preferred_size(block_preferred_size_), log(&Poco::Logger::get("ColumnGathererStream"))
{
    if (source_streams.empty())
        throw Exception("There are no streams to gather", ErrorCodes::EMPTY_DATA_PASSED);

    children.assign(source_streams.begin(), source_streams.end());

    for (size_t i = 0; i < children.size(); ++i)
    {
        const Block & header = children[i]->getHeader();

        /// Sometimes MergeTreeReader injects additional column with partitioning key
        if (header.columns() > 2)
            throw Exception(
                "Block should have 1 or 2 columns, but contains " + toString(header.columns()),
                ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

        if (i == 0)
        {
            column.name = column_name;
            column.type = header.getByName(column_name).type;
            column.column = column.type->createColumn();
        }
        else if (header.getByName(column_name).column->getName() != column.column->getName())
            throw Exception("Column types don't match", ErrorCodes::INCOMPATIBLE_COLUMNS);
    }

    LOG_DEBUG(log, "Merge low cardinality column use new algo: {}", enable_low_cardinality_merge_new_algo);
}


Block ColumnGathererStream::readImpl()
{
    /// Special case: single source and there are no skipped rows
    if (children.size() == 1 && row_sources_buf.eof() && !source_to_fully_copy)
        return children[0]->read();

    if (!source_to_fully_copy && row_sources_buf.eof())
        return Block();

    MutableColumnPtr output_column = column.column->cloneEmpty();
    output_block = Block{column.cloneEmpty()};
    /// Surprisingly this call may directly change output_block, bypassing
    /// output_column. See ColumnGathererStream::gather.
    output_column->gather(*this);
    if (!output_column->empty())
        output_block.getByPosition(0).column = std::move(output_column);

    return output_block;
}


void ColumnGathererStream::fetchNewBlock(Source & source, size_t source_num)
{
    try
    {
        source.block = children[source_num]->read();
        source.update(column_name);
    }
    catch (Exception & e)
    {
        e.addMessage("Cannot fetch required block. Stream " + children[source_num]->getName() + ", part " + toString(source_num));
        throw;
    }

    if (0 == source.size)
    {
        throw Exception("Fetched block is empty. Stream " + children[source_num]->getName() + ", part " + toString(source_num),
                        ErrorCodes::RECEIVED_EMPTY_DATA);
    }
}


void ColumnGathererStream::readSuffixImpl()
{
    const BlockStreamProfileInfo & profile_info = getProfileInfo();

    /// Don't print info for small parts (< 10M rows)
    if (profile_info.rows < 10000000)
        return;

    double seconds = profile_info.total_stopwatch.elapsedSeconds();

    if (!seconds)
        LOG_DEBUG(log, "Gathered column {} ({} bytes/elem.) in 0 sec.",
            column_name, static_cast<double>(profile_info.bytes) / profile_info.rows);
    else
        LOG_DEBUG(log, "Gathered column {} ({} bytes/elem.) in {} sec., {} rows/sec., {}/sec.",
            column_name, static_cast<double>(profile_info.bytes) / profile_info.rows, seconds,
            profile_info.rows / seconds, ReadableSize(profile_info.bytes / seconds));
}

void ColumnGathererStream::gatherLowCardinality(ColumnLowCardinality &column_res)
{
    if (!enable_low_cardinality_merge_new_algo)
    {
        gather(column_res);
        return ;
    }

    row_sources_buf.nextIfAtEnd();
    RowSourcePart * row_source_pos = reinterpret_cast<RowSourcePart *>(row_sources_buf.position());
    RowSourcePart * row_sources_end = reinterpret_cast<RowSourcePart *>(row_sources_buf.buffer().end());

    size_t cur_block_preferred_size = static_cast<size_t>(row_sources_end - row_source_pos);
    column_res.reserve(cur_block_preferred_size);

    if (cardinalityDict)
        column_res.loadDictionaryFrom(*cardinalityDict);

    while (row_source_pos < row_sources_end)
    {
        RowSourcePart row_source = *row_source_pos;
        size_t source_num = row_source.getSourceNum();
        Source & source = sources[source_num];
        bool source_skip = row_source.getSkipFlag();
        ++row_source_pos;

        if (source.pos >= source.size) /// Fetch new block from source_num part
        {
            fetchNewBlock(source, source_num);
            column_res.mergeGatherColumn(*source.column, source.index_map);
            if (!source.index_map.empty())
            {
                auto *low_ref = const_cast<IColumn *>(source.column);
                auto *low_src = typeid_cast<ColumnLowCardinality *>(low_ref);
                low_src->transformIndex(source.index_map, column_res.getDictionary().size());
            }
        }

        size_t len = 1;
        size_t max_len = std::min(static_cast<size_t>(row_sources_end - row_source_pos), source.size - source.pos); // interval should be in the same block
        while (len < max_len && row_source_pos->data == row_source.data)
        {
            ++len;
            ++row_source_pos;
        }

        row_sources_buf.position() = reinterpret_cast<char *>(row_source_pos);

        if (!source_skip)
        {
            if (len == 1)
                column_res.insertIndexFrom(*source.column, source.pos);
            else
                column_res.insertIndexRangeFrom(*source.column, source.pos, len);
        }

        source.pos += len;
    }

    bool need_save_dict = false;
    for (auto &ref : sources)
    {
        if (ref.pos >= ref.size)
        {
            ref.index_map.clear();
        }
        else
        {
            need_save_dict = true;
        }
    }

    if (need_save_dict)
    {
        LOG_DEBUG(log, "save dict size: {}", column_res.getDictionary().size());
        cardinalityDict = column_res.cloneEmpty();
        (typeid_cast<ColumnLowCardinality *>(cardinalityDict.get()))->loadDictionaryFrom(column_res);
    }
    else
    {
        cardinalityDict = nullptr;
    }
}


}
