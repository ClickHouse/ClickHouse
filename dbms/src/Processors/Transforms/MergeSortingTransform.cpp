#include <Processors/Transforms/MergeSortingTransform.h>

#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>

#include <queue>
#include <Processors/ISource.h>
#include <Processors/Transforms/MergingSortedTransform.h>


namespace ProfileEvents
{
    extern const Event ExternalSortWritePart;
    extern const Event ExternalSortMerge;
}


namespace DB
{

class BufferingToFileTransform : public IAccumulatingTransform
{
public:
    BufferingToFileTransform(const Block & header, Logger * log_, std::string path_)
        : IAccumulatingTransform(header, header), log(log_)
        , path(std::move(path_)), file_buf_out(path), compressed_buf_out(file_buf_out)
        , out_stream(std::make_shared<NativeBlockOutputStream>(compressed_buf_out, 0, header))
    {
        LOG_INFO(log, "Sorting and writing part of data into temporary file " + path);
        ProfileEvents::increment(ProfileEvents::ExternalSortWritePart);
        out_stream->writePrefix();
    }

    String getName() const override { return "BufferingToFileTransform"; }

    void consume(Chunk chunk) override
    {
        out_stream->write(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    Chunk generate() override
    {
        if (out_stream)
        {
            out_stream->writeSuffix();
            compressed_buf_out.next();
            file_buf_out.next();
            LOG_INFO(log, "Done writing part of data into temporary file " + path);

            out_stream.reset();

            file_in = std::make_unique<ReadBufferFromFile>(path);
            compressed_in = std::make_unique<CompressedReadBuffer>(*file_in);
            block_in = std::make_shared<NativeBlockInputStream>(*compressed_in, getOutputPort().getHeader(), 0);
        }

        if (!block_in)
            return {};

        auto block = block_in->read();
        if (!block)
        {
            block_in->readSuffix();
            block_in.reset();
            return {};
        }

        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

private:
    Logger * log;
    std::string path;
    WriteBufferFromFile file_buf_out;
    CompressedWriteBuffer compressed_buf_out;
    BlockOutputStreamPtr out_stream;

    std::unique_ptr<ReadBufferFromFile> file_in;
    std::unique_ptr<CompressedReadBuffer> compressed_in;
    BlockInputStreamPtr block_in;
};

/** Part of implementation. Merging array of ready (already read from somewhere) chunks.
  * Returns result of merge as stream of chunks, not more than 'max_merged_block_size' rows in each.
  */
class MergeSorter
{
public:
    MergeSorter(Chunks chunks_, SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_);

    Chunk read();

private:
    Chunks chunks;
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;
    size_t total_merged_rows = 0;

    using CursorImpls = std::vector<SortCursorImpl>;
    CursorImpls cursors;

    bool has_collation = false;

    std::priority_queue<SortCursor> queue_without_collation;
    std::priority_queue<SortCursorWithCollation> queue_with_collation;

    /** Two different cursors are supported - with and without Collation.
      *  Templates are used (instead of virtual functions in SortCursor) for zero-overhead.
      */
    template <typename TSortCursor>
    Chunk mergeImpl(std::priority_queue<TSortCursor> & queue);
};

class MergeSorterSource : public ISource
{
public:
    MergeSorterSource(Block header, Chunks chunks, SortDescription & description, size_t max_merged_block_size, UInt64 limit)
        : ISource(std::move(header)), merge_sorter(std::move(chunks), description, max_merged_block_size, limit) {}

    String getName() const override { return "MergeSorterSource"; }

protected:
    Chunk generate() override { return merge_sorter.read(); }

private:
    MergeSorter merge_sorter;
};

MergeSorter::MergeSorter(Chunks chunks_, SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_)
    : chunks(std::move(chunks_)), description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_)
{
    Chunks nonempty_chunks;
    for (auto & chunk : chunks)
    {
        if (chunk.getNumRows() == 0)
            continue;

        cursors.emplace_back(chunk.getColumns(), description);
        has_collation |= cursors.back().has_collation;

        nonempty_chunks.emplace_back(std::move(chunk));
    }

    chunks.swap(nonempty_chunks);

    if (!has_collation)
    {
        for (auto & cursor : cursors)
            queue_without_collation.push(SortCursor(&cursor));
    }
    else
    {
        for (auto & cursor : cursors)
            queue_with_collation.push(SortCursorWithCollation(&cursor));
    }
}


Chunk MergeSorter::read()
{
    if (chunks.empty())
        return Chunk();

    if (chunks.size() == 1)
    {
        auto res = std::move(chunks[0]);
        chunks.clear();
        return res;
    }

    return !has_collation
           ? mergeImpl<SortCursor>(queue_without_collation)
           : mergeImpl<SortCursorWithCollation>(queue_with_collation);
}


template <typename TSortCursor>
Chunk MergeSorter::mergeImpl(std::priority_queue<TSortCursor> & queue)
{
    size_t num_columns = chunks[0].getNumColumns();

    MutableColumns merged_columns = chunks[0].cloneEmptyColumns();
    /// TODO: reserve (in each column)

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!queue.empty())
    {
        TSortCursor current = queue.top();
        queue.pop();

        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

        ++total_merged_rows;
        ++merged_rows;

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }

        if (limit && total_merged_rows == limit)
        {
            chunks.clear();
            return Chunk(std::move(merged_columns), merged_rows);
        }

        if (merged_rows == max_merged_block_size)
            return Chunk(std::move(merged_columns), merged_rows);
    }

    chunks.clear();

    if (merged_rows == 0)
        return {};

    return Chunk(std::move(merged_columns), merged_rows);
}


MergeSortingTransform::MergeSortingTransform(
    const Block & header,
    SortDescription & description_,
    size_t max_merged_block_size_, UInt64 limit_,
    size_t max_bytes_before_remerge_,
    size_t max_bytes_before_external_sort_, const std::string & tmp_path_)
    : IProcessor({header}, {header})
    , description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_path(tmp_path_)
{
    auto & sample = inputs.front().getHeader();

    /// Replace column names to column position in sort_description.
    for (auto & column_description : description)
    {
        if (!column_description.column_name.empty())
        {
            column_description.column_number = sample.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }

    /// Remove constants from header and map old indexes to new.
    size_t num_columns = sample.columns();
    ColumnNumbers map(num_columns, num_columns);
    const_columns_to_remove.assign(num_columns, true);
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & column = sample.getByPosition(pos);
        if (!(column.column && column.column->isColumnConst()))
        {
            map[pos] = header_without_constants.columns();
            header_without_constants.insert(column);
            const_columns_to_remove[pos] = false;
        }
    }

    /// Remove constants from column_description and remap positions.
    SortDescription description_without_constants;
    description_without_constants.reserve(description.size());
    for (const auto & column_description : description)
    {
        auto old_pos = column_description.column_number;
        auto new_pos = map[old_pos];
        if (new_pos < num_columns)
        {
            description_without_constants.push_back(column_description);
            description_without_constants.back().column_number = new_pos;
        }
    }

    description.swap(description_without_constants);
}

MergeSortingTransform::~MergeSortingTransform() = default;

IProcessor::Status MergeSortingTransform::prepare()
{
    if (stage == Stage::Serialize)
    {
        if (!processors.empty())
            return Status::ExpandPipeline;

        auto status = prepareSerialize();
        if (status != Status::Finished)
            return status;

        stage = Stage::Consume;
    }

    if (stage == Stage::Consume)
    {
        auto status = prepareConsume();
        if (status != Status::Finished)
            return status;

        stage = Stage::Generate;
    }

    /// stage == Stage::Generate

    if (!generated_prefix)
        return Status::Ready;

    if (!processors.empty())
        return Status::ExpandPipeline;

    return prepareGenerate();
}

IProcessor::Status MergeSortingTransform::prepareConsume()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    /// Check can input.
    if (!current_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        current_chunk = input.pull();
    }

    /// Now consume.
    return Status::Ready;
}

IProcessor::Status MergeSortingTransform::prepareSerialize()
{
    auto & output = outputs.back();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (current_chunk)
        output.push(std::move(current_chunk));

    if (merge_sorter)
        return Status::Ready;

    output.finish();
    return Status::Finished;
}

IProcessor::Status MergeSortingTransform::prepareGenerate()
{
    auto & output = outputs.front();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (merge_sorter)
    {
        if (!generated_chunk)
            return Status::Ready;

        output.push(std::move(generated_chunk));
        return Status::PortFull;
    }
    else
    {
        auto & input = inputs.back();

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        auto chunk = input.pull();
        enrichChunkWithConstants(chunk);
        output.push(std::move(chunk));
        return Status::PortFull;
    }
}

void MergeSortingTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));

    if (stage == Stage::Serialize)
        serialize();

    if (stage == Stage::Generate)
        generate();
}

Processors MergeSortingTransform::expandPipeline()
{
    if (processors.size() > 1)
    {
        /// Add external_merging_sorted.
        inputs.emplace_back(header_without_constants, this);
        connect(external_merging_sorted->getOutputs().front(), inputs.back());
    }

    auto & buffer = processors.front();

    static_cast<MergingSortedTransform &>(*external_merging_sorted).addInput();
    connect(buffer->getOutputs().back(), external_merging_sorted->getInputs().back());

    if (!buffer->getInputs().empty())
    {
        /// Serialize
        outputs.emplace_back(header_without_constants, this);
        connect(getOutputs().back(), buffer->getInputs().back());
        /// Hack. Say buffer that we need data from port (otherwise it will return PortFull).
        external_merging_sorted->getInputs().back().setNeeded();
    }
    else
        /// Generate
        static_cast<MergingSortedTransform &>(*external_merging_sorted).setHaveAllInputs();

    return std::move(processors);
}

void MergeSortingTransform::consume(Chunk chunk)
{
    /** Algorithm:
      * - read to memory blocks from source stream;
      * - if too many of them and if external sorting is enabled,
      *   - merge all blocks to sorted stream and write it to temporary file;
      * - at the end, merge all sorted streams from temporary files and also from rest of blocks in memory.
      */

    /// If there were only const columns in sort description, then there is no need to sort.
    /// Return the chunk as is.
    if (description.empty())
    {
        generated_chunk = std::move(chunk);
        return;
    }

    removeConstColumns(chunk);

    sum_rows_in_blocks += chunk.getNumRows();
    sum_bytes_in_blocks += chunk.allocatedBytes();
    chunks.push_back(std::move(chunk));

    /** If significant amount of data was accumulated, perform preliminary merging step.
      */
    if (chunks.size() > 1
        && limit
        && limit * 2 < sum_rows_in_blocks   /// 2 is just a guess.
        && remerge_is_useful
        && max_bytes_before_remerge
        && sum_bytes_in_blocks > max_bytes_before_remerge)
    {
        remerge();
    }

    /** If too many of them and if external sorting is enabled,
      *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
      * NOTE. It's possible to check free space in filesystem.
      */
    if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
    {
        Poco::File(tmp_path).createDirectories();
        temporary_files.emplace_back(std::make_unique<Poco::TemporaryFile>(tmp_path));
        const std::string & path = temporary_files.back()->path();
        merge_sorter = std::make_unique<MergeSorter>(std::move(chunks), description, max_merged_block_size, limit);
        auto current_processor = std::make_shared<BufferingToFileTransform>(header_without_constants, log, path);

        processors.emplace_back(current_processor);

        if (!external_merging_sorted)
        {
            bool quiet = false;
            bool have_all_inputs = false;

            external_merging_sorted = std::make_shared<MergingSortedTransform>(
                    header_without_constants,
                    0,
                    description,
                    max_merged_block_size,
                    limit,
                    quiet,
                    have_all_inputs);

            processors.emplace_back(external_merging_sorted);
        }

        stage = Stage::Serialize;
        sum_bytes_in_blocks = 0;
        sum_rows_in_blocks = 0;
    }
}

void MergeSortingTransform::serialize()
{
    current_chunk = merge_sorter->read();
    if (!current_chunk)
        merge_sorter.reset();
}

void MergeSortingTransform::generate()
{
    if (!generated_prefix)
    {
        if (temporary_files.empty())
            merge_sorter = std::make_unique<MergeSorter>(std::move(chunks), description, max_merged_block_size, limit);
        else
        {
            ProfileEvents::increment(ProfileEvents::ExternalSortMerge);
            LOG_INFO(log, "There are " << temporary_files.size() << " temporary sorted parts to merge.");

            if (!chunks.empty())
                processors.emplace_back(std::make_shared<MergeSorterSource>(
                        header_without_constants, std::move(chunks), description, max_merged_block_size, limit));
        }

        generated_prefix = true;
    }

    if (merge_sorter)
    {
        generated_chunk = merge_sorter->read();
        if (!generated_chunk)
            merge_sorter.reset();
        else
            enrichChunkWithConstants(generated_chunk);
    }
}

void MergeSortingTransform::remerge()
{
    LOG_DEBUG(log, "Re-merging intermediate ORDER BY data (" << chunks.size()
                    << " blocks with " << sum_rows_in_blocks << " rows) to save memory consumption");

    /// NOTE Maybe concat all blocks and partial sort will be faster than merge?
    MergeSorter remerge_sorter(std::move(chunks), description, max_merged_block_size, limit);

    Chunks new_chunks;
    size_t new_sum_rows_in_blocks = 0;
    size_t new_sum_bytes_in_blocks = 0;

    while (auto chunk = remerge_sorter.read())
    {
        new_sum_rows_in_blocks += chunk.getNumRows();
        new_sum_bytes_in_blocks += chunk.allocatedBytes();
        new_chunks.emplace_back(std::move(chunk));
    }

    LOG_DEBUG(log, "Memory usage is lowered from "
            << formatReadableSizeWithBinarySuffix(sum_bytes_in_blocks) << " to "
            << formatReadableSizeWithBinarySuffix(new_sum_bytes_in_blocks));

    /// If the memory consumption was not lowered enough - we will not perform remerge anymore. 2 is a guess.
    if (new_sum_bytes_in_blocks * 2 > sum_bytes_in_blocks)
        remerge_is_useful = false;

    chunks = std::move(new_chunks);
    sum_rows_in_blocks = new_sum_rows_in_blocks;
    sum_bytes_in_blocks = new_sum_bytes_in_blocks;
}


void MergeSortingTransform::removeConstColumns(Chunk & chunk)
{
    size_t num_columns = chunk.getNumColumns();
    size_t num_rows = chunk.getNumRows();

    if (num_columns != const_columns_to_remove.size())
        throw Exception("Block has different number of columns with header: " + toString(num_columns)
                        + " vs " + toString(const_columns_to_remove.size()), ErrorCodes::LOGICAL_ERROR);

    auto columns = chunk.detachColumns();
    Columns column_without_constants;
    column_without_constants.reserve(header_without_constants.columns());

    for (size_t position = 0; position < num_columns; ++position)
    {
        if (!const_columns_to_remove[position])
            column_without_constants.push_back(std::move(columns[position]));
    }

    chunk.setColumns(std::move(column_without_constants), num_rows);
}

void MergeSortingTransform::enrichChunkWithConstants(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_result_columns = const_columns_to_remove.size();

    auto columns = chunk.detachColumns();
    Columns column_with_constants;
    column_with_constants.reserve(num_result_columns);

    auto & header = inputs.front().getHeader();

    size_t next_non_const_column = 0;
    for (size_t i = 0; i < num_result_columns; ++i)
    {
        if (const_columns_to_remove[i])
            column_with_constants.emplace_back(header.getByPosition(i).column->cloneResized(num_rows));
        else
        {
            if (next_non_const_column >= columns.size())
                throw Exception("Can't enrich chunk with constants because run out of non-constant columns.",
                        ErrorCodes::LOGICAL_ERROR);

            column_with_constants.emplace_back(std::move(columns[next_non_const_column]));
            ++next_non_const_column;
        }
    }

    chunk.setColumns(std::move(column_with_constants), num_rows);
}

}
