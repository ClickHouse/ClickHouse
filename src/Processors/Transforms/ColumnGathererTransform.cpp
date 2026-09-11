#include <Processors/Transforms/ColumnGathererTransform.h>

#include <Core/Block.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSparse.h>
#include <IO/WriteHelpers.h>
#include <Processors/Port.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event GatheringColumnMilliseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int EMPTY_DATA_PASSED;
    extern const int RECEIVED_EMPTY_DATA;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

void ColumnGathererStream::Source::update(Columns columns_)
{
    columns = std::move(columns_);
    if (columns.empty())
    {
        column = nullptr;
        size = 0;
        pos = 0;
        return;
    }

    size = columns.front()->size();
    for (size_t i = 1; i < columns.size(); ++i)
    {
        if (columns[i]->size() != size)
            throw Exception(
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "Sizes of gathered columns doesn't match: first {}, {}: {}",
                size,
                i,
                columns[i]->size());
    }

    column = columns.front();
    pos = 0;
}

ColumnGathererStream::ColumnGathererStream(
    size_t num_inputs,
    ReadBuffer & row_sources_buf_,
    size_t block_preferred_size_rows_,
    size_t block_preferred_size_bytes_,
    std::optional<size_t> max_dynamic_subcolumns_,
    std::vector<UInt8> is_result_sparse_)
    : sources(num_inputs)
    , row_sources_buf(row_sources_buf_)
    , block_preferred_size_rows(block_preferred_size_rows_)
    , block_preferred_size_bytes(block_preferred_size_bytes_)
    , max_dynamic_subcolumns(max_dynamic_subcolumns_)
    , is_result_sparse(std::move(is_result_sparse_))
{
    if (num_inputs == 0)
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "There are no streams to gather");
    if (is_result_sparse.empty())
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "ColumnGathererStream requires at least one result column");
}

void ColumnGathererStream::updateStats(size_t rows, size_t bytes)
{
    merged_rows += rows;
    merged_bytes += bytes;
    ++merged_blocks;
}

void ColumnGathererStream::applySparsePolicy(Columns & columns) const
{
    if (columns.size() != is_result_sparse.size())
        throw Exception(
            ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "Gathered chunk has {} columns, expected {}",
            columns.size(),
            is_result_sparse.size());

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!is_result_sparse[i])
            columns[i] = removeSpecialRepresentations(columns[i]);
    }
}

size_t ColumnGathererStream::resultRows() const
{
    return result_columns.empty() ? 0 : result_columns.front()->size();
}

size_t ColumnGathererStream::resultBytes() const
{
    size_t bytes = 0;
    for (const auto & column : result_columns)
        bytes += column->byteSize();
    return bytes;
}

bool ColumnGathererStream::resultIsEmpty() const
{
    return result_columns.empty() || result_columns.front()->empty();
}

void ColumnGathererStream::initialize(Inputs inputs)
{
    const size_t num_result_columns = is_result_sparse.size();
    std::vector<VectorWithMemoryTracking<ColumnPtr>> source_columns_per_output(num_result_columns);

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (!inputs[i].chunk)
            continue;

        auto columns = inputs[i].chunk.detachColumns();
        applySparsePolicy(columns);
        sources[i].update(std::move(columns));

        for (size_t j = 0; j < num_result_columns; ++j)
            source_columns_per_output[j].push_back(sources[i].columns[j]);
    }

    if (source_columns_per_output.front().empty())
        return;

    result_columns.resize(num_result_columns);
    for (size_t j = 0; j < num_result_columns; ++j)
    {
        result_columns[j] = source_columns_per_output[j][0]->cloneEmpty();
        if (is_result_sparse[j] && !result_columns[j]->isSparse())
            result_columns[j] = ColumnSparse::create(std::move(result_columns[j]));

        if (result_columns[j]->hasDynamicStructure())
            result_columns[j]->chooseDynamicStructureForMerge(source_columns_per_output[j], max_dynamic_subcolumns);
        if (result_columns[j]->hasStatistics())
            result_columns[j]->takeOrCalculateStatisticsFrom(source_columns_per_output[j]);
    }
}

Chunk ColumnGathererStream::emitFullyCopiedSource()
{
    Chunk res;
    size_t rows = source_to_fully_copy->size;
    size_t bytes = 0;

    for (size_t i = 0; i < result_columns.size(); ++i)
    {
        auto & src_col = source_to_fully_copy->columns[i];
        bytes += src_col->allocatedBytes();

        /// For columns with dynamic structure we cannot just take the source column because the resulting
        /// column may have different dynamic structure (after calling `chooseDynamicStructureForMerge`).
        /// We need to use `cloneEmpty` + `insertRangeFrom` to properly re-insert data.
        if (result_columns[i]->hasDynamicStructure())
        {
            auto col = result_columns[i]->cloneEmpty();
            col->insertRangeFrom(*src_col, 0, src_col->size());
            res.addColumn(std::move(col));
        }
        /// For columns with statistics only, we can reuse the source column but need to preserve merged statistics.
        else if (result_columns[i]->hasStatistics())
        {
            auto col = IColumn::mutate(std::move(src_col));
            col->takeOrCalculateStatisticsFrom({result_columns[i]->getPtr()});
            res.addColumn(std::move(col));
            if (i == 0)
                source_to_fully_copy->column = nullptr;
        }
        else
        {
            res.addColumn(src_col);
        }
    }

    updateStats(rows, bytes);
    source_to_fully_copy->pos = source_to_fully_copy->size;
    source_to_fully_copy = nullptr;
    return res;
}

Chunk ColumnGathererStream::emitAndResetResultColumns()
{
    const size_t rows = resultRows();
    for (size_t i = 1; i < result_columns.size(); ++i)
    {
        if (result_columns[i]->size() != rows)
            throw Exception(
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "Sizes of gathered result columns doesn't match: first {}, column {}: {}",
                rows,
                i,
                result_columns[i]->size());
    }

    Chunk res;
    size_t bytes = 0;
    for (auto & column : result_columns)
    {
        bytes += column->allocatedBytes();
        auto return_column = column->cloneEmpty();
        column.swap(return_column);
        res.addColumn(std::move(return_column));
    }

    updateStats(rows, bytes);
    return res;
}

void ColumnGathererStream::gatherAllColumns()
{
    row_sources_buf.nextIfAtEnd();
    RowSourcePart * row_source_pos = reinterpret_cast<RowSourcePart *>(row_sources_buf.position());
    RowSourcePart * row_sources_end = reinterpret_cast<RowSourcePart *>(row_sources_buf.buffer().end());

    if (next_required_source == -1)
    {
        size_t size_to_reserve = std::min(static_cast<size_t>(row_sources_end - row_source_pos), block_preferred_size_rows);
        for (auto & column : result_columns)
            column->reserve(size_to_reserve);
    }

    next_required_source = -1;

    do
    {
        if (row_source_pos >= row_sources_end)
            break;

        RowSourcePart row_source = *row_source_pos;
        size_t source_num = row_source.getSourceNum();
        Source & source = sources[source_num];
        bool source_skip = row_source.getSkipFlag();

        if (source.pos >= source.size)
        {
            next_required_source = source_num;
            return;
        }

        ++row_source_pos;

        size_t len = 1;
        size_t max_len = std::min(static_cast<size_t>(row_sources_end - row_source_pos), source.size - source.pos);

        while (len < max_len && row_source_pos->data == row_source.data)
        {
            ++len;
            ++row_source_pos;
        }

        row_sources_buf.position() = reinterpret_cast<char *>(row_source_pos);

        if (!source_skip)
        {
            if (source.pos == 0 && source.size == len)
            {
                source_to_fully_copy = &source;
                return;
            }

            for (size_t i = 0; i < result_columns.size(); ++i)
            {
                if (len == 1)
                    result_columns[i]->insertFrom(*source.columns[i], source.pos);
                else
                    result_columns[i]->insertRangeFrom(*source.columns[i], source.pos, len);
            }
        }

        source.pos += len;
    } while (resultRows() < block_preferred_size_rows && resultBytes() < block_preferred_size_bytes);
}

IMergingAlgorithm::Status ColumnGathererStream::merge()
{
    /// Nothing to read after initialize.
    if (result_columns.empty())
        return Status(Chunk(), true);

    if (source_to_fully_copy) /// Was set on a previous iteration
        return Status(emitFullyCopiedSource());

    /// Special case: single source and there are no skipped rows
    /// Note: looks like this should never happen because row_sources_buf cannot just skip row info.
    if (sources.size() == 1 && row_sources_buf.eof())
    {
        if (sources.front().pos < sources.front().size)
        {
            next_required_source = 0;
            Chunk res;
            size_t bytes = 0;
            for (auto & column : sources.front().columns)
            {
                bytes += column->allocatedBytes();
                res.addColumn(std::move(column));
            }
            updateStats(sources.front().size, bytes);
            sources.front().column = nullptr;
            sources.front().pos = sources.front().size = 0;
            return Status(std::move(res));
        }

        if (next_required_source == -1)
            return Status(Chunk(), true);

        next_required_source = 0;
        return Status(next_required_source);
    }

    if (next_required_source != -1 && sources[next_required_source].size == 0)
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Cannot fetch required block. Source {}", toString(next_required_source));

    /// Single-column keeps the `IColumn::gather` path. Multiple columns walk `rows_sources` once.
    if (result_columns.size() == 1)
        result_columns[0]->gather(*this);
    else
        gatherAllColumns();

    if (next_required_source != -1)
        return Status(next_required_source);

    if (source_to_fully_copy && resultIsEmpty())
        return Status(emitFullyCopiedSource());

    return Status(emitAndResetResultColumns(), row_sources_buf.eof() && !source_to_fully_copy);
}


void ColumnGathererStream::consume(Input & input, size_t source_num)
{
    auto & source = sources[source_num];
    if (input.chunk)
    {
        auto columns = input.chunk.detachColumns();
        applySparsePolicy(columns);
        source.update(std::move(columns));
    }

    if (0 == source.size)
    {
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Fetched block is empty. Source {}", source_num);
    }
}

ColumnGathererTransform::ColumnGathererTransform(
    SharedHeader header,
    size_t num_inputs,
    std::unique_ptr<ReadBuffer> row_sources_buf_,
    size_t block_preferred_size_rows_,
    size_t block_preferred_size_bytes_,
    std::optional<size_t> max_dynamic_subcolumns_,
    std::vector<UInt8> is_result_sparse_)
    : IMergingTransform<ColumnGathererStream>(
        num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
        num_inputs, *row_sources_buf_, block_preferred_size_rows_, block_preferred_size_bytes_, max_dynamic_subcolumns_, is_result_sparse_)
    , row_sources_buf_holder(std::move(row_sources_buf_))
    , log(getLogger("ColumnGathererStream"))
{
    if (header->columns() != is_result_sparse_.size())
        throw Exception(
            ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "Header has {} columns, but sparse flags were provided for {}",
            header->columns(),
            is_result_sparse_.size());
}

void ColumnGathererTransform::onFinish()
{
    const auto names = getOutputPort().getHeader().getNames();
    const auto message = names.size() == 1
        ? fmt::format("Gathered column {}", names.front())
        : fmt::format("Gathered columns {}", fmt::join(names, ", "));
    logMergedStats(ProfileEvents::GatheringColumnMilliseconds, message, log);
}

}
