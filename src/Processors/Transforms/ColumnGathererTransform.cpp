#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/formatReadable.h>
#include <Columns/ColumnSparse.h>
#include <IO/WriteHelpers.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int EMPTY_DATA_PASSED;
    extern const int RECEIVED_EMPTY_DATA;
}

ColumnGathererStream::ColumnGathererStream(
    size_t num_inputs,
    ReadBuffer & row_sources_buf_,
    size_t block_preferred_size_rows_,
    size_t block_preferred_size_bytes_,
    bool is_result_sparse_)
    : sources(num_inputs)
    , row_sources_buf(row_sources_buf_)
    , block_preferred_size_rows(block_preferred_size_rows_)
    , block_preferred_size_bytes(block_preferred_size_bytes_)
    , is_result_sparse(is_result_sparse_)
{
    if (num_inputs == 0)
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "There are no streams to gather");
}

void ColumnGathererStream::initialize(Inputs inputs)
{
    Columns source_columns;
    source_columns.reserve(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (!inputs[i].chunk)
            continue;

        if (!is_result_sparse)
            convertToFullIfSparse(inputs[i].chunk);

        sources[i].update(inputs[i].chunk.detachColumns().at(0));
        source_columns.push_back(sources[i].column);
    }

    if (source_columns.empty())
        return;

    result_column = source_columns[0]->cloneEmpty();
    if (is_result_sparse && !result_column->isSparse())
        result_column = ColumnSparse::create(std::move(result_column));

    if (result_column->hasDynamicStructure())
        result_column->takeDynamicStructureFromSourceColumns(source_columns);
}

IMergingAlgorithm::Status ColumnGathererStream::merge()
{
    /// Nothing to read after initialize.
    if (!result_column)
        return Status(Chunk(), true);

    if (source_to_fully_copy) /// Was set on a previous iteration
    {
        Chunk res;
        /// For columns with Dynamic structure we cannot just take column source_to_fully_copy because resulting column may have
        /// different Dynamic structure (and have some merge statistics after calling takeDynamicStructureFromSourceColumns).
        /// We should insert into data resulting column using insertRangeFrom.
        if (result_column->hasDynamicStructure())
        {
            auto col = result_column->cloneEmpty();
            col->insertRangeFrom(*source_to_fully_copy->column, 0, source_to_fully_copy->column->size());
            res.addColumn(std::move(col));
        }
        else
        {
            res.addColumn(source_to_fully_copy->column);
        }
        merged_rows += source_to_fully_copy->size;
        source_to_fully_copy->pos = source_to_fully_copy->size;
        source_to_fully_copy = nullptr;
        return Status(std::move(res));
    }

    /// Special case: single source and there are no skipped rows
    /// Note: looks like this should never happen because row_sources_buf cannot just skip row info.
    if (sources.size() == 1 && row_sources_buf.eof())
    {
        if (sources.front().pos < sources.front().size)
        {
            next_required_source = 0;
            Chunk res;
            merged_rows += sources.front().column->size();
            merged_bytes += sources.front().column->allocatedBytes();
            res.addColumn(std::move(sources.front().column));
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

    /// Surprisingly this call may directly change some internal state of ColumnGathererStream.
    /// output_column. See ColumnGathererStream::gather.
    result_column->gather(*this);

    if (next_required_source != -1)
        return Status(next_required_source);

    if (source_to_fully_copy && result_column->empty())
    {
        Chunk res;
        merged_rows += source_to_fully_copy->column->size();
        merged_bytes += source_to_fully_copy->column->allocatedBytes();
        if (result_column->hasDynamicStructure())
        {
            auto col = result_column->cloneEmpty();
            col->insertRangeFrom(*source_to_fully_copy->column, 0, source_to_fully_copy->column->size());
            res.addColumn(std::move(col));
        }
        else
        {
            res.addColumn(source_to_fully_copy->column);
        }
        source_to_fully_copy->pos = source_to_fully_copy->size;
        source_to_fully_copy = nullptr;
        return Status(std::move(res));
    }

    auto col = result_column->cloneEmpty();
    result_column.swap(col);

    Chunk res;
    merged_rows += col->size();
    merged_bytes += col->allocatedBytes();
    res.addColumn(std::move(col));
    return Status(std::move(res), row_sources_buf.eof() && !source_to_fully_copy);
}


void ColumnGathererStream::consume(Input & input, size_t source_num)
{
    auto & source = sources[source_num];
    if (input.chunk)
    {
        if (!is_result_sparse)
            convertToFullIfSparse(input.chunk);

        source.update(input.chunk.getColumns().at(0));
    }

    if (0 == source.size)
    {
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Fetched block is empty. Source {}", source_num);
    }
}

ColumnGathererTransform::ColumnGathererTransform(
    const Block & header,
    size_t num_inputs,
    ReadBuffer & row_sources_buf_,
    size_t block_preferred_size_rows_,
    size_t block_preferred_size_bytes_,
    bool is_result_sparse_)
    : IMergingTransform<ColumnGathererStream>(
        num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
        num_inputs, row_sources_buf_, block_preferred_size_rows_, block_preferred_size_bytes_, is_result_sparse_)
    , log(getLogger("ColumnGathererStream"))
{
    if (header.columns() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Header should have 1 column, but contains {}",
            toString(header.columns()));
}

void ColumnGathererTransform::work()
{
    Stopwatch stopwatch;
    IMergingTransform<ColumnGathererStream>::work();
    elapsed_ns += stopwatch.elapsedNanoseconds();
}

void ColumnGathererTransform::onFinish()
{
    auto merged_rows = algorithm.getMergedRows();
    auto merged_bytes = algorithm.getMergedRows();
    /// Don't print info for small parts (< 10M rows)
    if (merged_rows < 10000000)
        return;

    double seconds = static_cast<double>(elapsed_ns) / 1000000000ULL;
    const auto & column_name = getOutputPort().getHeader().getByPosition(0).name;

    if (seconds == 0.0)
        LOG_DEBUG(log, "Gathered column {} ({} bytes/elem.) in 0 sec.",
            column_name, static_cast<double>(merged_bytes) / merged_rows);
    else
        LOG_DEBUG(log, "Gathered column {} ({} bytes/elem.) in {} sec., {} rows/sec., {}/sec.",
            column_name, static_cast<double>(merged_bytes) / merged_rows, seconds,
            merged_rows / seconds, ReadableSize(merged_bytes / seconds));
}

}
