#include "RowGroupChunkReader.h"
#include <Columns/FilterDescription.h>
#include <IO/SharedThreadPools.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <arrow/io/util_internal.h>
#include <Common/threadPoolCallbackRunner.h>

namespace DB
{

Chunk RowGroupChunkReader::readChunk(size_t rows)
{
    if (!remain_rows)
        return {};
    rows = std::min(rows, remain_rows);
    MutableColumns columns;
    size_t rows_read = 0;
    while (!rows_read)
    {
        size_t rows_to_read = std::min(rows - rows_read, remain_rows);
        if (!rows_to_read)
            break;
        for (auto & reader : column_readers)
        {
            if (!reader->availableRows())
            {
                reader->readPageIfNeeded();
            }
            rows_to_read = std::min(reader->availableRows(), rows_to_read);
        }
        if (!rows_to_read)
            break;

        auto select_result = selectConditions->selectRows(rows_to_read);

        if (select_result.skip_all)
        {
            metrics.skipped_rows += rows_to_read;
        }

        bool all = select_result.valid_count == rows_to_read;
        if (all)
            select_result.set = std::nullopt;
        auto column_names = parquet_reader->header.getNames();
        if (select_result.skip_all)
        {
            metrics.filtered_rows += rows_to_read;
            rows_read = 0;
            for (const auto & name : column_names)
            {
                if (!select_result.intermediate_columns.contains(name))
                {
                    reader_columns_mapping.at(name)->skip(rows_to_read);
                }
            }
        }
        else
        {
            for (const auto & name : column_names)
            {
                if (select_result.intermediate_columns.contains(name))
                {
                    if (all)
                        columns.emplace_back(select_result.intermediate_columns.at(name)->assumeMutable());
                    else
                        columns.emplace_back(
                            select_result.intermediate_columns.at(name)->filter(select_result.intermediate_filter, select_result.valid_count)->assumeMutable());
                }
                else
                {
                    auto & reader = reader_columns_mapping.at(name);
                    auto column = reader->createColumn();
                    column->reserve(select_result.valid_count);
                    reader->read(column, select_result.set, rows_to_read);
                    columns.emplace_back(std::move(column));
                }
            }
            metrics.filtered_rows += (rows_to_read - (columns[0]->size() - rows_read));
            rows_read = columns[0]->size();
        }
        remain_rows -= rows_to_read;
    }

    metrics.output_rows += rows_read;
    if (rows_read)
        return Chunk(std::move(columns), rows_read);
    else
        return {};
}

arrow::io::ReadRange getColumnRange(const parquet::ColumnChunkMetaData & column_metadata)
{
    int64_t col_start = column_metadata.data_page_offset();
    if (column_metadata.has_dictionary_page() && column_metadata.dictionary_page_offset() > 0
        && col_start > column_metadata.dictionary_page_offset())
    {
        col_start = column_metadata.dictionary_page_offset();
    }
    int64_t len = column_metadata.total_compressed_size();
    return {col_start, len};
}


RowGroupPrefetch::RowGroupPrefetch(SeekableReadBuffer & file_, std::mutex & mutex, const parquet::ArrowReaderProperties & arrow_properties_)
    : file(file_), file_mutex(mutex), arrow_properties(arrow_properties_)
{
    callback_runner = threadPoolCallbackRunnerUnsafe<ColumnChunkData>(getIOThreadPool().get(), "ParquetRead");
}
void RowGroupPrefetch::prefetchRange(const arrow::io::ReadRange & range)
{
    if (fetched)
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "RowGroupPrefetch: prefetchColumnChunk called after startPrefetch");
    ranges.emplace_back(range);
}
void RowGroupPrefetch::startPrefetch()
{
    if (fetched)
        return;
    fetched = true;
    ranges = arrow::io::internal::CoalesceReadRanges(
        ranges, arrow_properties.cache_options().hole_size_limit, arrow_properties.cache_options().range_size_limit);
    read_range_buffers.resize(ranges.size());
    for (size_t i = 0; i < ranges.size(); i++)
    {
        auto & range = ranges[i];
        read_range_buffers[i].range = range;
        auto task = [this, range, i]() -> ColumnChunkData
        {
            auto & buffer = read_range_buffers[i].buffer;

            buffer.resize(range.length);
            int64_t count = 0;
            if (file.supportsReadAt())
            {
                auto pb = [](size_t) { return true; };
                count = file.readBigAt(reinterpret_cast<char *>(buffer.data()), range.length, range.offset, pb);
            }
            else
            {
                std::lock_guard lock(file_mutex);
                file.seek(range.offset, SEEK_SET);
                count = file.readBig(reinterpret_cast<char *>(buffer.data()), range.length);
            }
            if (count != range.length)
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Failed to read column data");
            return {reinterpret_cast<uint8_t *>(buffer.data()), buffer.size()};
        };
        // Assuming Priority is an enum or a type that is defined elsewhere
        Priority priority = {0}; // Set the appropriate priority value
        auto future = callback_runner(std::move(task), priority);
        tasks.emplace_back(TaskEntry{range, std::move(future)});
    }
}
ColumnChunkData RowGroupPrefetch::readRange(const arrow::io::ReadRange & range)
{
    if (!fetched)
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "RowGroupPrefetch: readRange called before startPrefetch");

    // wait fetch finished
    const auto it = std::lower_bound(
        tasks.begin(),
        tasks.end(),
        range,
        [](const TaskEntry & entry, const arrow::io::ReadRange & range_)
        { return entry.range.offset + entry.range.length < range_.offset + range_.length; });
    if (it != tasks.end() && it->range.Contains(range))
    {
        it->task.wait();
    }
    else
    {
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Range was not requested for caching: offset={}, length={}", range.offset, range.length);
    }

    const auto buffer_it = std::lower_bound(
        read_range_buffers.begin(),
        read_range_buffers.end(),
        range,
        [](const ReadRangeBuffer & buffer, const arrow::io::ReadRange & range_)
        { return buffer.range.offset + buffer.range.length < range_.offset + range_.length; });
    if (buffer_it != read_range_buffers.end() && buffer_it->range.Contains(range))
    {
        return {
            reinterpret_cast<uint8_t *>(buffer_it->buffer.data() + (range.offset - buffer_it->range.offset)),
            static_cast<size_t>(range.length)};
    }
    else
    {
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Range was not requested for caching: offset={}, length={}", range.offset, range.length);
    }
}


RowGroupChunkReader::RowGroupChunkReader(
    ParquetReader * parquetReader, size_t row_group_idx, RowGroupPrefetchPtr prefetch_, std::unordered_map<String, ColumnFilterPtr> filters)
    : parquet_reader(parquetReader)
    , row_group_meta(parquetReader->meta_data->RowGroup(static_cast<int>(row_group_idx)))
    , prefetch(std::move(prefetch_))
{
    column_readers.reserve(parquet_reader->header.columns());
    column_buffers.resize(parquet_reader->header.columns());
    for (const auto & col_with_name : parquet_reader->header)
    {
        if (!parquetReader->parquet_columns.contains(col_with_name.name))
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", col_with_name.name);

        const auto & node = parquetReader->parquet_columns.at(col_with_name.name);
        if (!node->is_primitive())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "arrays and maps are not implemented in native parquet reader");

        auto idx = parquet_reader->meta_data->schema()->ColumnIndex(*node);
        auto filter = filters.contains(col_with_name.name) ? filters.at(col_with_name.name) : nullptr;
        remain_rows = row_group_meta->ColumnChunk(idx)->num_values();
        auto data = prefetch->readRange(getColumnRange(*row_group_meta->ColumnChunk(idx)));
        auto page_reader = std::make_unique<LazyPageReader>(
            std::make_shared<ReadBufferFromMemory>(reinterpret_cast<char *>(data.data), data.size),
            parquet_reader->properties,
            remain_rows,
            row_group_meta->ColumnChunk(idx)->compression());
        const auto * column_desc = parquet_reader->meta_data->schema()->Column(idx);
        auto column_reader = ParquetColumnReaderFactory::builder()
                                 .nullable(node->is_optional())
                                 .dictionary(row_group_meta->ColumnChunk(idx)->has_dictionary_page())
                                 .columnDescriptor(column_desc)
                                 .pageReader(std::move(page_reader))
                                 .targetType(col_with_name.type)
                                 .filter(filter)
                                 .build();
        column_readers.push_back(column_reader);
        reader_columns_mapping[col_with_name.name] = column_reader;
        chassert(idx >= 0);
        if (filter)
            filter_columns.push_back(col_with_name.name);
    }
    selectConditions = std::make_unique<SelectConditions>(reader_columns_mapping, filter_columns, parquet_reader->expression_filters, parquet_reader->header);
}

static IColumn::Filter mergeFilters(std::vector<IColumn::Filter> & filters)
{
    assert(!filters.empty());
    if (filters.size() == 1)
        return std::move(filters[0]);
    IColumn::Filter result;
    size_t size = filters.front().size();
    result.resize_fill(size, 1);
    for (size_t i = 0; i < filters.size(); i++)
    {
        auto & current = filters[i];
        for (size_t j = 0; j < size; j++)
        {
            if (!result[i])
                continue;
            if (!current[i])
                result[i] = 0;
        }
    }
    return result;
}

static void combineRowSetAndFilter(RowSet & set, const IColumn::Filter& filter_data)
{
    int count = 0;
    for (size_t i = 0; i < set.totalRows(); ++i)
    {
        if (!set.get(i))
            continue;
        if (!filter_data[count])
            set.set(i, false);
        count++;
    }
}

SelectResult SelectConditions::selectRows(size_t rows)
{
    OptionalRowSet total_set;
    if (has_filter)
        total_set = std::optional(RowSet(rows));
    else
        return SelectResult{std::nullopt, {}, {}, rows, false};

    bool skip_all = false;

    // apply fast filters
    for (const auto & name : fast_filter_columns)
    {
        readers.at(name)->computeRowSet(total_set, rows);
        if (total_set.value().none())
        {
            skip_all = true;
            break;
        }
    }

    size_t count = 0;

    // apply actions filter
    std::unordered_map<String, ColumnPtr> intermediate_columns;
    std::vector<IColumn::Filter> intermediate_filters;
    for (const auto & expr_filter : expression_filters)
    {
        if (skip_all)
            break;
        count = total_set.has_value() ? total_set.value().count() : rows;
        // prepare condition columns
        ColumnsWithTypeAndName input;
        for (const auto &name : expr_filter->getInputs())
        {
            if (!intermediate_columns.contains(name))
            {
                auto reader = readers.at(name);
                auto column = reader->createColumn();
                if (count == rows)
                    reader->read(column, std::nullopt, rows);
                else
                    reader->read(column, total_set, rows);
                intermediate_columns.emplace(name, std::move(column));
            }
            input.emplace_back(intermediate_columns.at(name), header.getByName(name).type, name);
        }

        auto filter = expr_filter->execute(input);
        size_t filter_count = countBytesInFilter(filter);
        intermediate_filters.emplace_back(std::move(filter));
        if (!filter_count)
        {
            skip_all = true;
            break;
        }
    }

    if (skip_all)
        return SelectResult{std::nullopt, std::move(intermediate_columns), {}, 0, true};
    else
    {
        auto total_count = total_set.value().count();
        if (!intermediate_filters.empty())
        {
            auto filter = mergeFilters(intermediate_filters);
            combineRowSetAndFilter(total_set.value(), filter);
            return SelectResult{std::move(total_set), std::move(intermediate_columns), std::move(filter), total_count, false};
        }
        return SelectResult{std::move(total_set), {}, {}, total_count, false};
    }
}
SelectConditions::SelectConditions(
    std::unordered_map<String, SelectiveColumnReaderPtr> & readers_,
    std::vector<String> & fast_filter_columns_,
    std::vector<std::shared_ptr<ExpressionFilter>> & expression_filters_,
    const Block & header_)
    : readers(readers_), fast_filter_columns(fast_filter_columns_), expression_filters(expression_filters_), header(header_)
{    has_filter = !fast_filter_columns.empty() || !expression_filters.empty();
}

}
