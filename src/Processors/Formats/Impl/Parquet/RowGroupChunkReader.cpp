#include "RowGroupChunkReader.h"
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Columns/FilterDescription.h>
#include <IO/SharedThreadPools.h>
#include <Common/threadPoolCallbackRunner.h>
#include <arrow/io/util_internal.h>

namespace DB
{

Chunk RowGroupChunkReader::readChunk(size_t rows)
{
    if (!remain_rows)
        return {};
    rows = std::min(rows, remain_rows);
    MutableColumns columns;
    for (auto & reader : column_readers)
    {
        columns.push_back(reader->createColumn());
    }
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

        OptionalRowSet row_set = std::nullopt;
        if (!filter_columns.empty())
            row_set = std::optional(RowSet(rows_to_read));
        if (row_set.has_value())
            for (auto & column : filter_columns)
            {
                reader_columns_mapping[column]->computeRowSet(row_set, rows_to_read);
                if (row_set.value().none())
                    break;
            }
        bool skip_all = false;
        if (row_set.has_value())
            skip_all = row_set.value().none();
        if (skip_all)
        {
            metrics.skipped_rows += rows_to_read;
        }

        bool all = true;
        if (row_set.has_value())
            all = row_set.value().all();
        if (all)
            row_set = std::nullopt;
        if (!skip_all)
            for (auto & column : columns)
            {
                if (all)
                    column->reserve(rows);
                else
                    column->reserve(row_set.value().count());
            }
        for (size_t i = 0; i < column_readers.size(); i++)
        {
            if (skip_all)
                column_readers[i]->skip(rows_to_read);
            else
                column_readers[i]->read(columns[i], row_set, rows_to_read);
        }
        remain_rows -= rows_to_read;
        metrics.filtered_rows += (rows_to_read - (columns[0]->size() - rows_read));
        rows_read = columns[0]->size();
    }

    //    if (parquet_reader->remain_filter.has_value())
    //    {
    //        std::cerr<<"has filter\n"<<std::endl;
    //        auto input = parquet_reader->header.cloneWithColumns(std::move(columns));
    //        auto output = input.getColumns();
    //        parquet_reader->remain_filter.value().execute(input);
    //        const auto& filter = checkAndGetColumn<ColumnUInt8>(*input.getByPosition(0).column).getData();
    //        size_t resize_hint = 0;
    //        for (size_t i = 0; i < columns.size(); i++)
    //        {
    //            output[i] = output[i]->assumeMutable()->filter(filter, resize_hint);
    //            resize_hint = output[i]->size();
    //        }
    //        return Chunk(std::move(output), resize_hint);
    //    }
    metrics.output_rows += rows_read;
    return Chunk(std::move(columns), rows_read);
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


RowGroupPrefetch::RowGroupPrefetch(SeekableReadBuffer & file_, std::mutex & mutex, const parquet::ArrowReaderProperties& arrow_properties_) : file(file_), file_mutex(mutex), arrow_properties(arrow_properties_)
{
    callback_runner = threadPoolCallbackRunnerUnsafe<ColumnChunkData>(getIOThreadPool().get(), "ParquetRead");
}
void RowGroupPrefetch::prefetchRange(const arrow::io::ReadRange& range)
{
    if (fetched)
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "RowGroupPrefetch: prefetchColumnChunk called after startPrefetch");
    ranges.emplace_back(range);
}
void RowGroupPrefetch::startPrefetch()
{
    if (fetched) return;
    fetched = true;
    ranges = arrow::io::internal::CoalesceReadRanges(ranges, arrow_properties.cache_options().hole_size_limit, arrow_properties.cache_options().range_size_limit);
    read_range_buffers.resize(ranges.size());
    for (size_t i=0; i < ranges.size(); i++)
    {
        auto& range = ranges[i];
        read_range_buffers[i].range = range;
        auto task = [this, range, i]() -> ColumnChunkData {
            auto& buffer = read_range_buffers[i].buffer;

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
        tasks.begin(), tasks.end(), range,
        [](const TaskEntry& entry, const arrow::io::ReadRange& range_) {
            return entry.range.offset + entry.range.length < range_.offset + range_.length;
        });
    if (it != tasks.end() && it->range.Contains(range)) {
        it->task.wait();
    } else {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Range was not requested for caching: offset={}, length={}", range.offset, range.length);
    }

    const auto buffer_it = std::lower_bound(
        read_range_buffers.begin(), read_range_buffers.end(), range,
        [](const ReadRangeBuffer& buffer, const arrow::io::ReadRange& range_) {
            return buffer.range.offset + buffer.range.length < range_.offset + range_.length;
        });
    if (buffer_it != read_range_buffers.end() && buffer_it->range.Contains(range)) {
        return {reinterpret_cast<uint8_t *>(buffer_it->buffer.data() + (range.offset - buffer_it->range.offset)), static_cast<size_t>(range.length)};
    } else {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Range was not requested for caching: offset={}, length={}", range.offset, range.length);
    }
}


RowGroupChunkReader::RowGroupChunkReader(
    ParquetReader * parquetReader, size_t row_group_idx, RowGroupPrefetchPtr prefetch_, std::unordered_map<String, ColumnFilterPtr> filters)
    : parquet_reader(parquetReader), row_group_meta(parquetReader->meta_data->RowGroup(static_cast<int>(row_group_idx))), prefetch(std::move(prefetch_))
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
        //        auto range = getColumnRange(*row_group_meta->ColumnChunk(idx));
        //        size_t compress_size = range.second;
        //        size_t offset = range.first;
        //        column_buffers[reader_idx].resize(compress_size, 0);
        remain_rows = row_group_meta->ColumnChunk(idx)->num_values();
        //        if (!parquet_reader->file.supportsReadAt())
        //        {
        //            std::lock_guard lock(parquet_reader->file_mutex);
        //            parquet_reader->file.seek(offset, SEEK_SET);
        //            size_t count = parquet_reader->file.readBig(reinterpret_cast<char *>(column_buffers[reader_idx].data()), compress_size);
        //            if (count != compress_size)
        //                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Failed to read column data");
        //        }
        //        else
        //        {
        //            auto pb = [](size_t ) {return true;};
        //            size_t count = parquet_reader->file.readBigAt(reinterpret_cast<char *>(column_buffers[reader_idx].data()), compress_size, offset, pb);
        //            if (count != compress_size)
        //                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Failed to read column data");
        //        }
        auto data = prefetch->readRange(getColumnRange(*row_group_meta->ColumnChunk(idx)));
        auto page_reader = std::make_unique<LazyPageReader>(
            std::make_shared<ReadBufferFromMemory>(
                reinterpret_cast<char *>(data.data), data.size),
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
        //            auto column_reader = SelectiveColumnReaderFactory::createLeafColumnReader(
        //            *row_group_meta->ColumnChunk(idx), parquet_reader->meta_data->schema()->Column(idx), std::move(page_reader), filter);
        column_readers.push_back(column_reader);
        reader_columns_mapping[col_with_name.name] = column_reader;
        chassert(idx >= 0);
        if (filter)
            filter_columns.push_back(col_with_name.name);
    }
}

static std::shared_ptr<FilterDescription> mergeFilterDescriptions(std::unordered_map<size_t, std::shared_ptr<FilterDescription>>& /*intermediate_filter_descriptions*/)
{
//    if (intermediate_filter_descriptions.empty()) return nullptr;
//    if (intermediate_filter_descriptions.size() == 1)
//        return intermediate_filter_descriptions.begin()->second;
//    auto first_column = intermediate_filter_descriptions.begin()->second->data_holder;
//    MutableColumnPtr new_filter_column = first_column->cloneEmpty();
//    auto size = first_column->size();
//    for (auto & [idx, filter_description] : intermediate_filter_descriptions)
//    {
//        for (size_t i = 0; i < size; ++i)
//        {
//
//        }
//    }
    return nullptr;
}

static void combineRowSetAndFilter(RowSet& set, std::shared_ptr<FilterDescription> filter_description)
{
    const auto &filter_data = *filter_description->data;
    if (filter_data.size() != set.totalRows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "bug, filter data size is not equal to row set size");
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
        return SelectResult{std::nullopt, {}, nullptr};

    bool skip_all = false;

    // apply fast filters
    for (auto & idx : fast_filter_column_idxs)
    {
        readers[idx]->computeRowSet(total_set, rows);
        if (total_set.value().none())
        {
            skip_all = true;
            break;
        }
    }

    size_t count = 0;
    if (!skip_all)
        count = total_set.has_value() ? total_set.value().count() : rows;

    // apply actions filter
    std::unordered_map<size_t, ColumnPtr> intermediate_columns;
    std::unordered_map<size_t, ColumnPtr> intermediate_filter_columns;
    std::unordered_map<size_t, std::shared_ptr<FilterDescription>> intermediate_filter_descriptions;
    for (auto & [idx, filter] : actions_filters)
    {
        if (!count) break;
        auto reader = readers[idx];
        auto column = reader->createColumn();
        column->reserve(count);
        reader->read(column, total_set, rows);
        intermediate_columns[idx] = std::move(column);
        auto filter_column = filter->testByExpression(intermediate_columns[idx]);
        filter_column = filter_column->convertToFullColumnIfSparse();
        intermediate_filter_columns[idx] = filter_column;
        intermediate_filter_descriptions[idx] = std::make_shared<FilterDescription>(*filter_column);
        size_t filter_count = intermediate_filter_descriptions[idx]->countBytesInFilter();
        if (!filter_count)
        {
            skip_all = true;
            break;
        }
    }

    if (skip_all)
        return SelectResult{std::nullopt, std::move(intermediate_columns), nullptr, true};
    else
    {
        auto filter_description = mergeFilterDescriptions(intermediate_filter_descriptions);
        if (filter_description)
            combineRowSetAndFilter(total_set.value(), filter_description);
        return SelectResult{std::move(total_set), std::move(intermediate_columns), filter_description};
    }
}
SelectConditions::SelectConditions(
    std::unordered_map<size_t, SelectiveColumnReaderPtr> & readers_,
    std::vector<size_t> & fastFilterColumnIdxs,
    std::unordered_map<size_t, ColumnFilterPtr> & actionsFilters)
    : readers(readers_), fast_filter_column_idxs(fastFilterColumnIdxs), actions_filters(actionsFilters)
{
    has_filter = fastFilterColumnIdxs.empty() && actionsFilters.empty();
}
}
