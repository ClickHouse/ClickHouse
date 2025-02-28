#include "RowGroupChunkReader.h"
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/castColumn.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilterHelper.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <arrow/io/util_internal.h>
#include <parquet/page_index.h>
#include <Common/Stopwatch.h>
#include <Common/threadPoolCallbackRunner.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int LOGICAL_ERROR;
}

namespace ProfileEvents
{
extern const Event ParquetFilteredRows;
extern const Event ParquetSkippedRows;
extern const Event ParquetOutputRows;
}

namespace DB
{
RowGroupChunkReader::RowGroupChunkReader(
    ParquetReader * parquetReader,
    size_t row_group_idx,
    RowGroupPrefetchPtr prefetch_conditions_,
    RowGroupPrefetchPtr prefetch_,
    std::unordered_map<String, ColumnFilterPtr> filters)
    : parquet_reader(parquetReader)
    , row_group_meta(parquetReader->meta_data->RowGroup(static_cast<int>(row_group_idx)))
    , prefetch_conditions(std::move(prefetch_conditions_))
    , prefetch(std::move(prefetch_))
{
    column_readers.reserve(parquet_reader->header.columns());
    column_buffers.resize(parquet_reader->header.columns());
    context.parquet_reader = parquetReader;
    context.row_group_meta = row_group_meta;
    context.prefetch = prefetch;
    context.prefetch_conditions = prefetch_conditions;
    context.filter_columns = filter_columns;
    remain_rows = row_group_meta->num_rows();
    builder = std::make_unique<ColumnReaderBuilder>(
        parquet_reader->header, context, parquet_reader->filters, parquet_reader->condition_columns);

    if (parquet_reader->hasFilter())
    {
        auto page_index_reader = parquet::PageIndexReader::Make(
            parquetReader->arrow_file.get(), parquet_reader->meta_data, parquet_reader->settings.reader_properties);
        if (page_index_reader)
        {
            Int32 rg_idx = static_cast<int>(row_group_idx);
            //TODO need handle group node
            //        std::vector<Int32> column_indices;
            //        for (const auto & item : parquet_reader->parquet_columns)
            //        {
            //            int column_idx = parquet_reader->metaData().schema()->ColumnIndex(*parquet_reader->parquet_columns.at(item.first));
            //            /// nested column node will get -1
            //            if (column_idx < 0) continue;
            //            column_indices.push_back(column_idx);
            //        }
            //        static const parquet::PageIndexSelection ALL_INDEX = {true, true};
            //        page_index_reader->WillNeed({rg_idx}, column_indices, ALL_INDEX);
            row_group_index_reader = page_index_reader->RowGroup(rg_idx);
            context.row_group_index_reader = row_group_index_reader;
        }
    }

    for (const auto & col_with_name : parquet_reader->header)
    {
        const auto & node = parquetReader->getParquetColumn(col_with_name.name);
        SelectiveColumnReaderPtr column_reader;
        column_reader = builder->buildReader(node, col_with_name.type, 0, 0);
        column_readers.push_back(column_reader);
        reader_data_types.push_back(column_reader->getResultType());
        reader_columns_mapping[col_with_name.name] = column_reader;
    }

    // fallback filter, for example, read number data using string type, number reader doesn't support BytesValuesFilter
    std::call_once(
        parquet_reader->filter_fallback_checked,
        [&]()
        {
            ActionsDAG::NodeRawConstPtrs unsupported_conditions;
            // find unsupported fast filter
            for (auto & [col_name, reader] : reader_columns_mapping)
            {
                if (!filters.contains(col_name))
                    continue;
                if (!reader->supportedFilterKinds().contains(filters.at(col_name)->kind()))
                {
                    parquet_reader->filters.erase(col_name);
                    auto nodes = parquetReader->filter_split_result->fallback_filters.at(col_name);
                    unsupported_conditions.insert(unsupported_conditions.end(), nodes.begin(), nodes.end());
                }
            }
            auto actions_dag = ActionsDAG::buildFilterActionsDAG(unsupported_conditions);
            if (actions_dag.has_value())
            {
                auto expr_filter = std::make_shared<ExpressionFilter>(std::move(actions_dag.value()));
                parquet_reader->addExpressionFilter(expr_filter);
            }
        });

    for (auto & [name, filter] : parquet_reader->filters)
    {
        if (reader_columns_mapping.contains(name))
            filter_columns.push_back(name);
    }
    // try merge read condition columns and result columns;
    if (prefetch_conditions && prefetch_conditions.get() != prefetch.get()
        && prefetch_conditions->totalSize() + prefetch->totalSize() < parquet_reader->settings.format_settings.parquet.local_read_min_bytes_for_seek)
    {
        bool merged = prefetch_conditions->merge(prefetch);
        if (merged)
        {
            prefetch = prefetch_conditions;
            context.prefetch = prefetch;
        }
    }


    if (prefetch_conditions)
        prefetch_conditions->startPrefetch();
    else
        prefetch->startPrefetch();
    selectConditions = std::make_unique<SelectConditions>(
        reader_columns_mapping, filter_columns, parquet_reader->expression_filters, parquet_reader->header);
}

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
        for (const auto & column : filter_columns)
        {
            auto reader = reader_columns_mapping.at(column);
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
            ProfileEvents::increment(ProfileEvents::ParquetSkippedRows, rows_to_read);
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
            ProfileEvents::increment(ProfileEvents::ParquetFilteredRows, rows_to_read);
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
                        columns.emplace_back(select_result.intermediate_columns.at(name)
                                                 ->filter(select_result.intermediate_filter, select_result.valid_count)
                                                 ->assumeMutable());
                }
                else
                {
                    auto & reader = reader_columns_mapping.at(name);
                    auto column = reader->createColumn();
                    column->reserve(select_result.valid_count);
                    reader->read(column, select_result.set, rows_to_read);
                    if (select_result.set)
                        select_result.set->setOffset(0);
                    columns.emplace_back(std::move(column));
                }
            }
            rows_read = columns[0]->size();
            metrics.filtered_rows += (rows_to_read - rows_read);
            ProfileEvents::increment(ProfileEvents::ParquetFilteredRows, rows_to_read - rows_read);
        }
        remain_rows -= rows_to_read;
    }

    metrics.output_rows += rows_read;
    ProfileEvents::increment(ProfileEvents::ParquetOutputRows, rows_read);
    if (rows_read)
    {
        Columns casted_columns;
        auto types = parquet_reader->header.getColumnsWithTypeAndName();
        for (size_t i = 0; i < types.size(); i++)
        {
            ColumnWithTypeAndName src_col = ColumnWithTypeAndName{std::move(columns[i]), reader_data_types.at(i), types.at(i).name};
            // intermediate column is already casted
            if (removeNullableOrLowCardinalityNullable(src_col.column)->getDataType()
                == removeNullableOrLowCardinalityNullable(types.at(i).type)->getColumnType())
            {
                casted_columns.emplace_back(std::move(src_col.column));
            }
            else
            {
                auto casted = castColumn(src_col, types.at(i).type);
                casted_columns.emplace_back(casted);
            }
        }
        return Chunk(std::move(casted_columns), rows_read);
    }
    else
        return {};
}

arrow::io::ReadRange getColumnRange(const parquet::ColumnChunkMetaData & column_metadata)
{
    // From ComputeColumnChunkRange() in contrib/arrow/cpp/src/parquet/file_reader.cc:
    //  > The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    //  > dictionary page header size in total_compressed_size and total_uncompressed_size
    //  > (see IMPALA-694). We add padding to compensate.
    int64_t col_start = column_metadata.data_page_offset();
    if (column_metadata.has_dictionary_page() && column_metadata.dictionary_page_offset() > 0
        && col_start > column_metadata.dictionary_page_offset())
    {
        col_start = column_metadata.dictionary_page_offset();
    }
    int64_t len = column_metadata.total_compressed_size();
    return {col_start, len};
}


RowGroupPrefetch::RowGroupPrefetch(
    SeekableReadBuffer & file_, std::mutex & mutex, const parquet::ArrowReaderProperties & arrow_properties_, ThreadPool & io_pool)
    : file(file_), file_mutex(mutex), arrow_properties(arrow_properties_)
{
    callback_runner = threadPoolCallbackRunnerUnsafe<ColumnChunkData>(io_pool, "ParquetRead");
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
    auto status = arrow::io::internal::CoalesceReadRanges(
        ranges, arrow_properties.cache_options().hole_size_limit, arrow_properties.cache_options().range_size_limit);
    if (!status.ok())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Failed to coalesce read ranges: {}", status.status().message());
    }
    else
        ranges = status.MoveValueUnsafe();
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
bool RowGroupPrefetch::merge(const RowGroupPrefetchPtr other)
{
    if (!other || other->ranges.empty())
        return false;
    if (this == other.get())
        return false;
    if (&this->file != &other->file || &this->file_mutex != &other->file_mutex)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RowGroupPrefetch: merge called with different files");
    if (fetched || other->fetched)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RowGroupPrefetch: merge called after startPrefetch");
    ranges.insert(ranges.end(), other->ranges.begin(), other->ranges.end());
    return true;
}
size_t RowGroupPrefetch::totalSize() const
{
    size_t total_size = 0;
    for (const auto & range : ranges)
        total_size += range.length;
    return total_size;
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

static void combineRowSetAndFilter(RowSet & set, const IColumn::Filter & filter_data)
{
    set.setOffset(0);
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
        for (const auto & name : expr_filter->getInputs())
        {
            if (!intermediate_columns.contains(name))
            {
                auto reader = readers.at(name);
                auto column = reader->createColumn();
                column->reserve(rows);
                if (count == rows)
                {
                    OptionalRowSet set;
                    reader->read(column, set, rows);
                }
                else
                {
                    reader->read(column, total_set, rows);
                    // clean row set offset
                    total_set->setOffset(0);
                }
                auto casted
                    = castColumn(ColumnWithTypeAndName{std::move(column), reader->getResultType(), name}, header.getByName(name).type);
                intermediate_columns.emplace(name, std::move(casted));
            }
            input.emplace_back(intermediate_columns.at(name)->cloneFinalized(), header.getByName(name).type, name);
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
        if (!intermediate_filters.empty())
        {
            auto filter = mergeFilters(intermediate_filters);
            combineRowSetAndFilter(total_set.value(), filter);
            auto valid_count = total_set.value().count();
            return SelectResult{std::move(total_set), std::move(intermediate_columns), std::move(filter), valid_count, false};
        }
        auto valid_count = total_set.value().count();
        return SelectResult{std::move(total_set), {}, {}, valid_count, false};
    }
}
SelectConditions::SelectConditions(
    std::unordered_map<String, SelectiveColumnReaderPtr> & readers_,
    std::vector<String> & fast_filter_columns_,
    std::vector<std::shared_ptr<ExpressionFilter>> & expression_filters_,
    const Block & header_)
    : readers(readers_), fast_filter_columns(fast_filter_columns_), expression_filters(expression_filters_), header(header_)
{
    has_filter = !fast_filter_columns.empty() || !expression_filters.empty();
}

}
