#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>

#if USE_PARQUET

#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromVector.h>
#include <Processors/Port.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>


namespace CurrentMetrics
{
    extern const Metric ParquetEncoderThreads;
    extern const Metric ParquetEncoderThreadsActive;
    extern const Metric ParquetEncoderThreadsScheduled;
}

namespace DB
{

using namespace Parquet;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

    bool needsFileLevelVariantAnalysis(const DataTypePtr & type, bool output_json_as_variant)
    {
        return variantWriteRequiresFileLevelAnalysis(type, output_json_as_variant);
    }

    bool headerNeedsFileLevelVariantAnalysis(const Block & header, bool output_json_as_variant)
    {
        for (const auto & column : header)
        {
            if (needsFileLevelVariantAnalysis(column.type, output_json_as_variant))
                return true;
        }

        return false;
    }

    bool needsDataDependentVariantSchema(const DataTypePtr & type, bool output_json_as_variant)
    {
        if (variantWriteRequiresFileLevelAnalysis(type, output_json_as_variant))
            return true;

        DataTypePtr normalized_type = unwrapVariantTypeHint(type);
        if (!normalized_type)
            return false;

        if (const auto * object_type = typeid_cast<const DataTypeObject *>(normalized_type.get()))
        {
            if (!output_json_as_variant || object_type->getSchemaFormat() != DataTypeObject::SchemaFormat::JSON)
                return false;

            /// `JSON` columns with no declared shredded shape can still infer a
            /// first-row-group `typed_value` layout for faster `VARIANT` output.
            return !getDeclaredVariantShreddedTypeForParquetWrite(type);
        }

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_type.get()))
            return needsDataDependentVariantSchema(array_type->getNestedType(), output_json_as_variant);

        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type.get()))
        {
            for (const auto & element : tuple_type->getElements())
            {
                if (needsDataDependentVariantSchema(element, output_json_as_variant))
                    return true;
            }

            return false;
        }

        if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_type.get()))
        {
            return needsDataDependentVariantSchema(map_type->getKeyType(), output_json_as_variant)
                || needsDataDependentVariantSchema(map_type->getValueType(), output_json_as_variant);
        }

        return false;
    }

    bool headerNeedsDataDependentVariantSchema(const Block & header, bool output_json_as_variant)
    {
        for (const auto & column : header)
        {
            if (needsDataDependentVariantSchema(column.type, output_json_as_variant))
                return true;
        }

        return false;
    }

    TemporaryDataOnDiskScopePtr getParquetVariantAnalysisTempData()
    {
        if (auto query_context = CurrentThread::tryGetQueryContext())
            return query_context->getTempDataOnDisk();

        if (auto global_context = Context::getGlobalContextInstance())
            return global_context->getTempDataOnDisk();

        return nullptr;
    }
}

ParquetBlockOutputFormat::ParquetBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, FormatFilterInfoPtr format_filter_info_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}, format_filter_info(format_filter_info_)
{
    if (format_settings.parquet.parallel_encoding && format_settings.max_threads > 1)
        pool = std::make_unique<ThreadPool>(
            CurrentMetrics::ParquetEncoderThreads,
            CurrentMetrics::ParquetEncoderThreadsActive,
            CurrentMetrics::ParquetEncoderThreadsScheduled,
            format_settings.max_threads);

    using C = FormatSettings::ParquetCompression;
    switch (format_settings.parquet.output_compression_method)
    {
        case C::NONE: options.compression = CompressionMethod::None; break;
        case C::SNAPPY: options.compression = CompressionMethod::Snappy; break;
        case C::ZSTD: options.compression = CompressionMethod::Zstd; break;
        case C::LZ4: options.compression = CompressionMethod::Lz4; break;
        case C::GZIP: options.compression = CompressionMethod::Gzip; break;
        case C::BROTLI: options.compression = CompressionMethod::Brotli; break;
    }
    options.compression_level = static_cast<int>(format_settings.parquet.output_compression_level);
    options.output_string_as_string = format_settings.parquet.output_string_as_string;
    options.output_fixed_string_as_fixed_byte_array = format_settings.parquet.output_fixed_string_as_fixed_byte_array;
    options.output_datetime_as_uint32 = format_settings.parquet.output_datetime_as_uint32;
    options.output_date_as_uint16 = format_settings.parquet.output_date_as_uint16;
    options.output_enum_as_byte_array = format_settings.parquet.output_enum_as_byte_array;
    options.output_json_as_variant = format_settings.parquet.output_json_as_variant;
    options.data_page_size = format_settings.parquet.data_page_size;
    options.write_batch_size = format_settings.parquet.write_batch_size;
    options.write_page_index = format_settings.parquet.write_page_index;
    options.write_bloom_filter = format_settings.parquet.write_bloom_filter;
    options.write_checksums = format_settings.parquet.write_checksums;
    options.bloom_filter_bits_per_value = format_settings.parquet.bloom_filter_bits_per_value;
    options.bloom_filter_flush_threshold_bytes = format_settings.parquet.bloom_filter_flush_threshold_bytes;
    options.write_geometadata = format_settings.parquet.write_geometadata;
    options.max_dictionary_size = format_settings.parquet.max_dictionary_size;
    options.use_dictionary_encoding = options.max_dictionary_size > 0;

    if (format_filter_info_ && format_filter_info_->column_mapper)
        column_field_ids = format_filter_info_->column_mapper->getStorageColumnEncoding();

    const Block & header = getPort(PortKind::Main).getHeader();
    needs_file_level_variant_analysis = headerNeedsFileLevelVariantAnalysis(header, options.output_json_as_variant);
    if (needs_file_level_variant_analysis)
    {
        auto tmp_data = getParquetVariantAnalysisTempData();
        if (!tmp_data)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot initialize temporary storage for `Parquet` `VARIANT` analysis");

        buffered_input.emplace(std::make_shared<const Block>(materializeBlock(header)), tmp_data);
    }
    else if (!headerNeedsDataDependentVariantSchema(header, options.output_json_as_variant))
    {
        Block materialized_header = materializeBlock(header);
        schema = convertSchema(materialized_header, options, format_settings, column_field_ids, &variant_wrapper_paths);
    }
}

ParquetBlockOutputFormat::~ParquetBlockOutputFormat()
{
    if (pool)
    {
        is_stopped = true;
        pool->wait();
    }
}

void ParquetBlockOutputFormat::consume(Chunk chunk)
{
    if (needs_file_level_variant_analysis && !replaying_buffered_input)
    {
        if (chunk.getNumRows() == 0)
            return;

        const Block & header = getPort(PortKind::Main).getHeader();
        Block block = header.cloneWithColumns(chunk.detachColumns());
        buffered_input.value()->write(block);

        for (size_t i = 0; i < header.columns(); ++i)
        {
            analyzeVariantColumnTypesForWrite(
                block.getByPosition(i).column,
                header.getByPosition(i).type,
                options,
                format_settings,
                variant_write_analysis,
                header.getByPosition(i).name);
        }

        return;
    }

    /// Poll background tasks.
    if (pool)
    {
        std::unique_lock lock(mutex);
        while (true)
        {
            /// If some row groups are ready to be written to the file, write them.
            reapCompletedRowGroups(lock);

            if (background_exception)
                std::rethrow_exception(background_exception);

            if (is_stopped)
                return;

            /// If there's too much work in flight, wait for some of it to complete.
            if (row_groups.size() < 2)
                break;
            if (bytes_in_flight <= format_settings.parquet.row_group_bytes * 4 &&
                task_queue.size() <= format_settings.max_threads * 4)
                break;

            condvar.wait(lock);
        }
    }

    /// Do something like SquashingTransform to produce big enough row groups.
    /// Because the real SquashingTransform is only used for INSERT, not for SELECT ... INTO OUTFILE.
    /// The latter doesn't even have a pipeline where a transform could be inserted, so it's more
    /// convenient to do the squashing here. It's also parallelized here.
    if (chunk.getNumRows() != 0)
    {
        staging_rows += chunk.getNumRows();
        staging_bytes += chunk.allocatedBytes();
        staging_chunks.push_back(std::move(chunk));
    }

    const size_t target_rows = std::max(static_cast<UInt64>(1), format_settings.parquet.row_group_rows);

    if (staging_rows < target_rows &&
        staging_bytes < format_settings.parquet.row_group_bytes)
        return;

    /// In the rare case that more than `row_group_rows` rows arrived in one chunk, split the
    /// staging chunk into multiple row groups.
    if (staging_rows >= target_rows * 2)
    {
        /// Increase row group size slightly (by < 2x) to avoid a small row group at the end.
        size_t num_row_groups = std::max(static_cast<size_t>(1), staging_rows / target_rows);
        size_t row_group_size = (staging_rows - 1) / num_row_groups + 1; // round up

        Chunk concatenated = std::move(staging_chunks[0]);
        for (size_t i = 1; i < staging_chunks.size(); ++i)
            concatenated.append(staging_chunks[i]);
        staging_chunks.clear();

        for (size_t offset = 0; offset < staging_rows; offset += row_group_size)
        {
            size_t count = std::min(row_group_size, staging_rows - offset);
            MutableColumns columns = concatenated.cloneEmptyColumns();
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i]->insertRangeFrom(*concatenated.getColumns()[i], offset, count);

            Chunks piece;
            piece.emplace_back(std::move(columns), count);
            piece.back().setChunkInfos(concatenated.getChunkInfos());

            writeRowGroup(std::move(piece));
        }
    }
    else
    {
        writeRowGroup(std::move(staging_chunks));
    }

    staging_chunks.clear();
    staging_rows = 0;
    staging_bytes = 0;
}

void ParquetBlockOutputFormat::finalizeImpl()
{
    if (needs_file_level_variant_analysis && !replaying_buffered_input)
    {
        if (buffered_input)
            buffered_input->finishWriting();

        variant_type_hints.clear();
        for (const auto & [path, analysis] : variant_write_analysis)
        {
            if (auto shredded_type = inferVariantShreddedTypeForWrite(analysis))
                variant_type_hints.emplace(path, std::move(shredded_type));
        }

        replaying_buffered_input = true;
        if (buffered_input)
        {
            auto read_stream = buffered_input->getReadStream();
            while (true)
            {
                Block block = read_stream->read();
                if (block.empty())
                    break;

                consume(Chunk(block.getColumns(), block.rows()));
            }

            buffered_input.reset();
        }
    }

    if (!staging_chunks.empty())
        writeRowGroup(std::move(staging_chunks));

    if (pool)
    {
        std::unique_lock lock(mutex);

        /// Wait for background work to complete.
        while (true)
        {
            reapCompletedRowGroups(lock);

            if (background_exception)
                std::rethrow_exception(background_exception);

            if (is_stopped)
                return;

            if (row_groups.empty())
                break;

            condvar.wait(lock);
        }
    }

    if (file_state.offset == 0)
    {
        Block header = materializeBlock(getPort(PortKind::Main).getHeader());
        if (schema.empty())
            schema = convertSchema(header, options, format_settings, column_field_ids, &variant_wrapper_paths);
        base_offset = out.count();
        writeFileHeader(file_state, out);
    }
    Block header = materializeBlock(getPort(PortKind::Main).getHeader());
    writeFileFooter(file_state, schema, options, out, header, variant_wrapper_paths);
    chassert(out.count() - base_offset == file_state.offset);
}

void ParquetBlockOutputFormat::resetFormatterImpl()
{
    if (pool)
    {
        is_stopped = true;
        pool->wait();
        is_stopped = false;
    }

    background_exception = nullptr;
    threads_running = 0;
    task_queue.clear();
    row_groups.clear();
    schema.clear();
    variant_type_hints.clear();
    variant_write_analysis.clear();
    variant_wrapper_paths.clear();
    prepared_first_row_group_columns.clear();
    file_state = {};
    staging_chunks.clear();
    staging_rows = 0;
    staging_bytes = 0;
    replaying_buffered_input = false;
    buffered_input.reset();
    if (needs_file_level_variant_analysis)
    {
        auto tmp_data = getParquetVariantAnalysisTempData();
        if (!tmp_data)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot initialize temporary storage for `Parquet` `VARIANT` analysis");

        const Block & header = getPort(PortKind::Main).getHeader();
        buffered_input.emplace(std::make_shared<const Block>(materializeBlock(header)), tmp_data);
    }
}

void ParquetBlockOutputFormat::onCancel() noexcept
{
    is_stopped = true;
}

void ParquetBlockOutputFormat::writeRowGroup(std::vector<Chunk> chunks)
{
    if (pool)
    {
        writeRowGroupInParallel(std::move(chunks));
    }
    else
    {
        Chunk concatenated;
        for (auto & chunk : chunks)
        {
            if (concatenated.empty())
            {
                concatenated.swap(chunk);
            }
            else
            {
                concatenated.append(chunk);
                chunk.clear(); // free chunk's buffers so memory is released earlier
            }
        }
        writeRowGroupInOneThread(std::move(concatenated));
    }
}

void ParquetBlockOutputFormat::writeRowGroupInOneThread(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    const Block & header = getPort(PortKind::Main).getHeader();
    if (schema.empty())
        initializeSchemaForCustomEncoder(chunk.getColumns());

    Parquet::ColumnChunkWriteStates columns_to_write;
    if (!prepared_first_row_group_columns.empty())
    {
        auto first_row_group_columns = std::move(prepared_first_row_group_columns);
        prepared_first_row_group_columns.clear();
        chassert(first_row_group_columns.size() == header.columns());
        size_t total_subcolumns = 0;
        for (const auto & prepared : first_row_group_columns)
            total_subcolumns += prepared.size();
        columns_to_write.reserve(total_subcolumns);

        for (auto & prepared : first_row_group_columns)
            for (auto & state : prepared)
                columns_to_write.emplace_back(std::move(state));
    }
    else
    {
        chassert(header.columns() == chunk.getNumColumns());
        for (size_t i = 0; i < header.columns(); ++i)
            prepareColumnForWrite(
                chunk.getColumns()[i], header.getByPosition(i).type, header.getByPosition(i).name,
                options, format_settings, &columns_to_write, nullptr, column_field_ids, &variant_type_hints, nullptr, nullptr);
    }

    if (file_state.offset == 0)
    {
        base_offset = out.count();
        writeFileHeader(file_state, out);
    }
    for (auto & s : columns_to_write)
    {
        writeColumnChunkBody(s, options, format_settings, out);
        finalizeColumnChunkAndWriteFooter(std::move(s), file_state, out);
    }

    finalizeRowGroup(file_state, chunk.getNumRows(), options, out);
}

void ParquetBlockOutputFormat::writeRowGroupInParallel(std::vector<Chunk> chunks)
{
    std::unique_lock lock(mutex);

    const Block & header = getPort(PortKind::Main).getHeader();

    RowGroupState & r = row_groups.emplace_back();
    r.column_chunks.resize(header.columns());
    r.tasks_in_flight = 0;

    std::vector<Columns> columnses;
    for (auto & chunk : chunks)
    {
        chassert(header.columns() == chunk.getNumColumns());
        r.num_rows += chunk.getNumRows();
        columnses.push_back(chunk.detachColumns());
    }

    if (schema.empty())
    {
        Columns concatenated_columns;
        concatenated_columns.reserve(header.columns());

        for (size_t i = 0; i < header.columns(); ++i)
        {
            IColumn::MutablePtr concatenated = IColumn::mutate(columnses[0][i]->cloneEmpty());
            for (auto & columns : columnses)
                concatenated->insertRangeFrom(*columns[i], 0, columns[i]->size());
            concatenated_columns.push_back(std::move(concatenated));
        }

        initializeSchemaForCustomEncoder(concatenated_columns);
    }

    if (!prepared_first_row_group_columns.empty())
    {
        auto first_row_group_columns = std::move(prepared_first_row_group_columns);
        prepared_first_row_group_columns.clear();
        chassert(first_row_group_columns.size() == header.columns());
        for (size_t i = 0; i < header.columns(); ++i)
        {
            auto & prepared = first_row_group_columns[i];
            r.column_chunks[i].reserve(prepared.size());
            for (auto & state : prepared)
            {
                r.column_chunks[i].emplace_back(this);
                ++r.tasks_in_flight;

                auto & t = task_queue.emplace_back(&r, i, this);
                t.subcolumn_idx = r.column_chunks[i].size() - 1;
                t.state = std::move(state);
                t.mem.set(t.state.allocatedBytes());
            }
        }
    }
    else
    {
        for (size_t i = 0; i < header.columns(); ++i)
        {
            Task & t = task_queue.emplace_back(&r, i, this);
            t.column_type = header.getByPosition(i).type;
            t.column_name = header.getByPosition(i).name;

            /// Defer concatenating the columns to the threads.
            size_t bytes = 0;
            for (size_t j = 0; j < chunks.size(); ++j)
            {
                auto & col = columnses[j][i];
                bytes += col->allocatedBytes();
                t.column_pieces.push_back(std::move(col));
            }
            t.mem.set(bytes);
            ++r.tasks_in_flight;
        }
    }

    startMoreThreadsIfNeeded(lock);
}

void ParquetBlockOutputFormat::reapCompletedRowGroups(std::unique_lock<std::mutex> & lock)
{
    while (!row_groups.empty() && row_groups.front().tasks_in_flight == 0 && !is_stopped)
    {
        RowGroupState & r = row_groups.front();

        /// Write to the file.

        lock.unlock();

        if (file_state.offset == 0)
        {
            base_offset = out.count();
            writeFileHeader(file_state, out);
        }

        for (auto & cols : r.column_chunks)
        {
            for (ColumnChunk & col : cols)
            {
                out.write(col.serialized.data(), col.serialized.size());
                finalizeColumnChunkAndWriteFooter(std::move(col.state), file_state, out);
            }
        }

        finalizeRowGroup(file_state, r.num_rows, options, out);

        lock.lock();

        row_groups.pop_front();
    }
}

void ParquetBlockOutputFormat::startMoreThreadsIfNeeded(const std::unique_lock<std::mutex> &)
{
    /// Speculate that all current are already working on tasks.
    size_t to_add = std::min(task_queue.size(), format_settings.max_threads - threads_running);
    for (size_t i = 0; i < to_add; ++i)
    {
        auto job = [this, thread_group = CurrentThread::getGroup()]()
        {
            try
            {
                ThreadGroupSwitcher switcher(thread_group, ThreadName::PARQUET_ENCODER);

                threadFunction();
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                background_exception = std::current_exception();
                condvar.notify_all();
                --threads_running;
            }
        };

        if (threads_running == 0)
        {
            /// First thread. We need it to succeed; otherwise we may get stuck.
            pool->scheduleOrThrowOnError(job);
            ++threads_running;
        }
        else
        {
            /// More threads. This may be called from inside the thread pool, so avoid waiting;
            /// otherwise it may deadlock.
            if (!pool->trySchedule(job))
                break;
        }
    }
}

void ParquetBlockOutputFormat::threadFunction()
{
    std::unique_lock lock(mutex);

    while (true)
    {
        if (task_queue.empty() || is_stopped)
        {
            /// The check and the decrement need to be in the same critical section, to make sure
            /// we never get stuck with tasks but no threads.
            --threads_running;
            return;
        }

        auto task = std::move(task_queue.front());
        task_queue.pop_front();

        if (task.column_type)
        {
            lock.unlock();

            IColumn::MutablePtr concatenated = IColumn::mutate(std::move(task.column_pieces[0]));
            for (size_t i = 1; i < task.column_pieces.size(); ++i)
            {
                auto & c = task.column_pieces[i];
                concatenated->insertRangeFrom(*c, 0, c->size());
                c.reset();
            }
            task.column_pieces.clear();

            std::vector<ColumnChunkWriteState> subcolumns;
            prepareColumnForWrite(
                std::move(concatenated), task.column_type, task.column_name, options, format_settings, &subcolumns, nullptr, column_field_ids, &variant_type_hints, nullptr, nullptr);

            lock.lock();

            for (size_t i = 0; i < subcolumns.size(); ++i)
            {
                task.row_group->column_chunks[task.column_idx].emplace_back(this);
                task.row_group->tasks_in_flight += 1;

                auto & t = task_queue.emplace_back(task.row_group, task.column_idx, this);
                t.subcolumn_idx = i;
                t.state = std::move(subcolumns[i]);
                t.mem.set(t.state.allocatedBytes());
            }

            startMoreThreadsIfNeeded(lock);
        }
        else
        {
            lock.unlock();

            PODArray<char> serialized;
            {
                auto buf = WriteBufferFromVector<PODArray<char>>(serialized);
                writeColumnChunkBody(task.state, options, format_settings, buf);
            }

            lock.lock();

            auto & c = task.row_group->column_chunks[task.column_idx][task.subcolumn_idx];
            c.state = std::move(task.state);
            c.serialized = std::move(serialized);
            c.mem.set(c.serialized.size() + c.state.allocatedBytes());
        }

        --task.row_group->tasks_in_flight;

        condvar.notify_all();
    }
}

void ParquetBlockOutputFormat::initializeSchemaForCustomEncoder(const Columns & columns)
{
    const Block & header = getPort(PortKind::Main).getHeader();
    chassert(header.columns() == columns.size());
    schema.clear();
    if (!needs_file_level_variant_analysis)
        variant_type_hints.clear();
    variant_wrapper_paths.clear();
    prepared_first_row_group_columns.clear();
    prepared_first_row_group_columns.resize(header.columns());

    auto & root = schema.emplace_back();
    root.__set_name("schema");
    root.__set_num_children(static_cast<Int32>(header.columns()));

    for (size_t i = 0; i < header.columns(); ++i)
    {
        prepareColumnForWrite(
            columns[i],
            header.getByPosition(i).type,
            header.getByPosition(i).name,
            options,
            format_settings,
            &prepared_first_row_group_columns[i],
            &schema,
            column_field_ids,
            &variant_type_hints,
            needs_file_level_variant_analysis ? nullptr : &variant_type_hints,
            &variant_wrapper_paths);
    }
}

void registerOutputFormatParquet(FormatFactory & factory);
void registerOutputFormatParquet(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Parquet",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & format_settings,
           FormatFilterInfoPtr format_filter_info)
        {
            return std::make_shared<ParquetBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, format_filter_info);
        });
    factory.markFormatHasNoAppendSupport("Parquet");
    factory.markOutputFormatNotTTYFriendly("Parquet");
    factory.setContentType("Parquet", "application/octet-stream");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatParquet(FormatFactory &);
void registerOutputFormatParquet(FormatFactory &)
{
}
}

#endif
