#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>

#if USE_PARQUET

#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <Processors/Port.h>

#include <limits>
#include <unordered_set>


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
    extern const int BAD_ARGUMENTS;
}

namespace
{

    /// Enumerate, in the same DFS order that `prepareColumnRecursive` visits them, the dotted
    /// `field_id`-bearing paths under a column named `name` of type `type`. `Nullable` and
    /// `LowCardinality` wrappers don't add a path segment; `Array` adds `.element`, `Map` adds
    /// `.key` / `.value`, and `Tuple` adds the subfield names.
    void enumerateFieldPaths(const String & name, const DataTypePtr & type, std::vector<String> & out)
    {
        out.push_back(name);

        DataTypePtr inner = type;
        while (true)
        {
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(inner.get()))
            {
                inner = nullable_type->getNestedType();
                continue;
            }
            if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(inner.get()))
            {
                inner = lc_type->getDictionaryType();
                continue;
            }
            break;
        }

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(inner.get()))
        {
            enumerateFieldPaths(name + ".element", array_type->getNestedType(), out);
        }
        else if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(inner.get()))
        {
            for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
                enumerateFieldPaths(name + "." + tuple_type->getNameByPosition(i + 1), tuple_type->getElement(i), out);
        }
        else if (const auto * map_type = typeid_cast<const DataTypeMap *>(inner.get()))
        {
            enumerateFieldPaths(name + ".key", map_type->getKeyType(), out);
            enumerateFieldPaths(name + ".value", map_type->getValueType(), out);
        }
    }

    /// Build the per-column Parquet `field_id` map from the user-facing settings.
    /// Returns nullopt when the output should carry no field_ids (both overrides empty
    /// and auto-assign disabled).
    ///
    ///   1. The flattened schema paths must themselves be unique — if two output columns or
    ///      nested fields flatten to the same dotted key, the build is rejected.
    ///   2. Every entry in `overrides` is applied verbatim. Negative ids, unknown paths,
    ///      duplicate paths and duplicate ids are rejected so users get a clear signal when
    ///      the setting drifts from the query's schema. Keys may be top-level column names
    ///      or dotted nested paths (e.g. `arr.element`, `m.key`, `m.value`, `t.subfield`).
    ///   3. If `auto_assign` is true, the remaining paths — top-level and nested — are given
    ///      the smallest unused positive ids in schema DFS order (Iceberg writers
    ///      conventionally start at 1 and go up).
    ///   4. If `auto_assign` is false, the override map must cover every path produced by
    ///      the schema (top-level and nested).
    std::optional<std::unordered_map<String, Int64>> buildColumnFieldIds(
        const Block & header,
        const std::vector<std::pair<String, Int32>> & overrides,
        bool auto_assign)
    {
        if (overrides.empty() && !auto_assign)
            return std::nullopt;

        std::vector<String> all_paths;
        for (const auto & col : header)
            enumerateFieldPaths(col.name, col.type, all_paths);

        /// Path identity is a dotted string, so a top-level column literally named `a.b` and the
        /// nested field `b` under `a Tuple(b ...)` would flatten to the same key. Letting that slide
        /// silently would either emit duplicate `field_id`s in auto-assign mode or make full coverage
        /// in override-only mode impossible. Detect the collision and reject it up front.
        std::unordered_set<String> known_paths;
        known_paths.reserve(all_paths.size());
        for (const auto & path : all_paths)
        {
            if (!known_paths.insert(path).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "output_format_parquet_column_field_ids / output_format_parquet_auto_assign_field_ids "
                    "cannot disambiguate Parquet schema path '{}': two output columns or nested fields flatten "
                    "to the same dotted path. Rename the conflicting top-level column (or nested subfield) so "
                    "no name contains a '.' that collides with another column's nested path.",
                    path);
        }

        std::unordered_map<String, Int64> result;
        std::unordered_set<Int32> used_ids;

        for (const auto & [name, id] : overrides)
        {
            if (id < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "output_format_parquet_column_field_ids value {} must be non-negative", id);
            if (!known_paths.contains(name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "output_format_parquet_column_field_ids references unknown column '{}'", name);
            if (!result.emplace(name, id).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "output_format_parquet_column_field_ids has duplicate column '{}'", name);
            if (!used_ids.insert(id).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "output_format_parquet_column_field_ids assigns id {} to more than one column", id);
        }

        if (auto_assign)
        {
            Int32 next_id = 1;
            for (const auto & path : all_paths)
            {
                if (result.contains(path))
                    continue;
                while (used_ids.contains(next_id))
                    ++next_id;
                result.emplace(path, next_id);
                used_ids.insert(next_id);
                ++next_id;
            }
        }
        else
        {
            /// If auto-assign is off we require the map to cover every path so that the
            /// resulting Parquet file isn't a mix of "has field_id" and "no field_id" fields.
            if (result.size() != all_paths.size())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "output_format_parquet_column_field_ids is non-empty but does not cover every output column "
                    "or nested field (enable output_format_parquet_auto_assign_field_ids to fill the gaps automatically)");
        }

        return result;
    }
}

ParquetBlockOutputFormat::ParquetBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, FormatFilterInfoPtr format_filter_info_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}, format_filter_info(format_filter_info_)
{
    const bool has_metadata_mapping = format_filter_info_ && format_filter_info_->column_mapper;
    const bool user_set_field_ids = !format_settings.parquet.column_field_ids.empty()
        || format_settings.parquet.auto_assign_field_ids;

    /// When a datalake (e.g. Iceberg) writer hands us a column-id mapping, that mapping is
    /// the source of truth — letting session settings override it would produce Parquet files
    /// whose `field_id`s no longer match the table metadata, breaking subsequent reads.
    if (has_metadata_mapping && user_set_field_ids)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "output_format_parquet_column_field_ids / output_format_parquet_auto_assign_field_ids "
            "cannot be used when writing to a datalake table that provides its own column-id mapping");

    /// Resolve Parquet `field_id`s from the user-facing settings (explicit overrides and/or
    /// the Iceberg-style auto-assign toggle). Used only when there is no metadata mapping.
    column_field_ids = buildColumnFieldIds(
        *header_,
        format_settings.parquet.column_field_ids,
        format_settings.parquet.auto_assign_field_ids);

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

    /// Datalake (Iceberg) metadata mapping wins over user settings — see the check above.
    const auto & effective_field_ids = has_metadata_mapping
        ? std::optional<std::unordered_map<String, Int64>>(format_filter_info_->column_mapper->getStorageColumnEncoding())
        : column_field_ids;

    schema = convertSchema(*header_, options, effective_field_ids);
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
        base_offset = out.count();
        writeFileHeader(file_state, out);
    }
    Block header = materializeBlock(getPort(PortKind::Main).getHeader());
    writeFileFooter(file_state, schema, options, out, header);
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
    file_state = {};
    staging_chunks.clear();
    staging_rows = 0;
    staging_bytes = 0;
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
    Parquet::ColumnChunkWriteStates columns_to_write;
    chassert(header.columns() == chunk.getNumColumns());
    for (size_t i = 0; i < header.columns(); ++i)
        prepareColumnForWrite(
            chunk.getColumns()[i], header.getByPosition(i).type, header.getByPosition(i).name,
            options, &columns_to_write);

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
    r.tasks_in_flight = r.column_chunks.size();

    std::vector<Columns> columnses;
    for (auto & chunk : chunks)
    {
        chassert(header.columns() == chunk.getNumColumns());
        r.num_rows += chunk.getNumRows();
        columnses.push_back(chunk.detachColumns());
    }

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
                std::move(concatenated), task.column_type, task.column_name, options, &subcolumns);

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
void registerOutputFormatParquet(FormatFactory &)
{
}
}

#endif
