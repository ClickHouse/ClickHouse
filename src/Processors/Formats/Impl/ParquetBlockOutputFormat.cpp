#include "ParquetBlockOutputFormat.h"

#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromVector.h>
#include <parquet/arrow/writer.h>
#include "ArrowBufferedStreams.h"
#include "CHColumnToArrowColumn.h"


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
    extern const int UNKNOWN_EXCEPTION;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

    parquet::ParquetVersion::type getParquetVersion(const FormatSettings & settings)
    {
        switch (settings.parquet.output_version)
        {
            case FormatSettings::ParquetVersion::V1_0:
                return parquet::ParquetVersion::PARQUET_1_0;
            case FormatSettings::ParquetVersion::V2_4:
                return parquet::ParquetVersion::PARQUET_2_4;
            case FormatSettings::ParquetVersion::V2_6:
                return parquet::ParquetVersion::PARQUET_2_6;
            case FormatSettings::ParquetVersion::V2_LATEST:
                return parquet::ParquetVersion::PARQUET_2_LATEST;
        }
    }

    parquet::Compression::type getParquetCompression(FormatSettings::ParquetCompression method)
    {
        if (method == FormatSettings::ParquetCompression::NONE)
            return parquet::Compression::type::UNCOMPRESSED;

#if USE_SNAPPY
        if (method == FormatSettings::ParquetCompression::SNAPPY)
            return parquet::Compression::type::SNAPPY;
#endif

#if USE_BROTLI
        if (method == FormatSettings::ParquetCompression::BROTLI)
            return parquet::Compression::type::BROTLI;
#endif

        if (method == FormatSettings::ParquetCompression::ZSTD)
            return parquet::Compression::type::ZSTD;

        if (method == FormatSettings::ParquetCompression::LZ4)
            return parquet::Compression::type::LZ4;

        if (method == FormatSettings::ParquetCompression::GZIP)
            return parquet::Compression::type::GZIP;

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported parquet compression method");
    }
}

ParquetBlockOutputFormat::ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}
{
    if (format_settings.parquet.use_custom_encoder)
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
        options.output_string_as_string = format_settings.parquet.output_string_as_string;
        options.output_fixed_string_as_fixed_byte_array = format_settings.parquet.output_fixed_string_as_fixed_byte_array;
        options.data_page_size = format_settings.parquet.data_page_size;
        options.write_batch_size = format_settings.parquet.write_batch_size;

        schema = convertSchema(header_, options);
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

    if (format_settings.parquet.use_custom_encoder)
    {
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

        if (row_groups_complete.empty())
        {
            base_offset = out.count();
            writeFileHeader(out);
        }
        writeFileFooter(std::move(row_groups_complete), schema, options, out);
    }
    else
    {
        if (!file_writer)
        {
            Block header = materializeBlock(getPort(PortKind::Main).getHeader());
            std::vector<Chunk> chunks;
            chunks.push_back(Chunk(header.getColumns(), 0));
            writeRowGroup(std::move(chunks));
        }

        if (file_writer)
        {
            auto status = file_writer->Close();
            if (!status.ok())
                throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while closing a table: {}", status.ToString());
        }
    }
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
    file_writer.reset();
    row_groups_complete.clear();
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
        writeRowGroupInParallel(std::move(chunks));
    else if (!format_settings.parquet.use_custom_encoder)
        writeUsingArrow(std::move(chunks));
    else
    {
        Chunk concatenated;
        while (!chunks.empty())
        {
            if (concatenated.empty())
                concatenated.swap(chunks.back());
            else
                concatenated.append(chunks.back());
            chunks.pop_back();
        }
        writeRowGroupInOneThread(std::move(concatenated));
    }
}

void ParquetBlockOutputFormat::writeUsingArrow(std::vector<Chunk> chunks)
{
    const size_t columns_num = chunks.at(0).getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;

    if (!ch_column_to_arrow_column)
    {
        const Block & header = getPort(PortKind::Main).getHeader();
        ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(
            header,
            "Parquet",
            CHColumnToArrowColumn::Settings
            {
                .output_string_as_string = format_settings.parquet.output_string_as_string,
                .output_fixed_string_as_fixed_byte_array = format_settings.parquet.output_fixed_string_as_fixed_byte_array
            });
    }

    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunks, columns_num);

    if (!file_writer)
    {
        auto sink = std::make_shared<ArrowBufferedOutputStream>(out);

        parquet::WriterProperties::Builder builder;
        builder.version(getParquetVersion(format_settings));
        builder.compression(getParquetCompression(format_settings.parquet.output_compression_method));
        // write page index is disable at default.
        if (format_settings.parquet.write_page_index)
            builder.enable_write_page_index();

        parquet::ArrowWriterProperties::Builder writer_props_builder;
        if (format_settings.parquet.output_compliant_nested_types)
            writer_props_builder.enable_compliant_nested_types();
        else
            writer_props_builder.disable_compliant_nested_types();

        auto result = parquet::arrow::FileWriter::Open(
            *arrow_table->schema(),
            ArrowMemoryPool::instance(),
            sink,
            builder.build(),
            writer_props_builder.build());
        if (!result.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while opening a table: {}", result.status().ToString());
        file_writer = std::move(result.ValueOrDie());
    }

    auto status = file_writer->WriteTable(*arrow_table, INT64_MAX);

    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while writing a table: {}", status.ToString());
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

    if (row_groups_complete.empty())
    {
        base_offset = out.count();
        writeFileHeader(out);
    }

    std::vector<parquet::format::ColumnChunk> column_chunks;
    for (auto & s : columns_to_write)
    {
        size_t offset = out.count() - base_offset;
        writeColumnChunkBody(s, options, out);
        auto c = finalizeColumnChunkAndWriteFooter(offset, std::move(s), options, out);
        column_chunks.push_back(std::move(c));
    }

    auto r = makeRowGroup(std::move(column_chunks), chunk.getNumRows());
    row_groups_complete.push_back(std::move(r));
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

        if (row_groups_complete.empty())
        {
            base_offset = out.count();
            writeFileHeader(out);
        }

        std::vector<parquet::format::ColumnChunk> metadata;
        for (auto & cols : r.column_chunks)
        {
            for (ColumnChunk & col : cols)
            {
                size_t offset = out.count() - base_offset;

                out.write(col.serialized.data(), col.serialized.size());
                auto m = finalizeColumnChunkAndWriteFooter(offset, std::move(col.state), options, out);

                metadata.push_back(std::move(m));
            }
        }

        row_groups_complete.push_back(makeRowGroup(std::move(metadata), r.num_rows));

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
            if (thread_group)
                CurrentThread::attachToGroupIfDetached(thread_group);
            SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););

            try
            {
                setThreadName("ParquetEncoder");

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
                WriteBufferFromVector buf(serialized);
                writeColumnChunkBody(task.state, options, buf);
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
           const FormatSettings & format_settings)
        {
            return std::make_shared<ParquetBlockOutputFormat>(buf, sample, format_settings);
        });
    factory.markFormatHasNoAppendSupport("Parquet");
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
