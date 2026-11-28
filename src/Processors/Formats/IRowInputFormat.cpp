#include <Columns/IColumn.h>
#include <IO/WithFileName.h>
#include <IO/WithFileSize.h>
#include <IO/WriteHelpers.h> // toString
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/ParseError.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int SOCKET_TIMEOUT;
    extern const int NETWORK_ERROR;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_WRITE_TO_SOCKET;
    extern const int UNEXPECTED_END_OF_FILE;
}


static bool isConnectionError(int code)
{
    return code == ErrorCodes::SOCKET_TIMEOUT || code == ErrorCodes::NETWORK_ERROR || code == ErrorCodes::CANNOT_READ_FROM_SOCKET
        || code == ErrorCodes::CANNOT_WRITE_TO_SOCKET || code == ErrorCodes::UNEXPECTED_END_OF_FILE;
}

IRowInputFormat::IRowInputFormat(SharedHeader header, ReadBuffer & in_, Params params_)
    : IInputFormat(std::move(header), &in_)
    , serializations(getPort().getHeader().getSerializations())
    , params(params_)
    , block_missing_values(getPort().getHeader().columns())
{}

void IRowInputFormat::logError()
{
    String diagnostic;
    String raw_data;
    try
    {
        std::tie(diagnostic, raw_data) = getDiagnosticAndRawData();
    }
    catch (const Exception & exception)
    {
        diagnostic = "Cannot get diagnostic: " + exception.message();
        raw_data = "Cannot get raw data: " + exception.message();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Error while trying to obtain verbose diagnostic. Ok to ignore.
    }
    trimLeft(diagnostic, '\n');
    trimRight(diagnostic, '\n');

    auto now_time = time(nullptr);

    errors_logger->logError(InputFormatErrorsLogger::ErrorEntry{now_time, total_rows, diagnostic, raw_data});
}

Chunk IRowInputFormat::read()
{
    if (got_connection_exception)
        return {};

    if (everySegmentHasPrefix() ? !prefix_read : total_rows == 0)
    {
        try
        {
            readPrefix();
            prefix_read = true;
        }
        catch (Exception & e)
        {
            e.addMessage("(while reading header)");
            throw;
        }
    }

    const Block & header = getPort().getHeader();
    size_t num_columns = header.columns();
    MutableColumns columns = header.cloneEmptyColumns(serializations);

    block_missing_values.clear();

    size_t num_rows = 0;
    size_t total_rows_before_read = total_rows;

    /// A row can be consumed without being emitted to the chunk: a parse error
    /// tolerated by `input_format_allow_errors_*` is rolled back, and e.g.
    /// `format_regexp_skip_unmatched` makes `readRow` consume an unmatched line
    /// without inserting it. Remember which of the consumed rows were emitted, so
    /// that `_row_number` keeps the physical row numbers of the emitted rows (see
    /// `ChunkInfoRowNumbers`). Stays empty while no row is skipped - the common case.
    IColumnFilter emitted_row_mask;

    size_t chunk_start_offset = getDataOffsetMaybeCompressed(getReadBuffer());
    try
    {
        /// `countRows` skips rows without parsing them, so it cannot skip bad rows as allowed by `input_format_allow_errors_*`.
        if (need_only_count && supportsCountRows() && params.allow_errors_num == 0 && params.allow_errors_ratio == 0)
        {
            num_rows = countRows(params.max_block_size_rows);
            if (num_rows == 0)
            {
                readSuffix();
                return {};
            }
            total_rows += num_rows;
            approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(getReadBuffer()) - chunk_start_offset;
            auto chunk = getChunkForCount(num_rows);
            chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(total_rows_before_read));
            return chunk;
        }

        RowReadExtension info;
        bool continue_reading = true;
        Stopwatch watch(CLOCK_MONOTONIC_COARSE);
        size_t total_bytes = 0;

        size_t max_block_size_rows = params.max_block_size_rows;
        size_t max_block_size_bytes = params.max_block_size_bytes;
        size_t min_block_size_rows = params.min_block_size_rows;
        size_t min_block_size_bytes = params.min_block_size_bytes;
        size_t max_block_wait_ms = params.max_block_wait_ms;

        auto below_some_min_threshold = [&](size_t rows, size_t bytes)-> bool
        {
            return (!min_block_size_rows && !min_block_size_bytes) || rows < min_block_size_rows || bytes < min_block_size_bytes;
        };

        auto below_all_max_thresholds = [&](size_t rows, size_t bytes)-> bool
        {
            return (!max_block_size_rows || rows < max_block_size_rows) && (!max_block_size_bytes || bytes < max_block_size_bytes);
        };

        for (size_t rows = 0; ((below_some_min_threshold(rows, total_bytes) && below_all_max_thresholds(rows, total_bytes)) || num_rows == 0)
             && continue_reading;
             ++rows)
        {
            /// The `QueryStatus`, not `IProcessor::isCancelled`: the latter is also raised by a
            /// `CancelReason::PartialResult` stop, which must still return its partial result.
            if (rows != 0 && rows % CANCELLATION_CHECK_PERIOD_ROWS == 0)
                CurrentThread::checkIfNotCancelled();

            if (max_block_wait_ms != 0 && num_rows > 0)
            {
                UInt64 elapsed_ms = watch.elapsedMilliseconds();
                if (elapsed_ms >= max_block_wait_ms)
                    break;

                UInt64 remaining_us = (max_block_wait_ms - elapsed_ms) * 1000;
                if (!getReadBuffer().poll(remaining_us))
                    break;
            }

            try
            {
                info.read_columns.clear();
                size_t first_column_size_before = columns.empty() ? 0 : columns.front()->size();
                continue_reading = readRow(columns, info);
                for (size_t column_idx = 0; column_idx < info.read_columns.size(); ++column_idx)
                {
                    if (!info.read_columns[column_idx])
                    {
                        size_t column_size = columns[column_idx]->size();
                        if (column_size == 0)
                            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Unexpected empty column");
                        block_missing_values.setBit(column_idx, column_size - 1);
                    }
                }

                /// A consumed row that was not emitted: `readRow` returned true (the line
                /// was consumed) but inserted nothing into `columns`, e.g. because
                /// `format_regexp_skip_unmatched` skipped an unmatched line.
                bool row_emitted = columns.empty() ? continue_reading : columns.front()->size() > first_column_size_before;
                if (continue_reading || row_emitted)
                {
                    size_t row_idx = total_rows - total_rows_before_read;
                    if (!emitted_row_mask.empty() || !row_emitted)
                    {
                        /// `resize_fill` pads the rows before the first skipped one with `1`s,
                        /// and is a no-op once the mask is being maintained.
                        emitted_row_mask.resize_fill(row_idx, 1);
                        emitted_row_mask.push_back(row_emitted);
                    }
                }

                ++total_rows;

                /// Some formats may read row AND say the read is finished.
                /// For such a case, get the number or rows from first column.
                if (!columns.empty())
                    num_rows = columns.front()->size();

                if (!continue_reading)
                    break;

                /// The case when there is no columns. Just count rows.
                if (columns.empty())
                    ++num_rows;

                if (min_block_size_bytes || max_block_size_bytes)
                {
                    for (const auto & column : columns)
                        total_bytes += column->byteSizeAt(column->size() - 1);
                }

                if (max_block_wait_ms != 0 && num_rows > 0 && watch.elapsedMilliseconds() >= max_block_wait_ms)
                    break;
            }
            catch (Exception & e)
            {
                ++total_rows;

                /// Logic for possible skipping of errors.

                /// Skip a bad row only for a genuine parse error that was not thrown from the
                /// buffer itself. A throwing read self-cancels the buffer (ReadBuffer::next()'s
                /// catch handler), and syncAfterError() reads from it again
                /// (skipToNextLineOrEOF/ignore/eof -> next()), tripping chassert(!isCanceled()).
                if (!isParseError(e.code()) || getReadBuffer().isCanceled())
                    throw;

                if (params.allow_errors_num == 0 && params.allow_errors_ratio == 0)
                    throw;

                if (errors_logger)
                    logError();

                ++num_errors;
                Float64 current_error_ratio = static_cast<Float64>(num_errors) / static_cast<double>(total_rows);

                if (num_errors > params.allow_errors_num
                    && current_error_ratio > params.allow_errors_ratio)
                {
                    e.addMessage("(Already have " + toString(num_errors) + " errors"
                        " out of " + toString(total_rows) + " rows"
                        ", which is " + toString(current_error_ratio) + " of all rows)");
                    throw;
                }

                if (!allowSyncAfterError())
                {
                    e.addMessage("(Input format doesn't allow to skip errors)");
                    throw;
                }

                /// The bad row is consumed (counted in `total_rows` above) but, after the
                /// rollback below, is not emitted to the chunk. Remember it in the mask of
                /// emitted rows, so that `_row_number` skips it (see `ChunkInfoRowNumbers`).
                emitted_row_mask.resize_fill(total_rows - total_rows_before_read - 1, 1);
                emitted_row_mask.push_back(0);

                syncAfterError();

                /// Rollback all columns in block to initial size (remove values, that was appended to only part of columns).
                for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
                {
                    auto & column = columns[column_idx];
                    if (column->size() > num_rows)
                        column->popBack(column->size() - num_rows);
                }
            }
        }
    }
    catch (Exception & e)
    {
        if (params.connection_handling && isConnectionError(e.code()))
        {
            got_connection_exception  = true;

            for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
            {
                auto & column = columns[column_idx];
                if (column->size() > num_rows)
                    column->popBack(column->size() - num_rows);
            }
        }
        else
        {
            /// Collect verbose diagnostics only for a genuine parse error that was not thrown
            /// from the buffer itself. A throwing read self-cancels the buffer, and
            /// getDiagnosticInfo() reads from it (eof() -> next()), tripping chassert(!isCanceled()).
            if (!isParseError(e.code()) || getReadBuffer().isCanceled())
                throw;

            String verbose_diagnostic;
            try
            {
                verbose_diagnostic = getDiagnosticInfo();
            }
            catch (const Exception & exception)
            {
                verbose_diagnostic = "Cannot get verbose diagnostic: " + exception.message();
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Error while trying to obtain verbose diagnostic. Ok to ignore.
            }

            e.addMessage(fmt::format("(at row {})\n", total_rows));
            e.addMessage(verbose_diagnostic);
            throw;
        }

    }

    if (columns.empty() || columns[0]->empty())
    {
        if (num_errors && (params.allow_errors_num > 0 || params.allow_errors_ratio > 0))
        {
            LoggerPtr log = getLogger("IRowInputFormat");
            LOG_DEBUG(log, "Skipped {} rows with errors while reading the input stream", num_errors);
        }

        readSuffix();
        return {};
    }

    for (const auto & column : columns)
        column->finalize();

    Chunk chunk(std::move(columns), num_rows);
    std::optional<IColumnFilter> applied_filter;
    if (!emitted_row_mask.empty())
        applied_filter = std::move(emitted_row_mask);
    chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(total_rows_before_read, std::move(applied_filter)));
    approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(getReadBuffer()) - chunk_start_offset;


    return chunk;
}

void IRowInputFormat::syncAfterError()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method syncAfterError is not implemented for input format {}", getName());
}

void IRowInputFormat::resetParser()
{
    IInputFormat::resetParser();
    total_rows = 0;
    num_errors = 0;
    prefix_read = false;
    got_connection_exception = false;
    block_missing_values.clear();
}

size_t IRowInputFormat::countRows(size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method countRows is not implemented for input format {}", getName());
}

void IRowInputFormat::setSerializationHints(const SerializationInfoByName & hints)
{
    if (supportsCustomSerializations())
        serializations = getPort().getHeader().getSerializations(hints);
}


}
