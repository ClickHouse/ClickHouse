#include <Processors/Formats/IRowInputFormat.h>
#include <DataTypes/ObjectUtils.h>
#include <IO/WriteHelpers.h>    // toString
#include <IO/WithFileName.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_READ_MAP_FROM_TEXT;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
}


bool isParseError(int code)
{
    return code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
        || code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
        || code == ErrorCodes::CANNOT_PARSE_DATE
        || code == ErrorCodes::CANNOT_PARSE_DATETIME
        || code == ErrorCodes::CANNOT_PARSE_NUMBER
        || code == ErrorCodes::CANNOT_PARSE_UUID
        || code == ErrorCodes::CANNOT_PARSE_BOOL
        || code == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
        || code == ErrorCodes::CANNOT_READ_MAP_FROM_TEXT
        || code == ErrorCodes::CANNOT_READ_ALL_DATA
        || code == ErrorCodes::TOO_LARGE_STRING_SIZE
        || code == ErrorCodes::ARGUMENT_OUT_OF_BOUND       /// For Decimals
        || code == ErrorCodes::INCORRECT_DATA              /// For some ReadHelpers
        || code == ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING
        || code == ErrorCodes::CANNOT_PARSE_IPV4
        || code == ErrorCodes::CANNOT_PARSE_IPV6
        || code == ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM
        || code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE;
}

IRowInputFormat::IRowInputFormat(Block header, ReadBuffer & in_, Params params_)
    : IInputFormat(std::move(header), &in_)
    , serializations(getPort().getHeader().getSerializations())
    , params(params_)
    , block_missing_values(getPort().getHeader().columns())
{
}

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
    if (total_rows == 0)
    {
        try
        {
            readPrefix();
        }
        catch (Exception & e)
        {
            e.addMessage("(while reading header)");
            throw;
        }
    }

    const Block & header = getPort().getHeader();

    size_t num_columns = header.columns();
    MutableColumns columns(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = header.getByPosition(i).type->createColumn(*serializations[i]);

    ColumnCheckpoints checkpoints(columns.size());
    for (size_t column_idx = 0; column_idx < columns.size(); ++column_idx)
        checkpoints[column_idx] = columns[column_idx]->getCheckpoint();

    block_missing_values.clear();

    size_t num_rows = 0;
    size_t chunk_start_offset = getDataOffsetMaybeCompressed(getReadBuffer());
    try
    {
        if (need_only_count && supportsCountRows())
        {
            num_rows = countRows(params.max_block_size);
            if (num_rows == 0)
            {
                readSuffix();
                return {};
            }
            total_rows += num_rows;
            approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(getReadBuffer()) - chunk_start_offset;
            return getChunkForCount(num_rows);
        }

        RowReadExtension info;
        bool continue_reading = true;
        for (size_t rows = 0; (rows < params.max_block_size || num_rows == 0) && continue_reading; ++rows)
        {
            try
            {
                for (size_t column_idx = 0; column_idx < columns.size(); ++column_idx)
                    columns[column_idx]->updateCheckpoint(*checkpoints[column_idx]);

                info.read_columns.clear();
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
            }
            catch (Exception & e)
            {
                ++total_rows;

                /// Logic for possible skipping of errors.

                if (!isParseError(e.code()))
                    throw;

                if (params.allow_errors_num == 0 && params.allow_errors_ratio == 0)
                    throw;

                if (errors_logger)
                    logError();

                ++num_errors;
                Float64 current_error_ratio = static_cast<Float64>(num_errors) / total_rows;

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

                syncAfterError();

                /// Rollback all columns in block to initial size (remove values, that was appended to only part of columns).
                for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
                    columns[column_idx]->rollback(*checkpoints[column_idx]);
            }
        }
    }
    catch (Exception & e)
    {
        if (!isParseError(e.code()))
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
