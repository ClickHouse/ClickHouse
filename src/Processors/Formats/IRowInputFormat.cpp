#include <Processors/Formats/IRowInputFormat.h>
#include <IO/WriteHelpers.h>    // toString
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_UUID;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
}


bool isParseError(int code)
{
    return code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
        || code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
        || code == ErrorCodes::CANNOT_PARSE_DATE
        || code == ErrorCodes::CANNOT_PARSE_DATETIME
        || code == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
        || code == ErrorCodes::CANNOT_PARSE_NUMBER
        || code == ErrorCodes::CANNOT_PARSE_UUID
        || code == ErrorCodes::CANNOT_READ_ALL_DATA
        || code == ErrorCodes::TOO_LARGE_STRING_SIZE
        || code == ErrorCodes::ARGUMENT_OUT_OF_BOUND       /// For Decimals
        || code == ErrorCodes::INCORRECT_DATA              /// For some ReadHelpers
        || code == ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
}

IRowInputFormat::IRowInputFormat(Block header, ReadBuffer & in_, Params params_)
    : IInputFormat(std::move(header), in_), params(params_)
{
    const auto & port_header = getPort().getHeader();
    size_t num_columns = port_header.columns();
    serializations.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i] = port_header.getByPosition(i).type->getDefaultSerialization();
}


Chunk IRowInputFormat::generate()
{
    if (total_rows == 0)
        readPrefix();

    const Block & header = getPort().getHeader();

    size_t num_columns = header.columns();
    MutableColumns columns = header.cloneEmptyColumns();

    ///auto chunk_missing_values = std::make_unique<ChunkMissingValues>();
    block_missing_values.clear();

    size_t num_rows = 0;

    try
    {
        RowReadExtension info;
        bool continue_reading = true;
        for (size_t rows = 0; rows < params.max_block_size && continue_reading; ++rows)
        {
            try
            {
                ++total_rows;

                info.read_columns.clear();
                continue_reading = readRow(columns, info);

                for (size_t column_idx = 0; column_idx < info.read_columns.size(); ++column_idx)
                {
                    if (!info.read_columns[column_idx])
                    {
                        size_t column_size = columns[column_idx]->size();
                        if (column_size == 0)
                            throw Exception("Unexpected empty column", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);
                        block_missing_values.setBit(column_idx, column_size - 1);
                    }
                }

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
                /// Logic for possible skipping of errors.

                if (!isParseError(e.code()))
                    throw;

                if (params.allow_errors_num == 0 && params.allow_errors_ratio == 0)
                    throw;

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

                /// Truncate all columns in block to initial size (remove values, that was appended to only part of columns).

                for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
                {
                    auto & column = columns[column_idx];
                    if (column->size() > num_rows)
                        column->popBack(column->size() - num_rows);
                }
            }
        }
    }
    catch (ParsingException & e)
    {
        String verbose_diagnostic;
        try
        {
            verbose_diagnostic = getDiagnosticInfo();
        }
        catch (const Exception & exception)
        {
            verbose_diagnostic = "Cannot get verbose diagnostic: " + exception.message();
        }
        catch (...)
        {
            /// Error while trying to obtain verbose diagnostic. Ok to ignore.
        }

        e.setLineNumber(total_rows);
        e.addMessage(verbose_diagnostic);
        throw;
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
        catch (...)
        {
            /// Error while trying to obtain verbose diagnostic. Ok to ignore.
        }

        e.addMessage("(at row " + toString(total_rows) + ")\n" + verbose_diagnostic);
        throw;
    }

    if (columns.empty() || columns[0]->empty())
    {
        if (num_errors && (params.allow_errors_num > 0 || params.allow_errors_ratio > 0))
        {
            Poco::Logger * log = &Poco::Logger::get("IRowInputFormat");
            LOG_DEBUG(log, "Skipped {} rows with errors while reading the input stream", num_errors);
        }

        readSuffix();
        return {};
    }

    Chunk chunk(std::move(columns), num_rows);
    //chunk.setChunkInfo(std::move(chunk_missing_values));
    return chunk;
}

void IRowInputFormat::syncAfterError()
{
    throw Exception("Method syncAfterError is not implemented for input format", ErrorCodes::NOT_IMPLEMENTED);
}

void IRowInputFormat::resetParser()
{
    IInputFormat::resetParser();
    total_rows = 0;
    num_errors = 0;
    block_missing_values.clear();
}


}
