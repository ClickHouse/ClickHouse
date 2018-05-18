#include <Common/Exception.h>
#include <DataStreams/BlockInputStreamFromRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_UUID;
    extern const int TOO_LARGE_STRING_SIZE;
}


BlockInputStreamFromRowInputStream::BlockInputStreamFromRowInputStream(
    const RowInputStreamPtr & row_input_,
    const Block & sample_,
    size_t max_block_size_,
    UInt64 allow_errors_num_,
    Float64 allow_errors_ratio_)
    : row_input(row_input_), sample(sample_), max_block_size(max_block_size_),
    allow_errors_num(allow_errors_num_), allow_errors_ratio(allow_errors_ratio_)
{
}


static bool isParseError(int code)
{
    return code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
        || code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
        || code == ErrorCodes::CANNOT_PARSE_DATE
        || code == ErrorCodes::CANNOT_PARSE_DATETIME
        || code == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
        || code == ErrorCodes::CANNOT_PARSE_NUMBER
        || code == ErrorCodes::CANNOT_PARSE_UUID
        || code == ErrorCodes::TOO_LARGE_STRING_SIZE;
}


Block BlockInputStreamFromRowInputStream::readImpl()
{
    size_t num_columns = sample.columns();
    MutableColumns columns = sample.cloneEmptyColumns();

    try
    {
        for (size_t rows = 0; rows < max_block_size; ++rows)
        {
            try
            {
                ++total_rows;
                if (!row_input->read(columns))
                    break;
            }
            catch (Exception & e)
            {
                /// Logic for possible skipping of errors.

                if (!isParseError(e.code()))
                    throw;

                if (allow_errors_num == 0 && allow_errors_ratio == 0)
                    throw;

                ++num_errors;
                Float64 current_error_ratio = static_cast<Float64>(num_errors) / total_rows;

                if (num_errors > allow_errors_num
                    && current_error_ratio > allow_errors_ratio)
                {
                    e.addMessage("(Already have " + toString(num_errors) + " errors"
                        " out of " + toString(total_rows) + " rows"
                        ", which is " + toString(current_error_ratio) + " of all rows)");
                    throw;
                }

                if (!row_input->allowSyncAfterError())
                {
                    e.addMessage("(Input format doesn't allow to skip errors)");
                    throw;
                }

                row_input->syncAfterError();

                /// Truncate all columns in block to minimal size (remove values, that was appended to only part of columns).

                size_t min_size = std::numeric_limits<size_t>::max();
                for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
                    min_size = std::min(min_size, columns[column_idx]->size());

                for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
                {
                    auto & column = columns[column_idx];
                    if (column->size() > min_size)
                        column->popBack(column->size() - min_size);
                }
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
            verbose_diagnostic = row_input->getDiagnosticInfo();
        }
        catch (...)
        {
            /// Error while trying to obtain verbose diagnostic. Ok to ignore.
        }

        e.addMessage("(at row " + toString(total_rows) + ")\n" + verbose_diagnostic);
        throw;
    }

    if (columns.empty() || columns[0]->empty())
        return {};

    return sample.cloneWithColumns(std::move(columns));
}

}
