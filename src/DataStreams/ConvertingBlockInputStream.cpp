#include <DataStreams/ConvertingBlockInputStream.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnConst.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


static ColumnPtr castColumnWithDiagnostic(const ColumnWithTypeAndName & src_elem, const ColumnWithTypeAndName & res_elem)
{
    try
    {
        return castColumn(src_elem, res_elem.type);
    }
    catch (Exception & e)
    {
        e.addMessage("while converting source column " + backQuoteIfNeed(src_elem.name) + " to destination column " + backQuoteIfNeed(res_elem.name));
        throw;
    }
}


ConvertingBlockInputStream::ConvertingBlockInputStream(
    const BlockInputStreamPtr & input,
    const Block & result_header,
    MatchColumnsMode mode)
    : header(result_header), conversion(header.columns())
{
    children.emplace_back(input);

    Block input_header = input->getHeader();

    size_t num_input_columns = input_header.columns();
    size_t num_result_columns = result_header.columns();

    if (mode == MatchColumnsMode::Position && num_input_columns != num_result_columns)
        throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

    for (size_t result_col_num = 0; result_col_num < num_result_columns; ++result_col_num)
    {
        const auto & res_elem = result_header.getByPosition(result_col_num);

        switch (mode)
        {
            case MatchColumnsMode::Position:
                conversion[result_col_num] = result_col_num;
                break;

            case MatchColumnsMode::Name:
                if (input_header.has(res_elem.name))
                    conversion[result_col_num] = input_header.getPositionByName(res_elem.name);
                else
                    throw Exception("Cannot find column " + backQuote(res_elem.name) + " in source stream",
                        ErrorCodes::THERE_IS_NO_COLUMN);
                break;
        }

        const auto & src_elem = input_header.getByPosition(conversion[result_col_num]);

        /// Check constants.

        if (isColumnConst(*res_elem.column))
        {
            if (!isColumnConst(*src_elem.column))
                throw Exception("Cannot convert column " + backQuoteIfNeed(res_elem.name)
                    + " because it is non constant in source stream but must be constant in result",
                    ErrorCodes::ILLEGAL_COLUMN);
            else if (assert_cast<const ColumnConst &>(*src_elem.column).getField() != assert_cast<const ColumnConst &>(*res_elem.column).getField())
                throw Exception("Cannot convert column " + backQuoteIfNeed(res_elem.name)
                    + " because it is constant but values of constants are different in source and result",
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        /// Check conversion by dry run CAST function.

        castColumnWithDiagnostic(src_elem, res_elem);
    }
}


Block ConvertingBlockInputStream::readImpl()
{
    Block src = children.back()->read();

    if (!src)
        return src;

    Block res = header.cloneEmpty();
    /// This is important because header.cloneEmpty() doesn't copy info about aggregation bucket.
    /// Otherwise information in buckets may be lost (and aggregation will return wrong result).
    res.info = src.info;

    for (size_t res_pos = 0, size = conversion.size(); res_pos < size; ++res_pos)
    {
        const auto & src_elem = src.getByPosition(conversion[res_pos]);
        auto & res_elem = res.getByPosition(res_pos);

        ColumnPtr converted = castColumnWithDiagnostic(src_elem, res_elem);

        if (isColumnConst(*src_elem.column) && !isColumnConst(*res_elem.column))
            converted = converted->convertToFullColumnIfConst();

        res_elem.column = std::move(converted);
    }
    return res;
}

}
