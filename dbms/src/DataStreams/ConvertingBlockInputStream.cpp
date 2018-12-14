#include <DataStreams/ConvertingBlockInputStream.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnConst.h>
#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


static ColumnPtr castColumnWithDiagnostic(const ColumnWithTypeAndName & src_elem, const ColumnWithTypeAndName & res_elem, const Context & context)
{
    try
    {
        return castColumn(src_elem, res_elem.type, context);
    }
    catch (Exception & e)
    {
        e.addMessage("while converting source column " + backQuoteIfNeed(src_elem.name) + " to destination column " + backQuoteIfNeed(res_elem.name));
        throw;
    }
}


ConvertingBlockInputStream::ConvertingBlockInputStream(
    const Context & context_,
    const BlockInputStreamPtr & input,
    const Block & result_header,
    MatchColumnsMode mode)
    : context(context_), header(result_header), conversion(header.columns())
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
                    throw Exception("Cannot find column " + backQuoteIfNeed(res_elem.name) + " in source stream",
                        ErrorCodes::THERE_IS_NO_COLUMN);
                break;
        }

        const auto & src_elem = input_header.getByPosition(conversion[result_col_num]);

        /// Check constants.

        if (res_elem.column->isColumnConst())
        {
            if (!src_elem.column->isColumnConst())
                throw Exception("Cannot convert column " + backQuoteIfNeed(res_elem.name)
                    + " because it is non constant in source stream but must be constant in result",
                    ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
            else if (static_cast<const ColumnConst &>(*src_elem.column).getField() != static_cast<const ColumnConst &>(*res_elem.column).getField())
                throw Exception("Cannot convert column " + backQuoteIfNeed(res_elem.name)
                    + " because it is constant but values of constants are different in source and result",
                    ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
        }

        /// Check conversion by dry run CAST function.

        castColumnWithDiagnostic(src_elem, res_elem, context);
    }
}


Block ConvertingBlockInputStream::readImpl()
{
    Block src = children.back()->read();

    if (!src)
        return src;

    Block res = header.cloneEmpty();
    for (size_t res_pos = 0, size = conversion.size(); res_pos < size; ++res_pos)
    {
        const auto & src_elem = src.getByPosition(conversion[res_pos]);
        auto & res_elem = res.getByPosition(res_pos);

        ColumnPtr converted = castColumnWithDiagnostic(src_elem, res_elem, context);

        if (src_elem.column->isColumnConst() && !res_elem.column->isColumnConst())
            converted = converted->convertToFullColumnIfConst();

        res_elem.column = std::move(converted);
    }
    return res;
}

}
