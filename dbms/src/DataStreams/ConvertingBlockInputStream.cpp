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
}


ConvertingBlockInputStream::ConvertingBlockInputStream(
    const Context & context_,
    const BlockInputStreamPtr & input,
    const Block & result_header)
    : context(context_), header(result_header), conversion(header.columns())
{
    children.emplace_back(input);

    Block input_header = input->getHeader();
    size_t num_input_columns = input_header.columns();

    for (size_t result_col_num = 0, num_result_columns = result_header.columns(); result_col_num < num_result_columns; ++result_col_num)
    {
        const auto & res_elem = result_header.getByPosition(result_col_num);

        if (input_header.has(res_elem.name))
            conversion[result_col_num] = input_header.getPositionByName(res_elem.name);
        else if (result_col_num < num_input_columns)
            conversion[result_col_num] = result_col_num;
        else
            throw Exception("Cannot find column " + backQuoteIfNeed(res_elem.name) + " in source stream",
                ErrorCodes::THERE_IS_NO_COLUMN);

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

        castColumn(src_elem, res_elem.type, context);
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

        ColumnPtr converted = castColumn(src_elem, res_elem.type, context);

        if (src_elem.column->isColumnConst() && !res_elem.column->isColumnConst())
            converted = converted->convertToFullColumnIfConst();

        res_elem.column = std::move(converted);
    }
    return res;
}

}
