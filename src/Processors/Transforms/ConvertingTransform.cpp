#include <Processors/Transforms/ConvertingTransform.h>

#include <Interpreters/castColumn.h>
#include <Columns/ColumnConst.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

static ColumnPtr castColumnWithDiagnostic(
    const ColumnWithTypeAndName & src_elem,
    const ColumnWithTypeAndName & res_elem)
{
    try
    {
        return castColumn(src_elem, res_elem.type);
    }
    catch (Exception & e)
    {
        e.addMessage("while converting source column " + backQuoteIfNeed(src_elem.name) +
                     " to destination column " + backQuoteIfNeed(res_elem.name));
        throw;
    }
}

ConvertingTransform::ConvertingTransform(
    Block source_header_,
    Block result_header_,
    MatchColumnsMode mode_,
    bool ignore_constant_values_)
    : ISimpleTransform(std::move(source_header_), std::move(result_header_), false)
    , conversion(getOutputPort().getHeader().columns())
    , ignore_constant_values(ignore_constant_values_)
{
    const auto & source = getInputPort().getHeader();
    const auto & result = getOutputPort().getHeader();

    size_t num_input_columns = source.columns();
    size_t num_result_columns = result.columns();

    if (mode_ == MatchColumnsMode::Position && num_input_columns != num_result_columns)
        throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

    for (size_t result_col_num = 0; result_col_num < num_result_columns; ++result_col_num)
    {
        const auto & res_elem = result.getByPosition(result_col_num);

        switch (mode_)
        {
            case MatchColumnsMode::Position:
                conversion[result_col_num] = result_col_num;
                break;

            case MatchColumnsMode::Name:
                /// It may seem strange, but sometimes block may have columns with the same name.
                /// For this specific case, try to get column from the same position if it has correct name first.
                if (result_col_num < source.columns() && source.getByPosition(result_col_num).name == res_elem.name)
                    conversion[result_col_num] = result_col_num;
                else if (source.has(res_elem.name))
                    conversion[result_col_num] = source.getPositionByName(res_elem.name);
                else
                    throw Exception("Cannot find column " + backQuoteIfNeed(res_elem.name) + " in source stream",
                                    ErrorCodes::THERE_IS_NO_COLUMN);
                break;
        }

        const auto & src_elem = source.getByPosition(conversion[result_col_num]);

        /// Check constants.

        if (const auto * res_const = typeid_cast<const ColumnConst *>(res_elem.column.get()))
        {
            if (const auto * src_const = typeid_cast<const ColumnConst *>(src_elem.column.get()))
            {
                if (!ignore_constant_values && res_const->getField() != src_const->getField())
                    throw Exception("Cannot convert column " + backQuoteIfNeed(res_elem.name) + " because "
                                    "it is constant but values of constants are different in source and result",
                                    ErrorCodes::ILLEGAL_COLUMN);
            }
            else
                throw Exception("Cannot convert column " + backQuoteIfNeed(res_elem.name) + " because "
                                "it is non constant in source stream but must be constant in result",
                                ErrorCodes::ILLEGAL_COLUMN);
        }

        /// Check conversion by dry run CAST function.

        castColumnWithDiagnostic(src_elem, res_elem);
    }
}

void ConvertingTransform::transform(Chunk & chunk)
{
    const auto & source = getInputPort().getHeader();
    const auto & result = getOutputPort().getHeader();

    auto num_rows = chunk.getNumRows();
    auto src_columns = chunk.detachColumns();

    size_t num_res_columns = conversion.size();

    Columns res_columns;
    res_columns.reserve(num_res_columns);

    for (size_t res_pos = 0; res_pos < num_res_columns; ++res_pos)
    {
        auto src_elem = source.getByPosition(conversion[res_pos]);
        src_elem.column = src_columns[conversion[res_pos]];
        auto res_elem = result.getByPosition(res_pos);

        if (ignore_constant_values && isColumnConst(*res_elem.column))
        {
            res_columns.emplace_back(res_elem.column->cloneResized(num_rows));
            continue;
        }

        ColumnPtr converted = castColumnWithDiagnostic(src_elem, res_elem);

        if (!isColumnConst(*res_elem.column))
            converted = converted->convertToFullColumnIfConst();

        res_columns.emplace_back(std::move(converted));
    }

    chunk.setColumns(std::move(res_columns), num_rows);
}

}
