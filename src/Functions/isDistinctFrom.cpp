#include <cstddef>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Core/NamesAndTypes.h>
#include <Functions/isDistinctFrom.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnNullable.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


ColumnPtr FunctionIsDistinctFrom::executeImpl(const ColumnsWithTypeAndName & arguments,
                                              const DataTypePtr &,
                                              size_t input_rows_count) const
{
    if (arguments.size() != 2)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} expects exactly 2 arguments, got {}",
            backQuote(name),
            arguments.size());
    }
    ColumnPtr left_col = arguments[0].column;
    ColumnPtr right_col = arguments[1].column;
    if (!left_col || !right_col)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected error.get null Column.");
    }

    if (!arguments[0].type || !arguments[1].type)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected error.get null DataType");
    }

    // get common type for null-safe comparsion
    DataTypePtr common_type = getLeastSupertype(DataTypes{arguments[0].type, arguments[1].type});

    ColumnPtr c0_converted = castColumn(arguments[0], common_type);
    ColumnPtr c1_converted = castColumn(arguments[1], common_type);

    // address null
    if (c0_converted->isNullable() && c1_converted->isNullable())
    {
        auto c_res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = c_res->getData();
        vec_res.resize(arguments[0].column->size());
        c0_converted = c0_converted->convertToFullColumnIfConst();
        c1_converted = c1_converted->convertToFullColumnIfConst();

        for (size_t i = 0; i < input_rows_count ; i++)
        {
            bool is_null_l = c0_converted->isNullAt(i);
            bool is_null_r = c1_converted->isNullAt(i);
            if (is_null_l && is_null_r)
            {
                vec_res[i] = 0;
            }
            else if ((!is_null_l && is_null_r) || (is_null_l && !is_null_r))
            {
                vec_res[i] = 1;
            }
            else
            {
                vec_res[i] = c0_converted->compareAt(i, i, *c1_converted, 1) == 0 ? 0 : 1;
            }
        }
        return c_res;
    }

    // address normal
    ColumnPtr res;

    FunctionOverloadResolverPtr not_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionComparison<NotEqualsOp, NameNotEquals>>(params));

    auto executable_func = not_equals->build(arguments);
    auto data_type = executable_func->getResultType();
    res = executable_func->execute(arguments, data_type, input_rows_count, /* dry_run = */ false);

    return res;
}

REGISTER_FUNCTION(IsDistinctFrom)
{
    FunctionDocumentation::Description description = R"(
        Performs a null-safe comparison between two values.
        Returns `true` if the values are distinct (not equal), treating `NULL` values as comparable.
    )";
    FunctionDocumentation::Syntax syntax = "isDistinctFrom(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "First key to compare.", {"Any"}},
        {"y", "Second key to compare.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns `true` when values are distinct, `false` when values are equal (including both NULL).",
        {"Bool"}
    };
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Logical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsDistinctFrom>(documentation);
}

}
