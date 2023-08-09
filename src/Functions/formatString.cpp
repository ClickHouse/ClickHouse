#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <base/range.h>

#include <memory>
#include <string>
#include <vector>

#include "formatString.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <typename Name>
class FormatFunction : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FormatFunction>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 2",
                getName(),
                arguments.size());

        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!isStringOrFixedString(arg))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}",
                    arg->getName(),
                    arg_idx + 1,
                    getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & c0 = arguments[0].column;
        const ColumnConst * c0_const_string = typeid_cast<const ColumnConst *>(&*c0);

        if (!c0_const_string)
            throw Exception("First argument of function " + getName() + " must be constant string", ErrorCodes::ILLEGAL_COLUMN);

        String pattern = c0_const_string->getValue<String>();

        auto col_res = ColumnString::create();

        std::vector<const ColumnString::Chars *> data(arguments.size() - 1);
        std::vector<const ColumnString::Offsets *> offsets(arguments.size() - 1);
        std::vector<size_t> fixed_string_sizes(arguments.size() - 1);
        std::vector<std::optional<String>> constant_strings(arguments.size() - 1);

        bool has_column_string = false;
        bool has_column_fixed_string = false;
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const ColumnPtr & column = arguments[i].column;
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
            {
                has_column_string = true;
                data[i - 1] = &col->getChars();
                offsets[i - 1] = &col->getOffsets();
            }
            else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
            {
                has_column_fixed_string = true;
                data[i - 1] = &fixed_col->getChars();
                fixed_string_sizes[i - 1] = fixed_col->getN();
            }
            else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
            {
                constant_strings[i - 1] = const_col->getValue<String>();
            }
            else
                throw Exception(
                    "Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        FormatImpl::formatExecute(
            has_column_string,
            has_column_fixed_string,
            std::move(pattern),
            data,
            offsets,
            fixed_string_sizes,
            constant_strings,
            col_res->getChars(),
            col_res->getOffsets(),
            input_rows_count);

        return col_res;
    }
};


struct NameFormat
{
    static constexpr auto name = "format";
};
using FunctionFormat = FormatFunction<NameFormat>;

}

REGISTER_FUNCTION(Format)
{
    factory.registerFunction<FunctionFormat>();
}

}
