#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/formatString.h>
#include <IO/WriteHelpers.h>

#include <memory>
#include <string>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FormatFunction : public IFunction
{
public:
    static constexpr auto name = "format";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FormatFunction>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 2",
                getName(),
                arguments.size());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & c0 = arguments[0].column;
        const ColumnConst * c0_const_string = typeid_cast<const ColumnConst *>(&*c0);

        if (!c0_const_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be constant string", getName());

        String pattern = c0_const_string->getValue<String>();

        auto col_res = ColumnString::create();

        std::vector<const ColumnString::Chars *> data(arguments.size() - 1);
        std::vector<const ColumnString::Offsets *> offsets(arguments.size() - 1);
        std::vector<size_t> fixed_string_sizes(arguments.size() - 1);
        std::vector<std::optional<String>> constant_strings(arguments.size() - 1);
        std::vector<ColumnString::MutablePtr> converted_col_ptrs(arguments.size() - 1);

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
            {
                /// A non-String/non-FixedString-type argument: use the default serialization to convert it to String.
                /// Only strip top-level wrappers (Const, Sparse, LowCardinality) without recursing into subcolumns.
                /// Using the recursive convertToFullIfNeeded would strip LowCardinality from inside
                /// compound types like Variant while the type is not updated, creating a type/column mismatch.
                auto full_column
                    = column->convertToFullColumnIfConst()->convertToFullColumnIfSparse()->convertToFullColumnIfLowCardinality();
                auto serialization = arguments[i].type->getDefaultSerialization();
                auto converted_col_str = ColumnString::create();
                ColumnStringHelpers::WriteHelper<ColumnString> write_helper(*converted_col_str, column->size());
                auto & write_buffer = write_helper.getWriteBuffer();
                FormatSettings format_settings;
                for (size_t row = 0; row < column->size(); ++row)
                {
                    serialization->serializeText(*full_column, row, write_buffer, format_settings);
                    write_helper.finishRow();
                }
                write_helper.finalize();

                /// Keep the pointer alive
                converted_col_ptrs[i - 1] = std::move(converted_col_str);

                /// Same as the normal `ColumnString` branch
                has_column_string = true;
                data[i - 1] = &converted_col_ptrs[i - 1]->getChars();
                offsets[i - 1] = &converted_col_ptrs[i - 1]->getOffsets();
            }
        }

        FormatStringImpl::formatExecute(
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


using FunctionFormat = FormatFunction;

}

REGISTER_FUNCTION(Format)
{
    FunctionDocumentation::Description description = R"(
Format the `pattern` string with the values (strings, integers, etc.) listed in the arguments, similar to formatting in Python.
The pattern string can contain replacement fields surrounded by curly braces `{}`.
Anything not contained in braces is considered literal text and copied verbatim into the output.
Literal brace character can be escaped by two braces: `{{` and `}}`.
Field names can be numbers (starting from zero) or empty (then they are implicitly given monotonically increasing numbers).
)";
    FunctionDocumentation::Syntax syntax = "format(pattern, s0[, s1, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"pattern", "The format string containing placeholders.", {"String"}},
        {"s0[, s1, ...]", "One or more values to substitute into the pattern.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a formatted string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Numbered placeholders",
        "SELECT format('{1} {0} {1}', 'World', 'Hello')",
        R"(
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
        )"
    },
    {
        "Implicit numbering",
        "SELECT format('{} {}', 'Hello', 'World')",
        R"(
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringReplacement;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFormat>(documentation);
}

}
