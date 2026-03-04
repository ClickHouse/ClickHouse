#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/formatString.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
}

namespace
{

class ConcatWithSeparatorImpl : public IFunction
{
public:
    ConcatWithSeparatorImpl(ContextPtr context_, const char * name_, bool is_injective_)
        : context(context_), function_name(name_), injective(is_injective_) {}

    static FunctionPtr create(ContextPtr context, const char * name, bool is_injective)
    {
        return std::make_shared<ConcatWithSeparatorImpl>(context, name, is_injective);
    }

    String getName() const override { return function_name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return injective; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 1",
                getName(),
                arguments.size());

        const auto * separator_arg = arguments[0].get();
        if (!isStringOrFixedString(separator_arg))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}",
                separator_arg->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        assert(!arguments.empty());
        if (arguments.size() == 1)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        const ColumnConst * col_sep = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());
        if (!col_sep)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}. Must be a constant String.",
                arguments[0].column->getName(),
                getName());
        String sep_str = col_sep->getValue<String>();

        const size_t num_exprs = arguments.size() - 1;
        const size_t num_args = 2 * num_exprs - 1;

        std::vector<const ColumnString::Chars *> data(num_args);
        std::vector<const ColumnString::Offsets *> offsets(num_args);
        std::vector<size_t> fixed_string_sizes(num_args);
        std::vector<std::optional<String>> constant_strings(num_args);
        std::vector<ColumnString::MutablePtr> converted_col_ptrs(num_args);

        bool has_column_string = false;
        bool has_column_fixed_string = false;

        for (size_t i = 0; i < num_exprs; ++i)
        {
            if (i != 0)
                constant_strings[2 * i - 1] = sep_str;

            const ColumnPtr & column = arguments[i + 1].column;
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
            {
                chassert(col->size() == input_rows_count);

                has_column_string = true;
                data[2 * i] = &col->getChars();
                offsets[2 * i] = &col->getOffsets();
            }
            else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
            {
                chassert(fixed_col->size() == input_rows_count);

                has_column_fixed_string = true;
                data[2 * i] = &fixed_col->getChars();
                fixed_string_sizes[2 * i] = fixed_col->getN();
            }
            else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
            {
                constant_strings[2 * i] = const_col->getValue<String>();
            }
            else if (const auto * const_col_any = checkAndGetColumn<ColumnConst>(column.get()))
            {
                WriteBufferFromOwnString buf;
                FormatSettings format_settings;
                auto serialization = arguments[i + 1].type->getDefaultSerialization();

                const auto & nested = const_col_any->getDataColumn();
                serialization->serializeText(nested, 0, buf, format_settings);

                constant_strings[2 * i] = buf.str();
            }
            else
            {
                /// A non-String/non-FixedString-type argument: use the default serialization to convert it to String.
                /// Only strip top-level wrappers (Const, Sparse, LowCardinality) without recursing into subcolumns.
                /// Using the recursive convertToFullIfNeeded would strip LowCardinality from inside
                /// compound types like Variant while the type is not updated, creating a type/column mismatch.
                auto full_column
                    = column->convertToFullColumnIfConst()->convertToFullColumnIfSparse()->convertToFullColumnIfLowCardinality();

                chassert(full_column->size() == input_rows_count);

                auto serialization = arguments[i +1].type->getDefaultSerialization();
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
                converted_col_ptrs[i] = std::move(converted_col_str);

                /// Same as the normal `ColumnString` branch
                has_column_string = true;
                data[2 * i] = &converted_col_ptrs[i]->getChars();
                offsets[2 * i] = &converted_col_ptrs[i]->getOffsets();
            }
        }

        String pattern;
        pattern.reserve(num_args * 2);
        for (size_t i = 0; i < num_args; ++i)
            pattern += "{}";

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
        return std::move(col_res);
    }

private:
    ContextWeakPtr context;
    const char * function_name;
    bool injective;
};

}

REGISTER_FUNCTION(ConcatWithSeparator)
{
    FunctionDocumentation::Description description = R"(
Concatenates the provided strings, separating them by the specified separator.
)";
    FunctionDocumentation::Syntax syntax = "concatWithSeparator(sep[, exp1, exp2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"sep", "The separator to use.", {"const String", "const FixedString"}},
        {"exp1, exp2, ...", "Expression to be concatenated. Arguments which are not of type `String` or `FixedString` are converted to strings using their default serialization. As this decreases performance, it is not recommended to use non-String/FixedString arguments.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the String created by concatenating the arguments. If any of the argument values is `NULL`, the function returns `NULL`.",
        {"String"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT concatWithSeparator('a', '1', '2', '3', '4')",
        R"(
┌─concatWithSeparator('a', '1', '2', '3', '4')─┐
│ 1a2a3a4                                      │
└──────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    FunctionDocumentation::Description description_injective = R"(
Like [`concatWithSeparator`](#concatWithSeparator) but assumes that `concatWithSeparator(sep[,exp1, exp2, ... ]) → result` is injective.
A function is called injective if it returns different results for different arguments.

Can be used for optimization of `GROUP BY`.
)";
    FunctionDocumentation::Syntax syntax_injective = "concatWithSeparatorAssumeInjective(sep[, exp1, exp2, ... ])";
    FunctionDocumentation::Arguments arguments_injective = {
        {"sep", "The separator to use.", {"const String", "const FixedString"}},
        {"exp1, exp2, ...", "Expression to be concatenated. Arguments which are not of type `String` or `FixedString` are converted to strings using their default serialization. As this decreases performance, it is not recommended to use non-String/FixedString arguments.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_injective = {
        "Returns the String created by concatenating the arguments. If any of the argument values is `NULL`, the function returns `NULL`.",
        {"String"}
    };
    FunctionDocumentation::Examples examples_injective = {
    {
        "Usage example",
        R"(
CREATE TABLE user_data (
user_id UInt32,
first_name String,
last_name String,
score UInt32
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO user_data VALUES
(1, 'John', 'Doe', 100),
(2, 'Jane', 'Smith', 150),
(3, 'John', 'Wilson', 120),
(4, 'Jane', 'Smith', 90);

SELECT
    concatWithSeparatorAssumeInjective('-', first_name, last_name) as full_name,
    sum(score) as total_score
FROM user_data
GROUP BY concatWithSeparatorAssumeInjective('-', first_name, last_name);
        )",
        R"(
┌─full_name───┬─total_score─┐
│ Jane-Smith  │         240 │
│ John-Doe    │         100 │
│ John-Wilson │         120 │
└─────────────┴─────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_injective = {description_injective, syntax_injective, arguments_injective, {}, returned_value_injective, examples_injective, introduced_in, category};

    factory.registerFunction("concatWithSeparator", [](ContextPtr ctx){ return ConcatWithSeparatorImpl::create(ctx, "concatWithSeparator", false); }, documentation);
    factory.registerFunction("concatWithSeparatorAssumeInjective", [](ContextPtr ctx){ return ConcatWithSeparatorImpl::create(ctx, "concatWithSeparatorAssumeInjective", true); }, documentation_injective);

    /// Compatibility with Spark and MySQL:
    factory.registerAlias("concat_ws", "concatWithSeparator", FunctionFactory::Case::Insensitive);
}

}
