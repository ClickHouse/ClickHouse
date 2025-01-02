#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/formatString.h>
#include <IO/WriteHelpers.h>
#include <base/map.h>
#include <base/range.h>


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
template <typename Name, bool is_injective>
class ConcatWithSeparatorImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit ConcatWithSeparatorImpl(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context) { return std::make_shared<ConcatWithSeparatorImpl>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return is_injective; }

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
                has_column_string = true;
                data[2 * i] = &col->getChars();
                offsets[2 * i] = &col->getOffsets();
            }
            else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
            {
                has_column_fixed_string = true;
                data[2 * i] = &fixed_col->getChars();
                fixed_string_sizes[2 * i] = fixed_col->getN();
            }
            else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
            {
                constant_strings[2 * i] = const_col->getValue<String>();
            }
            else
            {
                /// A non-String/non-FixedString-type argument: use the default serialization to convert it to String
                auto full_column = column->convertToFullIfNeeded();
                auto serialization = arguments[i +1].type->getDefaultSerialization();
                auto converted_col_str = ColumnString::create();
                ColumnStringHelpers::WriteHelper write_helper(*converted_col_str, column->size());
                auto & write_buffer = write_helper.getWriteBuffer();
                FormatSettings format_settings;
                for (size_t row = 0; row < column->size(); ++row)
                {
                    serialization->serializeText(*full_column, row, write_buffer, format_settings);
                    write_helper.rowWritten();
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
};

struct NameConcatWithSeparator
{
    static constexpr auto name = "concatWithSeparator";
};
struct NameConcatWithSeparatorAssumeInjective
{
    static constexpr auto name = "concatWithSeparatorAssumeInjective";
};

using FunctionConcatWithSeparator = ConcatWithSeparatorImpl<NameConcatWithSeparator, false>;
using FunctionConcatWithSeparatorAssumeInjective = ConcatWithSeparatorImpl<NameConcatWithSeparatorAssumeInjective, true>;
}

REGISTER_FUNCTION(ConcatWithSeparator)
{
    factory.registerFunction<FunctionConcatWithSeparator>(FunctionDocumentation{
        .description = R"(
Returns the concatenation strings separated by string separator. Syntax: concatWithSeparator(sep, expr1, expr2, expr3...)
        )",
        .examples{{"concatWithSeparator", "SELECT concatWithSeparator('a', '1', '2', '3')", ""}},
        .categories{"String"}});

    factory.registerFunction<FunctionConcatWithSeparatorAssumeInjective>(FunctionDocumentation{
        .description = R"(
Same as concatWithSeparator, the difference is that you need to ensure that concatWithSeparator(sep, expr1, expr2, expr3...) → result is injective, it will be used for optimization of GROUP BY.

The function is named “injective” if it always returns different result for different values of arguments. In other words: different arguments never yield identical result.
        )",
        .examples{{"concatWithSeparatorAssumeInjective", "SELECT concatWithSeparatorAssumeInjective('a', '1', '2', '3')", ""}},
        .categories{"String"}});

    /// Compatibility with Spark and MySQL:
    factory.registerAlias("concat_ws", "concatWithSeparator", FunctionFactory::Case::Insensitive);
}

}
