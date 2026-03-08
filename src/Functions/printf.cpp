#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/formatString.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

#include <memory>
#include <vector>
#include <fmt/format.h>
#include <fmt/printf.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionPrintf : public IFunction
{
private:
    FunctionOverloadResolverPtr function_concat;

    struct Instruction
    {
        std::string_view format;
        size_t rows;
        bool is_literal; /// format is literal string without any argument
        ColumnWithTypeAndName input; /// Only used when is_literal is false

        ColumnWithTypeAndName execute() const
        {
            if (is_literal)
                return executeLiteral(format);
            if (isColumnConst(*input.column))
                return executeConstant(input);
            return executeNonconstant(input);
        }

    private:
        ColumnWithTypeAndName executeLiteral(std::string_view literal) const
        {
            ColumnWithTypeAndName res;
            auto str_col = ColumnString::create();
            str_col->insert(fmt::sprintf(literal));
            res.column = ColumnConst::create(std::move(str_col), rows);
            res.type = std::make_shared<DataTypeString>();
            return res;
        }

        ColumnWithTypeAndName executeConstant(const ColumnWithTypeAndName & arg) const
        {
            ColumnWithTypeAndName tmp_arg = arg;
            const auto & const_col = static_cast<const ColumnConst &>(*arg.column);
            tmp_arg.column = const_col.getDataColumnPtr();

            ColumnWithTypeAndName tmp_res = executeNonconstant(tmp_arg);
            return ColumnWithTypeAndName{ColumnConst::create(tmp_res.column, arg.column->size()), tmp_res.type, tmp_res.name};
        }

        template <typename T>
        bool executeNumber(const IColumn & column, ColumnString::Chars & res_chars, ColumnString::Offsets & res_offsets) const
        {
            const ColumnVector<T> * concrete_column = checkAndGetColumn<ColumnVector<T>>(&column);
            if (!concrete_column)
                return false;

            String s;
            size_t curr_offset = 0;
            const auto & data = concrete_column->getData();
            for (size_t i = 0; i < data.size(); ++i)
            {
                T a = data[i];
                s = fmt::sprintf(format, static_cast<NearestFieldType<T>>(a));

                res_chars.resize(curr_offset + s.size());
                memcpy(&res_chars[curr_offset], s.data(), s.size());
                curr_offset += s.size();
                res_offsets[i] = curr_offset;
            }
            return true;
        }

        template <typename COLUMN>
        bool executeString(const IColumn & column, ColumnString::Chars & res_chars, ColumnString::Offsets & res_offsets) const
        {
            const COLUMN * concrete_column = checkAndGetColumn<COLUMN>(&column);
            if (!concrete_column)
                return false;

            String s;
            size_t curr_offset = 0;
            for (size_t i = 0; i < concrete_column->size(); ++i)
            {
                auto a = concrete_column->getDataAt(i);
                s = fmt::sprintf(format, a);

                res_chars.resize(curr_offset + s.size());
                memcpy(&res_chars[curr_offset], s.data(), s.size());
                curr_offset += s.size();
                res_offsets[i] = curr_offset;
            }
            return true;
        }

        ColumnWithTypeAndName executeNonconstant(const ColumnWithTypeAndName & arg) const
        {
            size_t size = arg.column->size();
            auto res_col = ColumnString::create();
            auto & res_str = static_cast<ColumnString &>(*res_col);
            auto & res_offsets = res_str.getOffsets();
            auto & res_chars = res_str.getChars();
            res_offsets.resize_exact(size);
            res_chars.reserve(format.size() * size);

            WhichDataType which(arg.type);
            if (which.isNativeNumber()
                && (executeNumber<UInt8>(*arg.column, res_chars, res_offsets) || executeNumber<UInt16>(*arg.column, res_chars, res_offsets)
                    || executeNumber<UInt32>(*arg.column, res_chars, res_offsets)
                    || executeNumber<UInt64>(*arg.column, res_chars, res_offsets)
                    || executeNumber<Int8>(*arg.column, res_chars, res_offsets) || executeNumber<Int16>(*arg.column, res_chars, res_offsets)
                    || executeNumber<Int32>(*arg.column, res_chars, res_offsets)
                    || executeNumber<Int64>(*arg.column, res_chars, res_offsets)
                    || executeNumber<Float32>(*arg.column, res_chars, res_offsets)
                    || executeNumber<Float64>(*arg.column, res_chars, res_offsets)))
            {
                return {std::move(res_col), std::make_shared<DataTypeString>(), arg.name};
            }
            if (which.isStringOrFixedString()
                && (executeString<ColumnString>(*arg.column, res_chars, res_offsets)
                    || executeString<ColumnFixedString>(*arg.column, res_chars, res_offsets)))
            {
                return {std::move(res_col), std::make_shared<DataTypeString>(), arg.name};
            }
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument type of function {} is {}, but native numeric or string type is expected",
                FunctionPrintf::name,
                arg.type->getName());
        }
    };

public:
    static constexpr auto name = "printf";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionPrintf>(context); }

    explicit FunctionPrintf(ContextPtr context)
        : function_concat(FunctionFactory::instance().get("concat", context))
    {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto is_native_number_or_string = [](const IDataType & type)
        {
            return isNativeNumber(type) || isStringOrFixedString(type);
        };

        FunctionArgumentDescriptors mandatory_args{{"format", &isString, nullptr, "String"}};
        FunctionArgumentDescriptor variadic_args{"sub", is_native_number_or_string, nullptr, "Native number or String"};

        validateFunctionArgumentsWithVariadics(*this, arguments, mandatory_args, variadic_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & c0 = arguments[0].column;
        const ColumnConst * c0_const_string = typeid_cast<const ColumnConst *>(&*c0);

        /// Fast path: constant format string (original behavior)
        if (c0_const_string)
            return executeConstFormat(c0_const_string->getValue<String>(), arguments, input_rows_count);

        /// Slow path: dynamic (per-row) format string
        return executeDynamicFormat(arguments, input_rows_count);
    }

private:
    ColumnPtr executeConstFormat(const String & format, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        auto instructions = buildInstructions(format, arguments, input_rows_count);

        ColumnsWithTypeAndName concat_args(instructions.size());
        for (size_t i = 0; i < instructions.size(); ++i)
        {
            const auto & instruction = instructions[i];
            try
            {
                concat_args[i] = instruction.execute();
            }
            catch (const fmt::v12::format_error & e)
            {
                if (instruction.is_literal)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Bad format {} in function {} without input argument, reason: {}",
                        instruction.format,
                        getName(),
                        e.what());
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Bad format {} in function {} with {} as input argument, reason: {}",
                    instructions[i].format,
                    getName(),
                    instruction.input.dumpStructure(),
                    e.what());
            }
        }

        auto res = function_concat->build(concat_args)->execute(concat_args, std::make_shared<DataTypeString>(), input_rows_count, /* dry_run = */ false);
        return res;
    }

    /// Per-row format string execution: parse and execute format for each row individually.
    ColumnPtr executeDynamicFormat(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const ColumnPtr & format_col = arguments[0].column->convertToFullColumnIfConst();
        const auto * format_string_col = checkAndGetColumn<ColumnString>(format_col.get());
        if (!format_string_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a String column", getName());

        auto result_col = ColumnString::create();
        auto & result_chars = result_col->getChars();
        auto & result_offsets = result_col->getOffsets();
        result_offsets.resize(input_rows_count);

        size_t curr_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            String format = format_string_col->getDataAt(row).toString();

            /// Build single-row argument columns for this row
            ColumnsWithTypeAndName row_args(arguments.size());
            row_args[0] = {ColumnConst::create(ColumnString::create(1, format), 1), arguments[0].type, arguments[0].name};
            for (size_t i = 1; i < arguments.size(); ++i)
            {
                auto single_val_col = arguments[i].column->cut(row, 1);
                row_args[i] = {std::move(single_val_col), arguments[i].type, arguments[i].name};
            }

            try
            {
                auto instructions = buildInstructions(format, row_args, 1);
                ColumnsWithTypeAndName concat_args(instructions.size());
                for (size_t j = 0; j < instructions.size(); ++j)
                    concat_args[j] = instructions[j].execute();

                auto row_result = function_concat->build(concat_args)->execute(
                    concat_args, std::make_shared<DataTypeString>(), 1, false);

                StringRef val = row_result->getDataAt(0);
                result_chars.resize(curr_offset + val.size + 1);
                if (val.size)
                    memcpy(&result_chars[curr_offset], val.data, val.size);
                result_chars[curr_offset + val.size] = '\0';
                curr_offset += val.size + 1;
            }
            catch (const fmt::v12::format_error & e)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Bad format '{}' in function {} at row {}, reason: {}",
                    format,
                    getName(),
                    row,
                    e.what());
            }

            result_offsets[row] = curr_offset;
        }

        return result_col;
    }

    std::vector<Instruction>
    buildInstructions(const String & format, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        std::vector<Instruction> instructions;
        instructions.reserve(arguments.size());

        auto append_instruction = [&](const char * begin, const char * end, const ColumnWithTypeAndName & arg)
        {
            Instruction instr;
            instr.rows = input_rows_count;
            instr.format = std::string_view(begin, end - begin);

            size_t size = end - begin;
            if (size > 1 && begin[0] == '%' and begin[1] != '%')
            {
                instr.is_literal = false;
                instr.input = arg;
            }
            else
            {
                instr.is_literal = true;
            }
            instructions.emplace_back(std::move(instr));
        };

        auto check_index_range = [&](size_t idx)
        {
            if (idx >= arguments.size())
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, but format is {}",
                    getName(),
                    arguments.size(),
                    format);
        };

        const char * begin = format.data();
        const char * end = format.data() + format.size();
        const char * curr = begin;
        size_t idx = 0;
        while (curr < end)
        {
            const char * tmp = curr;
            bool is_first = curr == begin; /// If current instruction is the first one
            bool is_literal = false; /// If current instruction is literal string without any argument
            if (is_first)
            {
                if (*curr != '%')
                    is_literal = true;
                else if (curr + 1 < end && *(curr + 1) == '%')
                    is_literal = true;
                else
                    ++idx; /// Skip first argument if first instruction is not literal
            }

            if (!is_literal)
                ++curr;

            while (curr < end)
            {
                if (*curr != '%')
                    ++curr;
                else if (curr + 1 < end && *(curr + 1) == '%')
                    curr += 2;
                else
                {
                    check_index_range(idx);
                    append_instruction(tmp, curr, arguments[idx]);
                    ++idx;
                    break;
                }
            }

            if (curr == end)
            {
                check_index_range(idx);
                append_instruction(tmp, curr, arguments[idx]);
                ++idx;
            }
        }

        /// Check if all arguments are used
        if (idx != arguments.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, but format is {}",
                getName(),
                arguments.size(),
                format);

        return instructions;
    }
};

}

REGISTER_FUNCTION(Printf)
{
    FunctionDocumentation::Description description = R"(
The `printf` function formats the given string with the values (strings, integers, floating-points etc.) listed in the arguments, similar to printf function in C++.
The format string can contain format specifiers starting with `%` character.
Anything not contained in `%` and the following format specifier is considered literal text and copied verbatim into the output.
Literal `%` character can be escaped by `%%`.
The format string can be either a constant or a column expression, allowing different format patterns per row.
)";
    FunctionDocumentation::Syntax syntax = "printf(format[, sub1, sub2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"format", "The format string with `%` specifiers.", {"String"}},
        {"sub1, sub2, ...", "Optional. Zero or more values to substitute into the format string.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a formatted string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "C++-style formatting",
        "SELECT printf('%%%s %s %d', 'Hello', 'World', 2024);",
        R"(
┌─printf('%%%s %s %d', 'Hello', 'World', 2024)─┐
│ %Hello World 2024                            │
└──────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringReplacement;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrintf>(documentation);
}

}
