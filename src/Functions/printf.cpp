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
#include <fmt/format.h>
#include <fmt/printf.h>
#include <sstream>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionPrintf : public IFunction
{
private:
    ContextPtr context;
    FunctionOverloadResolverPtr function_concat;

    struct Instruction
    {
        std::string_view format;
        size_t rows;
        bool is_literal; /// format is literal string without any argument
        ColumnWithTypeAndName input; /// Only used when is_literal is false

        ColumnWithTypeAndName execute()
        {
            if (is_literal)
                return executeLiteral(format);
            else if (isColumnConst(*input.column))
                return executeConstant(input);
            else
                return executeNonconstant(input);
        }

        String toString() const
        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            oss << "format:" << format << ", rows:" << rows << ", is_literal:" << is_literal << ", input:" << input.dumpStructure()
                      << std::endl;
            return oss.str();
        }

    private:
        ColumnWithTypeAndName executeLiteral(std::string_view literal)
        {
            ColumnWithTypeAndName res;
            auto str_col = ColumnString::create();
            str_col->insert(fmt::sprintf(literal));
            res.column = ColumnConst::create(std::move(str_col), rows);
            res.type = std::make_shared<DataTypeString>();
            return res;
        }

        ColumnWithTypeAndName executeConstant(const ColumnWithTypeAndName & arg)
        {
            ColumnWithTypeAndName tmp_arg = arg;
            const auto & const_col = static_cast<const ColumnConst &>(*arg.column);
            tmp_arg.column = const_col.getDataColumnPtr();

            ColumnWithTypeAndName tmp_res = executeNonconstant(tmp_arg);
            return ColumnWithTypeAndName{ColumnConst::create(tmp_res.column, arg.column->size()), tmp_res.type, tmp_res.name};
        }

        ColumnWithTypeAndName executeNonconstant(const ColumnWithTypeAndName & arg)
        {
            size_t size = arg.column->size();
            auto res_col = ColumnString::create();
            auto & res_str = static_cast<ColumnString &>(*res_col);
            auto & res_offsets = res_str.getOffsets();
            auto & res_chars = res_str.getChars();
            res_offsets.reserve_exact(size);
            res_chars.reserve(format.size() * size * 2);

            String s;
            WhichDataType which(arg.type);

#define EXECUTE_BY_TYPE(IS_TYPE, GET_TYPE) \
            else if (which.IS_TYPE()) \
            { \
                for (size_t i = 0; i < size; ++i) \
                { \
                    auto a = arg.column->GET_TYPE(i); \
                    s = fmt::sprintf(format, a); \
                    res_str.insertData(s.data(), s.size()); \
                } \
            }

            if (false)
                ;
            EXECUTE_BY_TYPE(isNativeInt, getInt)
            EXECUTE_BY_TYPE(isNativeUInt, getUInt)
            EXECUTE_BY_TYPE(isFloat32, getFloat32)
            EXECUTE_BY_TYPE(isFloat64, getFloat64)
            else if (which.isStringOrFixedString())
            {
                for (size_t i = 0; i < size; ++i)
                {
                    auto a = arg.column->getDataAt(i).toView();
                    s = fmt::sprintf(format, a);
                    res_str.insertData(s.data(), s.size());
                }
            }
            else throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument type of function {} is {}, but native numeric or string type is expected",
                FunctionPrintf::name,
                arg.type->getName());
#undef EXECUTE_BY_TYPE

            ColumnWithTypeAndName res;
            res.name = arg.name;
            res.type = std::make_shared<DataTypeString>();
            res.column = std::move(res_col);
            return res;
        }
    };

public:
    static constexpr auto name = "printf";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionPrintf>(context); }

    explicit FunctionPrintf(ContextPtr context_)
        : context(context_), function_concat(FunctionFactory::instance().get("concat", context)) { }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 1",
                getName(),
                arguments.size());

        /// First pattern argument must have string type
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument type of function {} is {}, but String type is expected",
                getName(),
                arguments[0]->getName());

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (!isNativeNumber(arguments[i]) && !isStringOrFixedString(arguments[i]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The {}-th argument type of function {} is {}, but native numeric or string type is expected",
                    i + 1,
                    getName(),
                    arguments[i]->getName());
        }
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & c0 = arguments[0].column;
        const ColumnConst * c0_const_string = typeid_cast<const ColumnConst *>(&*c0);
        if (!c0_const_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be constant string", getName());

        String format = c0_const_string->getValue<String>();
        auto instructions = buildInstructions(format, arguments, input_rows_count);

        ColumnsWithTypeAndName concat_args(instructions.size());
        for (size_t i = 0; i < instructions.size(); ++i)
        {
            // std::cout << "instruction[" << i << "]:" << instructions[i].toString() << std::endl;
            concat_args[i] = instructions[i].execute();
            // std::cout << "concat_args[" << i << "]:" << concat_args[i].dumpStructure() << std::endl;
        }

        auto res = function_concat->build(concat_args)->execute(concat_args, std::make_shared<DataTypeString>(), input_rows_count);
        return res;
    }

private:
    std::vector<Instruction> buildInstructions(const String & format , const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
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
            bool is_literal = false;       /// If current instruction is literal string without any argument
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
    factory.registerFunction<FunctionPrintf>();
}

}
