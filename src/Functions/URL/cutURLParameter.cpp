#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <base/StringRef.h>
#include <Functions/FunctionFactory.h>
#include <base/find_symbols.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionCutURLParameter : public IFunction
{
public:
    static constexpr auto name = "cutURLParameter";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCutURLParameter>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]) && !isArray(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnPtr column_needle = arguments[1].column;
        // assert(isArray(arguments[1].type));

        const ColumnConst * col_needle = typeid_cast<const ColumnConst *>(&*column_needle);
        const ColumnArray * col_const_array = checkAndGetColumnConstData<ColumnArray>(column_needle.get());

        if (!col_needle && !col_const_array)
            throw Exception("Second argument of function " + getName() + " must be constant string or constant array", ErrorCodes::ILLEGAL_COLUMN);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vector(col->getChars(), col->getOffsets(), col_needle, col_const_array, vec_res, offsets_res);
            return col_res;
        }
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    static void cutURL(ColumnString::Chars & data, std::string pattern)
    {
        pattern += '=';
        const char * param_str = pattern.c_str();
        size_t param_len = pattern.size();

        const char * url_begin = reinterpret_cast<const char *>(&data[0]);
        const char * url_end = reinterpret_cast<const char *>(&data[data.size()-1]) - 1;
        const char * begin_pos = url_begin;
        const char * end_pos = begin_pos;

        do
        {
            const char * query_string_begin = find_first_symbols<'?', '#'>(url_begin, url_end);
            if (query_string_begin + 1 >= url_end)
                break;

            const char * pos = static_cast<const char *>(memmem(query_string_begin + 1, url_end - query_string_begin - 1, param_str, param_len));
            if (pos == nullptr)
                break;

            if (pos[-1] != '?' && pos[-1] != '#' && pos[-1] != '&')
            {
                pos = nullptr;
                break;
            }

            begin_pos = pos;
            end_pos = begin_pos + param_len;

            /// Skip the value.
            while (*end_pos && *end_pos != '&' && *end_pos != '#')
                ++end_pos;

            /// Capture '&' before or after the parameter.
            if (*end_pos == '&')
                ++end_pos;
            else if (begin_pos[-1] == '&')
                --begin_pos;
        } while (false);

        data.erase(data.begin() + (begin_pos - url_begin), data.begin() + (end_pos - url_begin));
    }

    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const ColumnConst * col_needle,
        const ColumnArray * col_const_array,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        ColumnString::Chars res_chars;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            res_chars.clear();
            res_chars.insert(res_chars.end(), data.begin()+prev_offset, data.begin()+cur_offset);

            if (!col_const_array)
            {
                cutURL(res_chars, col_needle->getValue<String>());
            }
            else
            {
                size_t data_size = col_const_array->getData().size();
                // assert(data_size>0);

                for (size_t j = 0; j < data_size; ++j)
                {
                    auto field = col_const_array->getData()[j];
                    // auto typename_ = field.getTypeName();
                    // assert(typename_ != "");
                    cutURL(res_chars, field.get<String>());
                }

            }
            res_data.resize(res_offset + res_chars.size() + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &res_chars[0], res_chars.size());
            res_offset += res_chars.size() + 1;
            res_data[res_offset - 1] = 0;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }

};

REGISTER_FUNCTION(CutURLParameter)
{
    factory.registerFunction<FunctionCutURLParameter>();
}

}
