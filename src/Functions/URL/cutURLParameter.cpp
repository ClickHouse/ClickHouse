#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        if (!isString(arguments[1]) && !isArray(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                 arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnPtr column_needle = arguments[1].column;

        const ColumnConst * col_needle = typeid_cast<const ColumnConst *>(&*column_needle);
        const ColumnArray * col_needle_const_array = checkAndGetColumnConstData<ColumnArray>(column_needle.get());

        if (!col_needle && !col_needle_const_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Second argument of function {} must be constant string or constant array",
                getName());

        if (col_needle_const_array)
        {
            if (!col_needle_const_array->getData().empty() && typeid_cast<const DataTypeArray &>(*arguments[1].type).getNestedType()->getTypeId() != TypeIndex::String)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Second argument of function {} must be constant array of strings",
                    getName());
        }

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vector(col->getChars(), col->getOffsets(), col_needle, col_needle_const_array, vec_res, offsets_res, input_rows_count);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

    static void cutURL(ColumnString::Chars & data, String pattern, size_t prev_offset, size_t & cur_offset)
    {
        pattern += '=';
        const char * param_str = pattern.c_str();
        size_t param_len = pattern.size();

        const char * url_begin = reinterpret_cast<const char *>(&data[prev_offset]);
        const char * url_end = reinterpret_cast<const char *>(&data[cur_offset - 2]);
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

        size_t cut_length = end_pos - begin_pos;
        cur_offset -= cut_length;
        data.erase(data.begin() + prev_offset + (begin_pos - url_begin), data.begin() + prev_offset+  (end_pos - url_begin));
    }

    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const ColumnConst * col_needle,
        const ColumnArray * col_needle_const_array,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t cur_offset;
        size_t cur_len;
        size_t res_offset = 0;
        size_t cur_res_offset;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            cur_offset = offsets[i];
            cur_len = cur_offset - prev_offset;
            cur_res_offset = res_offset + cur_len;
            res_data.resize(cur_res_offset);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset], cur_len);

            if (col_needle_const_array)
            {
                size_t num_needles = col_needle_const_array->getData().size();
                for (size_t j = 0; j < num_needles; ++j)
                {
                    auto field = col_needle_const_array->getData()[j];
                    cutURL(res_data, field.safeGet<String>(), res_offset, cur_res_offset);
                }
            }
            else
            {
                cutURL(res_data, col_needle->getValue<String>(), res_offset, cur_res_offset);
            }
            res_offsets[i] = cur_res_offset;
            res_offset = cur_res_offset;
            prev_offset = cur_offset;
        }
    }
};

REGISTER_FUNCTION(CutURLParameter)
{
    factory.registerFunction<FunctionCutURLParameter>();
}

}
