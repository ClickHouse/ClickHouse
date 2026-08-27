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

    static void findCutURL(const ColumnString::Chars & data, String pattern, size_t prev_offset, size_t cur_offset, size_t & cut_begin, size_t & cut_end)
    {
        pattern += '=';
        const char * param_str = pattern.data();
        size_t param_len = pattern.size();

        const char * url_begin = reinterpret_cast<const char *>(&data[prev_offset]);
        const char * url_end = reinterpret_cast<const char *>(&data[cur_offset]);
        const char * begin_pos = url_begin;
        const char * end_pos = begin_pos;

        const char * query_string_begin = find_first_symbols<'?', '#'>(url_begin, url_end);
        const char * search_pos = query_string_begin + 1;
        while (search_pos < url_end)
        {
            const char * pos = static_cast<const char *>(memmem(search_pos, url_end - search_pos, param_str, param_len));
            if (pos == nullptr)
                break;

            char prev_char = pos[-1];
            if (prev_char != '?' && prev_char != '#' && prev_char != '&')
            {
                search_pos = pos + param_len;
                continue;
            }

            begin_pos = pos;
            end_pos = begin_pos + param_len;

            /// Skip the value.
            end_pos = find_first_symbols<'&', '#'>(end_pos, url_end);

            /// Capture '&' before or after the parameter.
            if (end_pos < url_end && *end_pos == '&')
                ++end_pos;
            else if (prev_char == '&')
                --begin_pos;

            break;
        }

        cut_end = end_pos - url_begin;
        cut_begin = begin_pos - url_begin;
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
        PODArrayWithStackMemory<std::pair<size_t, size_t>, 16 * sizeof(size_t)> cuts;

        size_t prev_offset = 0;
        size_t cur_offset;
        size_t cur_len;
        size_t res_offset = 0;
        size_t cur_res_offset;
        size_t cut_begin;
        size_t cut_end;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            cur_offset = offsets[i];
            cur_len = cur_offset - prev_offset;

            cuts.clear();

            if (col_needle_const_array)
            {
                size_t num_needles = col_needle_const_array->getData().size();
                for (size_t j = 0; j < num_needles; ++j)
                {
                    auto field = col_needle_const_array->getData()[j];
                    findCutURL(data, field.safeGet<String>(), prev_offset, cur_offset, cut_begin, cut_end);
                    if (cut_end > cut_begin)
                        cuts.emplace_back(cut_begin, cut_end);
                }

                std::sort(cuts.begin(), cuts.end());
            }
            else
            {
                findCutURL(data, col_needle->getValue<String>(), prev_offset, cur_offset, cut_begin, cut_end);
                if (cut_end > cut_begin)
                    cuts.emplace_back(cut_begin, cut_end);
            }

            cuts.emplace_back(cur_len, cur_len); // Sentinel value to simplify the loop below.

            cur_res_offset = res_offset;
            size_t prev_cut_end = 0;
            for (const auto & [begin, end] : cuts)
            {
                size_t copy_size = std::max(begin, prev_cut_end) - prev_cut_end;

                if (copy_size > 0)
                {
                    res_data.resize(cur_res_offset + copy_size);
                    memcpySmallAllowReadWriteOverflow15(&res_data[cur_res_offset], &data[prev_offset + prev_cut_end], copy_size);
                    cur_res_offset += copy_size;
                }

                prev_cut_end = std::max(end, prev_cut_end);
            }

            res_offsets[i] = cur_res_offset;
            res_offset = cur_res_offset;
            prev_offset = cur_offset;
        }
    }
};

REGISTER_FUNCTION(CutURLParameter)
{
    /// cutURLParameter documentation
    FunctionDocumentation::Description description_cutURLParameter = R"(
Removes the `name` parameter from a URL, if present.
This function does not encode or decode characters in parameter names, e.g. `Client ID` and `Client%20ID` are treated as different parameter names.
    )";
    FunctionDocumentation::Syntax syntax_cutURLParameter = "cutURLParameter(url, name)";
    FunctionDocumentation::Arguments arguments_cutURLParameter = {
        {"url", "URL.", {"String"}},
        {"name", "Name of URL parameter.", {"String", "Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cutURLParameter = {"URL with `name` URL parameter removed.", {"String"}};
    FunctionDocumentation::Examples examples_cutURLParameter = {
    {
        "Usage example",
        R"(
SELECT
    cutURLParameter('http://bigmir.net/?a=b&c=d&e=f#g', 'a') AS url_without_a,
    cutURLParameter('http://bigmir.net/?a=b&c=d&e=f#g', ['c', 'e']) AS url_without_c_and_e;
        )",
        R"(
┌─url_without_a────────────────┬─url_without_c_and_e──────┐
│ http://bigmir.net/?c=d&e=f#g │ http://bigmir.net/?a=b#g │
└──────────────────────────────┴──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cutURLParameter = {1, 1};
    FunctionDocumentation::Category category_cutURLParameter = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_cutURLParameter = {description_cutURLParameter, syntax_cutURLParameter, arguments_cutURLParameter, {}, returned_value_cutURLParameter, examples_cutURLParameter, introduced_in_cutURLParameter, category_cutURLParameter};

    factory.registerFunction<FunctionCutURLParameter>(documentation_cutURLParameter);
}

}
