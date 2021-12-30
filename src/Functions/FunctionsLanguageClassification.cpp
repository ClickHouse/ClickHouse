#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_NLP

#include <Functions/FunctionsTextClassification.h>
#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include "compact_lang_det.h"

namespace DB
{
/* Determine language of Unicode UTF-8 text.
 * Uses the cld2 library https://github.com/CLD2Owners/cld2
 */

struct LanguageClassificationImpl
{
    using ResultType = String;

    static String codeISO(std::string_view code_string)
    {
        if (code_string.ends_with("-Latn"))
            code_string.remove_suffix(code_string.size() - 5);

        if (code_string.ends_with("-Hant"))
            code_string.remove_suffix(code_string.size() - 5);

        // Old deprecated codes
        if (code_string == "iw")
            return "he";

        if (code_string == "jw")
            return "jv";

        if (code_string == "in")
            return "id";

        if (code_string == "mo")
            return "ro";

        // Some languages do not have 2 letter codes, for example code for Cebuano is ceb
        if (code_string.size() != 2)
            return "other";

        return String(code_string);
    }

    static void constant(const String & data, String & res)
    {
        bool is_reliable = true;
        const char * str = data.c_str();
        auto lang = CLD2::DetectLanguage(str, strlen(str), true, &is_reliable);
        res = codeISO(LanguageCode(lang));
    }


    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(1024);
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * str = reinterpret_cast<const char *>(&data[prev_offset]);
            String ans;
            bool is_reliable = true;

            auto lang = CLD2::DetectLanguage(str, strlen(str), true, &is_reliable);
            ans = codeISO(LanguageCode(lang));

            size_t cur_offset = offsets[i];

            res_data.resize(res_offset + ans.size() + 1);
            memcpy(&res_data[res_offset], ans.data(), ans.size());
            res_offset += ans.size();

            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
            prev_offset = cur_offset;
        }
    }


};

class LanguageClassificationMixedDetect : public IFunction
{
public:
    static constexpr auto name = "detectLanguageMixed";

    static FunctionPtr create(ContextPtr) { return std::make_shared<LanguageClassificationMixedDetect>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & column = arguments[0].column;
        const ColumnString * col = checkAndGetColumn<ColumnString>(column.get());

        if (!col)
            throw Exception(
                "Illegal columns " + arguments[0].column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        auto & input_data = col->getChars();
        auto & input_offsets = col->getOffsets();

        /// Create and fill the result map.

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        size_t total_elements = input_rows_count * 3;
        keys_data->reserve(total_elements);
        values_data->reserve(total_elements);
        offsets->reserve(input_rows_count);

        bool is_reliable = true;
        CLD2::Language result_lang_top3[3];
        int32_t pc[3];
        int bytes[3];

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * str = reinterpret_cast<const char *>(input_data.data() + input_offsets[i - 1]);
            const size_t str_len = input_offsets[i] - input_offsets[i - 1] - 1;

            CLD2::DetectLanguageSummary(str, str_len, true, result_lang_top3, pc, bytes, &is_reliable);

            for (size_t j = 0; j < 3; ++j)
            {
                auto res_str = LanguageClassificationImpl::codeISO(LanguageCode(result_lang_top3[j]));
                int32_t res_int = static_cast<int>(pc[j]);

                keys_data->insertData(res_str.data(), res_str.size());
                values_data->insertData(reinterpret_cast<const char *>(&res_int), sizeof(res_int));
            }

            current_offset += 3;
            offsets->insert(current_offset);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};

struct NameLanguageUTF8Detect
{
    static constexpr auto name = "detectLanguage";
};


using FunctionLanguageUTF8Detect = FunctionsTextClassification<LanguageClassificationImpl, NameLanguageUTF8Detect>;

void registerFunctionLanguageDetectUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLanguageUTF8Detect>();
    factory.registerFunction<LanguageClassificationMixedDetect>();
}

}
#endif
