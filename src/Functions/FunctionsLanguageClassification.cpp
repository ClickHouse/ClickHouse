#include "config.h"

#if USE_NLP

#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Common/isValidUTF8.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTextClassification.h>

#include <compact_lang_det.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_nlp_functions;
}

/* Determine language of Unicode UTF-8 text.
 * Uses the cld2 library https://github.com/CLD2Owners/cld2
 */

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int SUPPORT_IS_DISABLED;
}

struct FunctionDetectLanguageImpl
{
    static std::string_view codeISO(std::string_view code_string)
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

        return code_string;
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        /// Constant 3 is based on the fact that in general we need 2 characters for ISO code + 1 zero byte
        res_data.reserve(input_rows_count * 3);
        res_offsets.resize(input_rows_count);

        bool is_reliable;
        size_t res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * str = data.data() + offsets[i - 1];
            const size_t str_len = offsets[i] - offsets[i - 1] - 1;

            std::string_view res;

            if (UTF8::isValidUTF8(str, str_len))
            {
                auto lang = CLD2::DetectLanguage(
                    reinterpret_cast<const char *>(str),
                    static_cast<int>(str_len),
                    true, &is_reliable);
                res = codeISO(LanguageCode(lang));
            }
            else
            {
                res = "un";
            }

            res_data.resize(res_offset + res.size() + 1);
            memcpy(&res_data[res_offset], res.data(), res.size());

            res_data[res_offset + res.size()] = 0;
            res_offset += res.size() + 1;

            res_offsets[i] = res_offset;
        }
    }
};

class FunctionDetectLanguageMixed : public IFunction
{
public:
    static constexpr auto name = "detectLanguageMixed";

    /// Number of top results
    static constexpr auto top_N = 3;

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_nlp_functions])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Natural language processing function '{}' is experimental. "
                            "Set `allow_experimental_nlp_functions` setting to enable it", name);

        return std::make_shared<FunctionDetectLanguageMixed>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Must be String.",
                arguments[0]->getName(), getName());

        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & column = arguments[0].column;
        const ColumnString * col = checkAndGetColumn<ColumnString>(column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns {} of arguments of function {}",
                arguments[0].column->getName(), getName());

        const auto & input_data = col->getChars();
        const auto & input_offsets = col->getOffsets();

        /// Create and fill the result map.

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        size_t total_elements = input_rows_count * top_N;
        keys_data->reserve(total_elements);
        values_data->reserve(total_elements);
        offsets->reserve(input_rows_count);

        bool is_reliable;
        CLD2::Language result_lang_top3[top_N];
        int32_t pc[top_N];
        int bytes[top_N];

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * str = input_data.data() + input_offsets[i - 1];
            const size_t str_len = input_offsets[i] - input_offsets[i - 1] - 1;

            if (UTF8::isValidUTF8(str, str_len))
            {
                CLD2::DetectLanguageSummary(
                    reinterpret_cast<const char *>(str),
                    static_cast<int>(str_len),
                    true, result_lang_top3, pc, bytes, &is_reliable);

                for (size_t j = 0; j < top_N; ++j)
                {
                    if (pc[j] == 0)
                        break;

                    auto res_str = FunctionDetectLanguageImpl::codeISO(LanguageCode(result_lang_top3[j]));
                    Float32 res_float = static_cast<Float32>(pc[j]) / 100;

                    keys_data->insertData(res_str.data(), res_str.size());
                    values_data->insertData(reinterpret_cast<const char *>(&res_float), sizeof(res_float));
                    ++current_offset;
                }
            }
            else
            {
                std::string_view res_str = "un";
                Float32 res_float = 0;

                keys_data->insertData(res_str.data(), res_str.size());
                values_data->insertData(reinterpret_cast<const char *>(&res_float), sizeof(res_float));
                ++current_offset;
            }
            offsets->insert(current_offset);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};

struct NameDetectLanguage
{
    static constexpr auto name = "detectLanguage";
};


using FunctionDetectLanguage = FunctionTextClassificationString<FunctionDetectLanguageImpl, NameDetectLanguage>;

REGISTER_FUNCTION(DetectLanguage)
{
    factory.registerFunction<FunctionDetectLanguage>();
    factory.registerFunction<FunctionDetectLanguageMixed>();
}

}
#endif
