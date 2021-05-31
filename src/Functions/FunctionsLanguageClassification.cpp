#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_CLD2

#include <Functions/FunctionsTextClassification.h>
#include <Functions/FunctionFactory.h>

#include "compact_lang_det.h"

namespace DB
{
/* Determine language of Unicode UTF-8 text.
 * Uses the cld2 library https://github.com/CLD2Owners/cld2
 */
template <bool mixed>
struct LanguageClassificationImpl
{

    using ResultType = String;


    static void constant(String data, String & res)
    {
        bool is_UTF8 = true;
        const char * str = data.c_str();
        if (!mixed)
        {
            String ans(LanguageName(CLD2::DetectLanguage(str, strlen(str), true, &is_UTF8)));
            res = ans;
        }
        else
        {
            CLD2::Language result_lang_top3[3];
            int pc[3];
            int bytes[3];
            CLD2::DetectLanguageSummary(str, strlen(str), true, result_lang_top3, pc, bytes, &is_UTF8);
            String lang1(LanguageName(result_lang_top3[0]));
            String lang2(LanguageName(result_lang_top3[1]));
            String lang3(LanguageName(result_lang_top3[2]));
            res = lang1 + " " + std::to_string(pc[0]) + "% | ";
            res += lang2 + " " + std::to_string(pc[1]) + "% | ";
            res += lang3 + " " + std::to_string(pc[2]) + "%";
        }
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
            const char * ans;
            bool is_UTF8 = true;
            if (!mixed)
            {
                ans = LanguageName(CLD2::DetectLanguage(str, strlen(str), true, &is_UTF8));
            }
            else
            {
                String top3;
                CLD2::Language result_lang_top3[3];
                int pc[3];
                int bytes[3];
                CLD2::DetectLanguageSummary(str, strlen(str), true, result_lang_top3, pc, bytes, &is_UTF8);

                String lang1(LanguageName(result_lang_top3[0]));
                String lang2(LanguageName(result_lang_top3[1]));
                String lang3(LanguageName(result_lang_top3[2]));
                top3 = lang1 + " " + std::to_string(pc[0]) + "% | ";
                top3 += lang2 + " " + std::to_string(pc[1]) + "% | ";
                top3 += lang3 + " " + std::to_string(pc[2]) + "%";
                ans = top3.c_str();
            }
            size_t cur_offset = offsets[i];

            size_t ans_size = strlen(ans);
            res_data.resize(res_offset + ans_size + 1);
            memcpy(&res_data[res_offset], ans, ans_size);
            res_offset += ans_size;

            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
            prev_offset = cur_offset;
        }
    }


};


struct NameLanguageUTF8Detect
{
    static constexpr auto name = "detectLanguageUTF8";
};

struct NameLanguageMixedUTF8Detect
{
    static constexpr auto name = "detectLanguageMixedUTF8";
};


using FunctionLanguageUTF8Detect = FunctionsTextClassification<LanguageClassificationImpl<false>, NameLanguageUTF8Detect>;
using FunctionLanguageMixedUTF8Detect = FunctionsTextClassification<LanguageClassificationImpl<true>, NameLanguageMixedUTF8Detect>;

void registerFunctionLanguageDetectUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLanguageUTF8Detect>();
    factory.registerFunction<FunctionLanguageMixedUTF8Detect>();
}

}
#endif
