#include <Functions/FunctionsTextClassification.h>
#include <Common/FrequencyHolder.h>
#include <Functions/FunctionFactory.h>
#include <Common/UTF8Helpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

//#include <algorithm>
//#include <cstring>
//#include <cmath>
//#include <limits>
#include <unordered_map>
//#include <memory>
//#include <utility>
//#include <sstream>
//#include <set>

namespace DB
{


struct TonalityClassificationImpl
{

    using ResultType = String;

    
    static ALWAYS_INLINE inline void word_processing(String & word)
    {
        std::set<char> to_skip {',', '.', '!', '?', ')', '(', '\"', '\'', '[', ']', '{', '}', ':', ';'};

        while (to_skip.find(word.back()) != to_skip.end())
        {
            word.pop_back();
        }

        while (to_skip.find(word.front()) != to_skip.end())
        {
            word.erase(0, 1);
        }
    }

    static String get_tonality(const Float64 & tonality_level)
    {
        if (tonality_level < 0.5) { return "NEG"; }
        if (tonality_level > 1) { return "POS"; }
        return "NEUT";
    } 
    
    static void constant(String data, String & res)
    {
        static std::unordered_map<String, Float64> emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();

        Float64 freq = 0;
        Float64 count_words = 0;

        String answer;

        ReadBufferFromMemory in(data.data(), data.size() + 1);
        skipWhitespaceIfAny(in);

        String to_check;
        while (!in.eof())
        {
            if (data.size() - (in.position() - data.data()) <= 3) {
                break;
            }
            readStringUntilWhitespace(to_check, in);
            skipWhitespaceIfAny(in);

            word_processing(to_check);
                

            if (emotional_dict.find(to_check) != emotional_dict.cend())
            {
                count_words += 1;
                freq += emotional_dict[to_check];
            }            
        }
        Float64 total_tonality = freq / count_words;
        res = get_tonality(total_tonality);
    }


    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        static std::unordered_map<String, Float64> emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();

        res_data.reserve(1024);
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * haystack = reinterpret_cast<const char *>(&data[prev_offset]);
            String str = haystack;

            String buf;

            Float64 freq = 0;
            Float64 count_words = 0;


            ReadBufferFromMemory in(str.data(), str.size() + 1);

            skipWhitespaceIfAny(in);
            String to_check;
            while (!in.eof())
            {
                if (str.size() - (in.position() - str.data()) <= 3) {
                    break;
                }
                readStringUntilWhitespace(to_check, in);
                skipWhitespaceIfAny(in);

                if (emotional_dict.find(to_check) != emotional_dict.cend())
                {
                    count_words += 1;
                    freq += emotional_dict[to_check];
                }
            }
            Float64 total_tonality = freq / count_words;
            buf = get_tonality(total_tonality);

            const auto ans = buf.c_str();
            size_t cur_offset = offsets[i];

            res_data.resize(res_offset + strlen(ans) + 1);
            memcpy(&res_data[res_offset], ans, strlen(ans));
            res_offset += strlen(ans);

            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
            prev_offset = cur_offset;
        }
    }


};

struct NameGetTonality
{
    static constexpr auto name = "getTonality";
};


using FunctionGetTonality = FunctionsTextClassification<TonalityClassificationImpl, NameGetTonality>;

void registerFunctionsTonalityClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetTonality>();
}

}
