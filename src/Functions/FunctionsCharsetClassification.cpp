#include <Common/FrequencyHolder.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTextClassification.h>

#include <memory>
#include <unordered_map>

namespace DB
{

/* Determine language and charset of text data. For each text, we build the distribution of bigrams bytes.
 * Then we use marked-up dictionaries with distributions of bigram bytes of various languages ​​and charsets.
 * Using a naive Bayesian classifier, find the most likely charset and language and return it
 */

template <bool detect_language>
struct CharsetClassificationImpl
{
    /* We need to solve zero-frequency problem for Naive Bayes Classifier
     * If the bigram is not found in the text, we assume that the probability of its meeting is 1e-06.
     * 1e-06 is minimal value in our marked-up dictionary.
     */
    static constexpr Float64 zero_frequency = 1e-06;

    /// If the data size is bigger than this, behaviour is unspecified for this function.
    static constexpr size_t max_string_size = 1u << 15;

    static ALWAYS_INLINE inline Float64 naiveBayes(
        const FrequencyHolder::EncodingMap & standard,
        const HashMap<UInt16, UInt64> & model,
        Float64 max_result)
    {
        Float64 res = 0;
        for (const auto & el : model)
        {
            /// Try to find bigram in the dictionary.
            const auto * it = standard.find(el.getKey());
            if (it != standard.end())
            {
                res += el.getMapped() * log(it->getMapped());
            } else
            {
                res += el.getMapped() * log(zero_frequency);
            }
            /// If at some step the result has become less than the current maximum, then it makes no sense to count it fully.
            if (res < max_result)
            {
                return res;
            }
        }
        return res;
    }

    /// Сount how many times each bigram occurs in the text.
    static ALWAYS_INLINE inline void calculateStats(
        const UInt8 * data,
        const size_t size,
        HashMap<UInt16, UInt64> & model)
    {
        UInt16 hash = 0;
        for (size_t i = 0; i < size; ++i)
        {
            hash <<= 8;
            hash += *(data + i);
            ++model[hash];
        }
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const auto & encodings_freq = FrequencyHolder::getInstance().getEncodingsFrequency();

        if (detect_language)
            /// 2 chars for ISO code + 1 zero byte
            res_data.reserve(offsets.size() * 3);
        else
            /// Mean charset length is 8
            res_data.reserve(offsets.size() * 8);

        res_offsets.resize(offsets.size());

        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * str = data.data() + offsets[i - 1];
            const size_t str_len = offsets[i] - offsets[i - 1] - 1;

            std::string_view res;

            HashMap<UInt16, UInt64> model;
            calculateStats(str, str_len, model);

            /// Go through the dictionary and find the charset with the highest weight
            Float64 max_result = log(zero_frequency) * (max_string_size);
            for (const auto & item : encodings_freq)
            {
                Float64 score = naiveBayes(item.map, model, max_result);
                if (max_result < score)
                {
                    max_result = score;
                    res = detect_language ? item.lang : item.name;
                }
            }

            res_data.resize(res_offset + res.size() + 1);
            memcpy(&res_data[res_offset], res.data(), res.size());

            res_data[res_offset + res.size()] = 0;
            res_offset += res.size() + 1;

            res_offsets[i] = res_offset;
        }
    }
};


struct NameDetectCharset
{
    static constexpr auto name = "detectCharset";
};

struct NameDetectLanguageUnknown
{
    static constexpr auto name = "detectLanguageUnknown";
};


using FunctionDetectCharset = FunctionTextClassificationString<CharsetClassificationImpl<false>, NameDetectCharset>;
using FunctionDetectLanguageUnknown = FunctionTextClassificationString<CharsetClassificationImpl<true>, NameDetectLanguageUnknown>;

void registerFunctionDetectCharset(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDetectCharset>();
    factory.registerFunction<FunctionDetectLanguageUnknown>();
}

}
