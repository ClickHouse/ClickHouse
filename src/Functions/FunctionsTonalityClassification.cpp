#include <Common/FrequencyHolder.h>

#if USE_NLP

#include <Common/StringUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTextClassification.h>

#include <unordered_map>

namespace DB
{

/**
  * Determines the sentiment of text data.
  * Uses a marked-up sentiment dictionary, each word has a tonality ranging from -12 to 6.
  * For each text, calculate the average sentiment value of its words and return it in range [-1,1]
  */
struct FunctionDetectTonalityImpl
{
    static Float32 detectTonality(
        const UInt8 * str,
        const size_t str_len,
        const FrequencyHolder::Map & emotional_dict)
    {
        Float64 weight = 0;
        UInt64 count_words = 0;

        String word;
        /// Select all words from the string
        for (size_t ind = 0; ind < str_len; ++ind)
        {
            /// Split words by whitespaces and punctuation signs
            if (isWhitespaceASCII(str[ind]) || isPunctuationASCII(str[ind]))
                continue;

            while (ind < str_len && !(isWhitespaceASCII(str[ind]) || isPunctuationASCII(str[ind])))
            {
                word.push_back(str[ind]);
                ++ind;
            }
            /// Try to find a word in the tonality dictionary
            const auto * it = emotional_dict.find(word);
            if (it != emotional_dict.end())
            {
                count_words += 1;
                weight += it->getMapped();
            }
            word.clear();
        }

        if (!count_words)
            return 0;

        /// Calculate average value of tonality.
        /// Convert values -12..6 to -1..1
        if (weight > 0)
            return static_cast<Float32>(weight / count_words / 6);
        return static_cast<Float32>(weight / count_words / 12);
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<Float32> & res,
        size_t input_rows_count)
    {
        const auto & emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();

        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = detectTonality(data.data() + prev_offset, offsets[i] - 1 - prev_offset, emotional_dict);
            prev_offset = offsets[i];
        }
    }
};

struct NameDetectTonality
{
    static constexpr auto name = "detectTonality";
};

using FunctionDetectTonality = FunctionTextClassificationFloat<FunctionDetectTonalityImpl, NameDetectTonality>;

REGISTER_FUNCTION(DetectTonality)
{
    factory.registerFunction<FunctionDetectTonality>();
}

}

#endif
