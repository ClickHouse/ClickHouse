#include <Common/FrequencyHolder.h>
#include <Common/StringUtils/StringUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/**
  * Determines the sentiment of text data.
  * Uses a marked-up sentiment dictionary, each word has a tonality ranging from -12 to 6.
  * For each text, calculate the average sentiment value of its words and return NEG, POS or NEUT
  */
struct TonalityClassificationImpl
{
    static Float32 detectTonality(const UInt8 * str, const size_t str_len, const FrequencyHolder::Map & emotional_dict)
    {
        Float64 weight = 0;
        UInt64 count_words = 0;

        String word;
        /// Select all Russian words from the string
        for (size_t ind = 0; ind < str_len;)
        {
            /// Assume that all non-ASCII characters are Russian letters
            if (!isASCII(str[ind]))
            {
                word.push_back(str[ind]);
                ++ind;

                while ((ind < str_len) && (!isASCII(str[ind])))
                {
                    word.push_back(str[ind]);
                    ++ind;
                }
                /// Try to find a russian word in the tonality dictionary
                const auto * it = emotional_dict.find(word);
                if (it != emotional_dict.end())
                {
                    count_words += 1;
                    weight += it->getMapped();
                }
                word.clear();
            }
            else
            {
                ++ind;
            }
        }
        /// Calculate average value of tonality.
        /// Convert values -12..6 to -1..1
        return std::max(weight / count_words / 6, -1.0);
    }

    /// If the function will return constant value for FixedString data type.
    static constexpr auto is_fixed_to_constant = false;

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<Float32> & res)
    {
        const auto & emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();

        size_t size = offsets.size();
        size_t prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = detectTonality(data.data() + prev_offset, offsets[i] - 1 - prev_offset, emotional_dict);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t /*n*/, Float32 & /*res*/) {}

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<Float32> & res)
    {
        const auto & emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();

        size_t size = data.size() / n;
        for (size_t i = 0; i < size; ++i)
            res[i] = detectTonality(data.data() + i * n, n, emotional_dict);
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<Float32> &)
    {
        throw Exception("Cannot apply function detectTonality to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<Float32> &)
    {
        throw Exception("Cannot apply function detectTonality to UUID argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

struct NameDetectTonality
{
    static constexpr auto name = "detectTonality";
};

using FunctionDetectTonality = FunctionStringOrArrayToT<TonalityClassificationImpl, NameDetectTonality, Float32>;

void registerFunctionsTonalityClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDetectTonality>();
}

}
