#include <Functions/FunctionsTextClassification.h>
#include <Common/FrequencyHolder.h>
#include <Functions/FunctionFactory.h>
#include <Common/UTF8Helpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <algorithm>
#include <cstring>
#include <cmath>
#include <limits>
#include <unordered_map>
#include <memory>
#include <utility>
#include <sstream>
#include <set>

namespace DB
{


template <size_t N>
struct CharsetClassificationImpl
{

    using ResultType = String;
    using CodePoint = UInt8;

    static constexpr Float64 zero_frequency = 0.000001;
    /// map_size for ngram count.
    static constexpr size_t map_size = 1u << 16;

    /// If the data size is bigger than this, behaviour is unspecified for this function.
    static constexpr size_t max_string_size = 1u << 15;

    /// Default padding to read safely.
    static constexpr size_t default_padding = 16;

    /// Max codepoints to store at once. 16 is for batching usage and PODArray has this padding.
    static constexpr size_t simultaneously_codepoints_num = default_padding + N - 1;

    /** map_size of this fits mostly in L2 cache all the time.
      * Actually use UInt16 as addings and subtractions do not UB overflow. But think of it as a signed
      * integer array.
      */
    using NgramCount = UInt16;

    static ALWAYS_INLINE inline Float64 Naive_bayes(std::unordered_map<UInt16, Float64> standart, std::unordered_map<UInt16, Float64> model)
    {
        Float64 res = 0;
        for (auto & el : model)
        {
            if (standart[el.first] != 0)
            {
                res += el.second * log(standart[el.first]);
            } else
            {
                res += el.second * log(zero_frequency);
            }
        }
        return res;
    }



    static ALWAYS_INLINE size_t readCodePoints(CodePoint * code_points, const char *& pos, const char * end)
    {
        constexpr size_t padding_offset = default_padding - N + 1;
        memcpy(code_points, code_points + padding_offset, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(CodePoint));
        memcpy(code_points + (N - 1), pos, default_padding * sizeof(CodePoint));
        pos += padding_offset;
        if (pos > end)
            return default_padding - (pos - end);
        return default_padding;
    }


    static ALWAYS_INLINE inline size_t calculateStats(
        const char * data,
        const size_t size,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        std::unordered_map<UInt16, Float64>& model)
    {

        const char * start = data;
        const char * end = data + size;
        CodePoint cp[simultaneously_codepoints_num] = {};
        /// read_code_points returns the position of cp where it stopped reading codepoints.
        size_t found = read_code_points(cp, start, end);
        /// We need to start for the first time here, because first N - 1 codepoints mean nothing.
        size_t i = N - 1;
        size_t len = 0;
        do
        {
            for (; i + N <= found; ++i)
            {
                UInt32 hash = 0;
                for (size_t j = 0; j < N; ++j) {
                    hash <<= 8;
                    hash += *(cp + i + j);
                }
                if (model[hash] == 0) {
                    model[hash] = 1;
                    ++len;
                }
                ++model[hash];
            }
            i = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        return len;
    }

    
    static void constant(String data, String & res)
    {
        static std::unordered_map<String, Float64> emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();
        static std::unordered_map<String, std::unordered_map<UInt16, Float64>> encodings_freq = FrequencyHolder::getInstance().getEncodingsFrequency();

        std::unordered_map<UInt16, Float64> model;
        calculateStats(data.data(), data.size(), readCodePoints, model);

        Float64 max_result = log(zero_frequency) * model.size();
        res = "Undefined";
        for (const auto& item : encodings_freq)
        {
            const Float64 freq_pr = Naive_bayes(item.second, model);
            if (max_result > freq_pr)
            {
                res = item.first;
                max_result = freq_pr;
            }
        }
    }


    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        static std::unordered_map<String, std::unordered_map<UInt16, Float64>> encodings_freq = FrequencyHolder::getInstance().getEncodingsFrequency();
        static std::unordered_map<String, Float64> emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();

        res_data.reserve(1024);
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * haystack = reinterpret_cast<const char *>(&data[prev_offset]);
            String str = haystack;

            String prom;

            std::unordered_map<UInt16, Float64> model;
            calculateStats(str.data(), str.size(), readCodePoints, model);

            Float64 max_result = log(zero_frequency) * model.size();

            prom = "Undefined";
            for (const auto& item : encodings_freq)
            {
                const Float64 freq_pr = Naive_bayes(item.second, model);
                if (max_result > freq_pr)
                {
                    prom = item.first;
                    max_result = freq_pr;
                }
            }
            
            const auto ans = prom.c_str();
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


struct NameCharsetDetect
{
    static constexpr auto name = "charsetDetect";
};


using FunctionCharsetDetect = FunctionsTextClassification<CharsetClassificationImpl<2>, NameCharsetDetect>;

void registerFunctionsCharsetClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCharsetDetect>();
}

}
