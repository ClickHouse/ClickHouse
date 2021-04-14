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
struct TextClassificationImpl
{

    using ResultType = String;
    using CodePoint = UInt8;
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
        for (auto & el : model) {
            if (standart[el.first] != 0) {
                res += el.second * log(standart[el.first]);
            } else {
                res += el.second * log(0.000001);
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
        NgramCount * ngram_stats,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        NgramCount * ngram_storage)
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
                if (ngram_stats[hash] == 0) {
                    ngram_storage[len] = hash;
                    ++len;
                }
                ++ngram_stats[hash];
            }
            i = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        return len;
    }

    
    static void constant(String data, String & res)
    {
        static std::unordered_map<String, Float64> emotional_dict = FrequencyHolder::getInstance().getEmotionalDict();
        static std::unordered_map<String, std::unordered_map<UInt16, Float64>> encodings_freq = FrequencyHolder::getInstance().getEncodingsFrequency();
            
        std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}}; // frequency of N-grams 
        std::unique_ptr<NgramCount[]> ngram_storage{new NgramCount[map_size]{}}; // list of N-grams
            
        size_t len = calculateStats(data.data(), data.size(), common_stats.get(), readCodePoints, ngram_storage.get()); // count of N-grams
        String ans;
        // Float64 count_bigram = data.size() - 1;
        std::unordered_map<UInt16, Float64> model;
        for (size_t i = 0; i < len; ++i) {
            ans += std::to_string(ngram_storage.get()[i]) + " " + std::to_string(static_cast<Float64>(common_stats.get()[ngram_storage.get()[i]])) + "\n";
            model[ngram_storage.get()[i]] = static_cast<Float64>(common_stats.get()[ngram_storage.get()[i]]);
        }

        for (const auto& item : encodings_freq) {
            ans += item.first + " " + std::to_string(Naive_bayes(item.second, model)) + "\n";
        }
        res = ans;
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

            std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}}; // frequency of N-grams 
            std::unique_ptr<NgramCount[]> ngram_storage{new NgramCount[map_size]{}}; // list of N-grams

            size_t len = calculateStats(str.data(), str.size(), common_stats.get(), readCodePoints, ngram_storage.get()); // count of N-grams
            // Float64 count_bigram = data.size() - 1;
            std::unordered_map<UInt16, Float64> model;

            for (size_t j = 0; j < len; ++j)
            {
                model[ngram_storage.get()[j]] = static_cast<Float64>(common_stats.get()[ngram_storage.get()[j]]);
            }

            std::vector<std::pair<std::string, Float64>> results;

            for (const auto& item : encodings_freq)
            {
                results.push_back(std::make_pair(item.first, Naive_bayes(item.second, model)));
            }

            std::sort(results.begin(), results.end(), [](auto &left, auto &right)
            {
                return left.second > right.second;
            });

            for (size_t ind = 0; ind < 3; ++ind) {
                prom += results[ind].first + " result=" + std::to_string(results[ind].second) + "\n"; 
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


using FunctionCharsetDetect = FunctionsTextClassification<TextClassificationImpl<2>, NameCharsetDetect>;

void registerFunctionsTextClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCharsetDetect>();
}

}
