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


template <size_t N, bool Tonality>
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


    static ALWAYS_INLINE inline Float64 L2_distance(std::unordered_map<UInt16, Float64> standart, std::unordered_map<UInt16, Float64> model)
    {
        Float64 res = 0;
        for (auto& el : standart) {
            if (model.find(el.first) != model.end()) {
                res += ((model[el.first] - el.second) * (model[el.first] - el.second));
            }
        }
        return res;
    }


    static ALWAYS_INLINE inline Float64 Naive_bayes(std::unordered_map<UInt16, Float64> standart, std::unordered_map<UInt16, Float64> model)
    {
        Float64 res = 0;
        for (auto & el : model) {
            if (standart[el.first] != 0) {
                res += el.second * log(standart[el.first]);
            } else {
                res += el.second * log(0.0000001);
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
        static std::unordered_map<String, std::unordered_map<UInt16, Float64>> encodings_freq = FrequencyHolder::getInstance().getEncodingsFrequency();

        if (!Tonality)
        {
            
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
        else 
        {
            Float64 freq = 0;
            Float64 count_words = 0;

            String ans;

            String to_check;
            ReadBufferFromString in(data);

            while (!in.eof())
            {
                readString(to_check, in);
                word_processing(to_check);

                if (emotional_dict.find(to_check) != emotional_dict.cend())
                {
                    count_words += 1;
                    ans += to_check + " " + std::to_string(emotional_dict[to_check]) + "\n";
                    freq += emotional_dict[to_check];
                }                
            }
            Float64 total_tonality = freq / count_words;
            ans += get_tonality(total_tonality) + std::to_string(total_tonality) + std::to_string(emotional_dict.size()) + "\n";
            res = ans;
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
            if (!Tonality)
            {
                std::unique_ptr<NgramCount[]> common_stats{new NgramCount[map_size]{}}; // frequency of N-grams 
                std::unique_ptr<NgramCount[]> ngram_storage{new NgramCount[map_size]{}}; // list of N-grams

                size_t len = calculateStats(str.data(), str.size(), common_stats.get(), readCodePoints, ngram_storage.get()); // count of N-grams
                // Float64 count_bigram = data.size() - 1;
                std::unordered_map<UInt16, Float64> model;

                for (size_t j = 0; j < len; ++j)
                {
                    model[ngram_storage.get()[j]] = static_cast<Float64>(common_stats.get()[ngram_storage.get()[j]]);
                }

                for (const auto& item : encodings_freq) {
                    prom += item.first + " " + std::to_string(Naive_bayes(item.second, model)) + "\n";
                }
            
            }
            else 
            {
                Float64 freq = 0;
                Float64 count_words = 0;


                String to_check;
                ReadBufferFromString in(str);

                while (!in.eof())
                {
                    readString(to_check, in);

                    word_processing(to_check);

                    if (emotional_dict.find(to_check) != emotional_dict.cend())
                    {
                        count_words += 1;
                        prom += to_check + " " + std::to_string(emotional_dict[to_check]) + "\n";
                        freq += emotional_dict[to_check];
                    }                
                }
                Float64 total_tonality = freq / count_words;
                prom += get_tonality(total_tonality) + std::to_string(total_tonality) + "\n";
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
struct NameGetTonality
{
    static constexpr auto name = "getTonality";
};


using FunctionCharsetDetect = FunctionsTextClassification<TextClassificationImpl<2, false>, NameCharsetDetect>;
using FunctionGetTonality = FunctionsTextClassification<TextClassificationImpl<2, true>, NameGetTonality>;

void registerFunctionsTextClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCharsetDetect>();
    factory.registerFunction<FunctionGetTonality>();
}

}
