#include <string_view>
#include <unordered_map>
#include <Functions/FunctionsStringSimilarity.h>
#include <Functions/FunctionsNgramClassify.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/FunctionFactory.h>
#include <fstream>
#include <vector>
#include <memory>
#include "Interpreters/InterpreterCreateQuery.h"
#include "base/defines.h"
#include "base/types.h"
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#    include <arm_acle.h>
#endif

#if (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#include "vec_crc32.h"
#endif

/*
- - - - - - - - - - - - - Чего я хочу - - - - - - - - - - - - - -
1. Разбивать строчки на ngram-мы | -> | кладем просто в хешмапу (хешируем)

Теперь для всех моделей, которые у нас есть: (через которые мы хотим посмотреть)
    2. Достаем хешмапу модели (нехешированную), хешируем ее
    3. Сливаем модельки -> в каждую хешмапу добавляем все уникальные ngram-ы другой
    4. Нормируем мапы 
    5. Считаем формулу Байеса (конечно же с логарифмами)
    6. Возвращаем вероятность принадлежности строки к модели

И дальше в зависимости от того, что нужно дальше 
    - Либо просто нормируем вероятности и возвращаем вероятность пренадлежности нашему классу
    - Либо сортируем всех (отнормированных) и возвращаем вектором пар
    - Либо просто возвращаем наиболее вероятную модельку
*/


static const String model_path = "~/ClickHouse/opt/Storages/NgramModels/";

namespace DB
{



template <typename Map, class CodePoint, size_t N, bool UTF8, bool case_insensitive>
class NaiveBayes {

    using ResultType = Float32;

    /// map_size for ngram difference.
    static constexpr size_t map_size = 1u << 16;

    /// If the haystack size is bigger than this, behaviour is unspecified for this function.
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

    static ALWAYS_INLINE UInt16 calculateASCIIHash(const CodePoint * code_points)
    {
        return intHashCRC32(unalignedLoad<UInt32>(code_points)) & 0xFFFFu;
    }

    static ALWAYS_INLINE UInt16 calculateUTF8Hash(const CodePoint * code_points)
    {
        UInt64 combined = (static_cast<UInt64>(code_points[0]) << 32) | code_points[1];
#ifdef __SSE4_2__
        return _mm_crc32_u64(code_points[2], combined) & 0xFFFFu;
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        return __crc32cd(code_points[2], combined) & 0xFFFFu;
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return crc32_ppc(code_points[2], reinterpret_cast<const unsigned char *>(&combined), sizeof(combined)) & 0xFFFFu;
#elif defined(__s390x__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        return s390x_crc32(code_points[2], combined) & 0xFFFFu;
#else
        return (intHashCRC32(combined) ^ intHashCRC32(code_points[2])) & 0xFFFFu;
#endif
    }

    template <size_t Offset, class Container, size_t... I>
    static ALWAYS_INLINE inline void unrollLowering(Container & cont, const std::index_sequence<I...> &)
    {
        ((cont[Offset + I] = std::tolower(cont[Offset + I])), ...);
    }

    static ALWAYS_INLINE size_t readASCIICodePoints(CodePoint * code_points, const char *& pos, const char * end)
    {
        /// Offset before which we copy some data.
        constexpr size_t padding_offset = default_padding - N + 1;
        /// We have an array like this for ASCII (N == 4, other cases are similar)
        /// |a0|a1|a2|a3|a4|a5|a6|a7|a8|a9|a10|a11|a12|a13|a14|a15|a16|a17|a18|
        /// And we copy                                ^^^^^^^^^^^^^^^ these bytes to the start
        /// Actually it is enough to copy 3 bytes, but memcpy for 4 bytes translates into 1 instruction
        memcpy(code_points, code_points + padding_offset, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(CodePoint));
        /// Now we have an array
        /// |a13|a14|a15|a16|a4|a5|a6|a7|a8|a9|a10|a11|a12|a13|a14|a15|a16|a17|a18|
        ///              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        /// Doing unaligned read of 16 bytes and copy them like above
        /// 16 is also chosen to do two `movups`.
        /// Such copying allow us to have 3 codepoints from the previous read to produce the 4-grams with them.
        memcpy(code_points + (N - 1), pos, default_padding * sizeof(CodePoint));

        if constexpr (case_insensitive)
        {
            /// We really need template lambdas with C++20 to do it inline
            unrollLowering<N - 1>(code_points, std::make_index_sequence<padding_offset>());
        }
        pos += padding_offset;
        if (pos > end)
            return default_padding - (pos - end);
        return default_padding;
    }

    static ALWAYS_INLINE size_t readUTF8CodePoints(CodePoint * code_points, const char *& pos, const char * end)
    {
        /// The same copying as described in the function above.
        memcpy(code_points, code_points + default_padding - N + 1, roundUpToPowerOfTwoOrZero(N - 1) * sizeof(CodePoint));

        size_t num = N - 1;
        while (num < default_padding && pos < end)
        {
            size_t length = UTF8::seqLength(*pos);

            if (pos + length > end)
                length = end - pos;

            CodePoint res;
            /// This is faster than just memcpy because of compiler optimizations with moving bytes.
            switch (length)
            {
                case 1:
                    res = 0;
                    memcpy(&res, pos, 1);
                    break;
                case 2:
                    res = 0;
                    memcpy(&res, pos, 2);
                    break;
                case 3:
                    res = 0;
                    memcpy(&res, pos, 3);
                    break;
                default:
                    memcpy(&res, pos, 4);
            }

            /// This is not a really true case insensitive utf8. We zero the 5-th bit of every byte.
            /// And first bit of first byte if there are two bytes.
            /// For ASCII it works https://catonmat.net/ascii-case-conversion-trick. For most cyrillic letters also does.
            /// For others, we don't care now. Lowering UTF is not a cheap operation.
            if constexpr (case_insensitive)
            {
                switch (length)
                {
                    case 4:
                        res &= ~(1u << (5 + 3 * CHAR_BIT));
                        [[fallthrough]];
                    case 3:
                        res &= ~(1u << (5 + 2 * CHAR_BIT));
                        [[fallthrough]];
                    case 2:
                        res &= ~(1u);
                        res &= ~(1u << (5 + CHAR_BIT));
                        [[fallthrough]];
                    default:
                        res &= ~(1u << 5);
                }
            }

            pos += length;
            code_points[num++] = res;
        }
        return num;
    }

    // Counts all Ngrams in data[] -> result is in ngram_stats
    static ALWAYS_INLINE inline size_t calculateNeedleStats( 
        const char * data,
        const size_t size,
        NgramCount * ngram_stats,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt16 (*hash_functor)(const CodePoint *))
    {
        const char * start = data;
        const char * end = data + size;
        CodePoint cp[simultaneously_codepoints_num] = {};
        size_t found = read_code_points(cp, start, end);
        size_t i = N - 1;
        size_t len = 0;
        do
        {
            for (; i + N <= found; ++i)
            {
                ++len;
                UInt16 hash = hash_functor(cp + i);
                ++ngram_stats[hash];
            }
            i = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));
        return len;
    }

    template <bool reuse_stats>
    static ALWAYS_INLINE inline UInt64 calculateHaystackStatsAndMetric(
        const char * data,
        const size_t size,
        NgramCount * ngram_stats,
        size_t & distance,
        [[maybe_unused]] UInt16 * ngram_storage,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt16 (*hash_functor)(const CodePoint *))
    {
        size_t ngram_cnt = 0;
        const char * start = data;
        const char * end = data + size;
        CodePoint cp[simultaneously_codepoints_num] = {};

        /// read_code_points returns the position of cp where it stopped reading codepoints.
        size_t found = read_code_points(cp, start, end);
        /// We need to start for the first time here, because first N - 1 codepoints mean nothing.
        size_t iter = N - 1;

        do
        {
            for (; iter + N <= found; ++iter)
            {
                UInt16 hash = hash_functor(cp + iter);
                /// For symmetric version we should add when we can't subtract to get symmetric difference.
                if (static_cast<Int16>(ngram_stats[hash]) > 0)
                    --distance;
                if constexpr (reuse_stats)
                    ngram_storage[ngram_cnt] = hash;
                ++ngram_cnt;
                --ngram_stats[hash];
            }
            iter = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        /// Return the state of hash map to its initial.
        if constexpr (reuse_stats)
        {
            for (size_t i = 0; i < ngram_cnt; ++i)
                ++ngram_stats[ngram_storage[i]];
        }
        return ngram_cnt;
    }

    template <class Callback, class... Args>
    static inline auto dispatchSearcher(Callback callback, Args &&... args)
    {
        if constexpr (!UTF8)
            return callback(std::forward<Args>(args)..., readASCIICodePoints, calculateASCIIHash);
        else
            return callback(std::forward<Args>(args)..., readUTF8CodePoints, calculateUTF8Hash);
    }
    
};





class NgramTextClassificationImpl
{
    using ResultType = String;
    static constexpr Float64 zero_frequency = 1e-06;
    
    template <typename Map>
    static Float64 naiveBayes(
        const Map & text, // text is normalized
        const Map & model) // model is not normalized
    {
        int cnt_words_in_model = 0;

        for (const auto &[word, stat] : model)
        {
            cnt_words_in_model += stat;
        }

        for (const auto &[word, stat] : text)
        {
            if (model.find(word) == model.end())
            {
                ++cnt_words_in_model;
            }
        }


        Float64 result = 0;
        for (const auto &[word, stat] : text)
        {
            const auto it = model.find(word); 
            if (it == model.end())
            {
                result += stat * log(zero_frequency);
            } else
            {
                result += stat * log(stat / cnt_words_in_model);
            }
        }
        return result;
    }

    template<class Map, size_t N>
    class NgramModel {
        // модель, в которой лежит только одна запись какого-то slice-а
    public:
        // explicit NgramModel(const String &filename) {
        //     load(filename);
        // }
        /*
        Формат модели

        Имя N[сколько грам]
        [N-gram1] [cnt1]
        [N-gram2] [cnt2]
        ...
        */
        
        Float64 scoreText(const Map &text_model) const {
            return naiveBayes(text_model, map);
        }
        void learn(const String &name_, const String &text) { // загружаем из строки модель
            name = name_;
            map = buildModel<Map, N>(text);
        }
        const Map& getMap() const {
            return map;
        }
        String getName() const {
            return name;
        }
        size_t getN() const {
            return N;
        }
    private:
        String name;
        Map map;

        Float64 naiveBayes(
                const Map & text, // text is normalized
                const Map & model) const { // model is not normalized 
            int cnt_words_in_model = 0;

            for (const auto &[word, stat] : model) {
                cnt_words_in_model += stat;
            }

            for (const auto &[word, stat] : text) {
                if (model.find(word) == model.end()) {
                    ++cnt_words_in_model;
                }
            }

            Float64 result = 0;
            for (const auto &[word, stat] : text) {
                const auto it = model.find(word); 
                if (it == model.end()) {
                    result += stat * log(1 / cnt_words_in_model);
                } else {
                    result += stat * log(zero_frequency);
                }
            }
            return result;
        }


    };

    template<class Map, size_t N>
    class Slice {
        // for example, Lang-slice, explicity slice, ..., e.t.c.
    public:
        Slice() = default;
        explicit Slice(const std::string &directory) {
            load(directory);
        }

        String classify(const String &text) const {
            Map text_map = buildModel<Map, N>(text);
            Float64 best_score = 0.;
            String result;
            for (const auto &model : models) {
                Float64 local_result = model.scoreText(text_map);
                if (local_result > best_score) {
                    best_score = local_result;
                    result = model.getName();
                }
            }
            return result;
        }

        std::vector<std::pair<String, Float64>> score(const String &text, bool sorted = false) {
            Map text_map = buildModel<Map, N>(text);
            std::vector<std::pair<String, Float64>> result;
            for (const auto &model : models) {
                result.emplace_back(model.getName(), model.score(text_map));
            }
            if (sorted) {
                sort(result.begin(), result.end(), [](const auto &a, const auto &b){ return a.second > b.second; });
            }
            return result;
        }
    private:
        std::vector<NgramModel<Map, N>> models;
        std::string name;
        void load(const std::string &directory) {
            const std::filesystem::path path{directory};
            for (auto const& dir_entry : std::filesystem::directory_iterator{path}) {
                models.emplace_back(dir_entry.path());
            }
            if (models.empty()) {
                // BOOM
            }
            for (const auto &model : models) {
                if (model.getN() != N) {
                    // BOOM
                }
            }
        }
    };

    template <size_t N>
    struct StringSlice {
        const size_t p = 313;
        size_t precalced_pn;
        size_t length = 0;
        StringSlice() = default;
        size_t value = 0;
        explicit StringSlice(const String &s) {
            precalced_pn = 1;
            for (size_t index = 0; index < std::min(s.size(), N); ++index) {
                value *= p;
                value += static_cast<size_t>(s[index]);
                precalced_pn *= p;
            }
        }

        explicit StringSlice(char *buf) {
            precalced_pn = 1;
            for (size_t index = 0; index < std::min(strlen(buf), N); ++index) {
                value *= p;
                value += static_cast<size_t>(buf[index]);
                precalced_pn *= p;
            }
        }
        void slide(char old_c, char new_c) {
            value -= precalced_pn * static_cast<size_t>(old_c);
            value *= p;
            value += static_cast<size_t>(new_c);
        }
        bool operator == (const StringSlice& ss) const {
            return length == ss.length && value == ss.value;
        }
        size_t get() const {
            return value;
        }
    };

    template<size_t N>
    class SliceHash {
    public:
        size_t operator()(const StringSlice<N>& p) const
        {
            return p.value + p.length;
        }
    };

    template <typename Map, size_t N>
   Map static buildModel(const String &text) {
        // depends on custom_Hash
        StringSlice<N> ss(text);
        std::unordered_map<StringSlice<N>, int, SliceHash<N>> map;
        ++map[ss];
        for (size_t index = N; index < text.size(); ++index) {
            ss.slide(text[index - N], text[index]);
            ++map[ss];
        }
        return map;
    }

    template <typename Map, size_t N>
    Float64 ngramScore(const String &text, const NgramModel<Map, N> &model) {
        // depends on buildModel
        Map text_model = buildModel<Map>(text, model.getN());
        return naiveBayes(text_model, model.getMap());
    }

    // path will be "~/ClickHouse/src/Storages/NgramModels/[slice-Name]-[N]/"
    static void constant(std::string data, const String &slice_name, String &res) {
        Slice<std::unordered_map<StringSlice, int, CustomHashFunction> > slice(model_path + slice_name);
        res = slice.classify(data);
    }

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets &offsets, const String &slice_name, ColumnString::Chars &res_data, ColumnString::Offsets &res_offsets) {
        Slice<std::unordered_map<StringSlice, int, CustomHashFunction> > slice(model_path + slice_name);
        size_t prev_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            const UInt8 * haystack = &data[prev_offset];
            const String &result_value = slice.classify(reinterpret_cast<const char *>(haystack));
            res_data.resize(prev_offset + result_value.size() + 1);
            prev_offset = offsets[i];
            memcpy(&res_data[prev_offset], result_value.data(), result_value.size());
            res_data[prev_offset + result_value.size()] = '\0';
            prev_offset += result_value.size() + 1;
            res_offsets[i] = prev_offset;
        }
    }
    
};

struct NgramClassificationName
{
    static constexpr auto name = "NgramClassify";
};

using FunctionNgramTextClassification = NgramTextClassification<NgramTextClassificationImpl, NgramClassificationName>;

REGISTER_FUNCTION(NgramClassify)
{
    factory.registerFunction<FunctionNgramTextClassification>();
}

}
