#include <cstddef>
#include <limits>
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
#include "Core/Types.h"
#include "Interpreters/InterpreterCreateQuery.h"
#include "base/defines.h"
#include "base/types.h"
#include "Common/setThreadName.h"
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/UTF8Helpers.h>
#include <filesystem>

namespace fs = std::filesystem; 

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


static const String model_path = "../../opt/NgramModels/";

namespace DB
{


/// map_size for ngram difference.
static constexpr size_t map_size = 1u << 16;

/// If the haystack size is bigger than this, behaviour is unspecified for this function.
// static constexpr size_t max_string_size = 1u << 15;

/// Default padding to read safely.n
static constexpr size_t default_padding = 16;

/// Max codepoints to store at once. 16 is for batching usage and PODArray has this padding.

/** map_size of this fits mostly in L2 cache all the time.
    * Actually use UInt16 as addings and subtractions do not UB overflow. But think of it as a signed
    * integer array.
    */
using NgramCount = UInt16;


template <class CodePoint, size_t N, bool UTF8, bool case_insensitive>
class NaiveBayes {
public:
    void learn(const std::string &text) {
        map = std::shared_ptr<NgramCount[]>(new NgramCount[map_size]);
        std::memset(static_cast<NgramCount*>(map[0]), 0, map_size * sizeof(NgramCount));
        dispatchSearcher(calculateNeedleStats, text.data(), text.size(), map.get());
        
        for (size_t index = 0; index < map_size; ++index) {
            if (map[index] != 0) {
                std::cout << "map[" << index << "] is nonzero (=" << map[index] << ")\n";
            }
        }


        map_normalized = std::shared_ptr<Float64[]>(new Float64[map_size]);
        if (text.size() < N) {
            // BOOM
        }
        Float64 total = static_cast<Float64>(text.size()) - static_cast<Float64>(N - 1);
        for (size_t index = 0; index < map_size; ++index) {
            if (map[index]) {
                map_normalized[index] = log(map[index] / total);
            }
        }
    }

    std::shared_ptr<NgramCount[]> getmap() {
        return map;
    }

    std::shared_ptr<Float64[]> getmapNorm() {
        return map_normalized;
    }

    Float64 score(std::shared_ptr<NgramCount[]> text_map) const {
        Float64 result = 0.;
        for (size_t index = 0; index < map_size; ++index) {
            if (text_map[index] == 0 || map_normalized[index] == 0) {
                continue;
            }
            result += static_cast<Float64>(text_map[index]) * map_normalized[index];
        }
        return result;
    }
private:

    static constexpr size_t simultaneously_codepoints_num = default_padding + N - 1;

    std::shared_ptr<Float64[]> map_normalized;
    std::shared_ptr<NgramCount[]> map;

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

        // Return the state of hash map to its initial.
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
            return callback(std::forward<Args>(args)..., readUTF8CodePoints, calculateUTF8Hash); // UTF8!
    }
};

template <class CodePoint, size_t N, bool UTF8, bool case_insensitive>
class Slice {
public:
    void fit(const std::unordered_map<String, String> &slice) {
        models.clear();
        models.reserve(slice.size());
        for (const auto &[slice_name, slice_text] : slice) {
            models[slice_name].learn(slice_text);
        }
    }
    std::unordered_map<String, Float64> score(const String &text) const {
        NaiveBayes<CodePoint, N, UTF8, case_insensitive> text_model;
        text_model.learn(text);
        std::shared_ptr<NgramCount[]> text_map = text_model.getmap();
        std::unordered_map<String, Float64> result;
        result.reserve(models.size());
        for (const auto &[name, model] : models) {
            result[name] = model.score(text_map);
        }
        return result;
    }
private:
    std::unordered_map<String, NaiveBayes<CodePoint, N, UTF8, case_insensitive>> models;
};


template<class Section>
class NgramStorage {
public:

    String classify(const String &name, const String &text) const {
        if (map.find(name) == map.end()) {
            // BOOM
        }
        std::unordered_map<String, Float64> scoring = map.at(name).score(text);
        Float64 best_score = std::numeric_limits<Float64>::min();
        std::string_view result;

        for (auto &[sub_name, score] : scoring) {
            std::cout << "tmp SCORE = " << score << " for string " << sub_name << '\n';
            if (score > best_score) {
                best_score = score;
                result = sub_name;
            }
        }

        return String(result);
    }


    explicit NgramStorage() 
    {
        std::cout << "Storage INIT\n";
        reload();
        // t_reload = ThreadFromGlobalPool([this] { reloadPeriodically(); });
    }


    ~NgramStorage() 
    {
        destroy.set();
        // t_reload.join();
    }


    void reload() {
        for (const auto &entry : fs::directory_iterator(model_path)) {
            if (!entry.is_directory()) {
                // BOOM
            }
            const String &section_name = entry.path().filename().string();

            std::cout << "Section name is " << section_name << '\n';


            std::unordered_map<String, String> section_models;
            for (const auto &model: fs::directory_iterator(entry.path())) {
                const String &model_name = model.path().filename().string();

                std::cout << "Model name is " << model_name << '\n';

                // here we need to load model
                if (!model.is_regular_file()) {
                    // BOOM
                }
                std::ifstream file(model.path().string(), std::ios_base::in);
                std::string str{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
                file.close();
                section_models[model_name] = str;
            }
            map[section_name].fit(section_models);
        }
    }

    // void reloadPeriodically()
    // {
    //     setThreadName("Ngram Storage Reload");

    //     while (true)
    //     {
    //         destroy.tryWait(cur_reload_period * 1000);
    //         reload();
    //     }
    // }
    // static constexpr Int64 cur_reload_period = 10;
    Poco::Event destroy;
    std::unordered_map<String, Section> map;
    // ThreadFromGlobalPool t_reload;
};




template <class Storage> 
class NgramTextClassificationImpl
{
public:
    NgramTextClassificationImpl() {
        checkload();
    }
    using ResultType = String;
    String classify(const String &name, const String &text) const {
        return storage->classify(name, text);
    }
private:
    void checkload() {
        if (!storage) {
            storage.reset(new Storage());
        }
    }
    std::shared_ptr<Storage> storage = nullptr;
};


using Bayes = NaiveBayes<UInt8, 5, false, true>;
using NgramSlice = Slice<UInt8, 5, false, true>;
using Storage = NgramStorage<NgramSlice>;


using BayesUTF8 = NaiveBayes<UInt32, 3, true, true>;
using NgramSliceUTF8 = Slice<UInt32, 3, true, true>;
using StorageUTF8 = NgramStorage<NgramSliceUTF8>;
    

struct NgramClassificationName
{
    static constexpr auto name = "NgramClassify";
};

struct NgramClassificationNameUTF8
{
    static constexpr auto name = "NgramClassifyUTF8";
};

using FunctionNgramClassification = NgramTextClassification<NgramTextClassificationImpl<Storage>, NgramClassificationName>;
using FunctionNgramClassificationUTF8 = NgramTextClassification<NgramTextClassificationImpl<StorageUTF8>, NgramClassificationNameUTF8>;

REGISTER_FUNCTION(NgramClassify)
{
    factory.registerFunction<FunctionNgramClassification>();
    factory.registerFunction<FunctionNgramClassificationUTF8>();
}

}
