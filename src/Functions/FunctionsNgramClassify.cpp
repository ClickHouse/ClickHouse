#pragma once

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsNgramClassify.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <Common/UTF8Helpers.h>
#include <Common/register_objects.h>

#include <base/defines.h>
#include <base/types.h>

#include <map>
#include <memory>
#include <stdexcept>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{
/**
  * Implementation of naive Bayes Classifier
  * Classifier uses n-grams with paramatrized N. 
  * Implementation is inspired by NgramDistanceImpl.
  */
template <size_t N, class CodePoint, bool UTF8>
class NaiveBayesClassifier
{
public:
    using Probability = Float32;

    /// map_size for ngram difference.
    static constexpr size_t map_size = 1u << 16;

    /// Default padding to read safely.
    static constexpr size_t default_padding = 16;

    /// Max codepoints to store at once. 16 is for batching usage and PODArray has this padding.
    static constexpr size_t simultaneously_codepoints_num = default_padding + N - 1;

    // Alpha parameter for smoothing
    static constexpr Probability alpha = 1;

    /** map_size of this fits mostly in L2 cache all the time.
      * Actually use UInt16 as addings and subtractions do not UB overflow. But think of it as a signed
      * integer array.
      */
    using NgramCount = UInt16;

    struct NgramCorpus
    {
        std::unique_ptr<NgramCount[]> corpus;
        std::size_t size;
    };

    struct ProcessedNgramCorpus
    {
        std::unique_ptr<Probability[]> corpus;
        std::size_t size;
    };

    using Model = std::unordered_map<std::string, ProcessedNgramCorpus>;

    using NamedText = std::pair<std::string, std::string>;

    using NamedTexts = std::vector<NamedText>;

private:
    Model model;
    size_t model_size;

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

        /// We really need template lambdas with C++20 to do it inline
        unrollLowering<N - 1>(code_points, std::make_index_sequence<padding_offset>());

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

            pos += length;
            code_points[num++] = res;
        }
        return num;
    }

    static ALWAYS_INLINE inline size_t calculateNgramStats(
        const char * data,
        const size_t size,
        NgramCount * ngram_corpus,
        size_t (*read_code_points)(CodePoint *, const char *&, const char *),
        UInt16 (*hash_functor)(const CodePoint *))
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
                ++len;
                UInt16 hash = hash_functor(cp + i);
                ++ngram_corpus[hash];
            }
            i = 0;
        } while (start < end && (found = read_code_points(cp, start, end)));

        return len;
    }

    template <class Callback, class... Args>
    static inline auto dispatchSearcher(Callback callback, Args &&... args)
    {
        if constexpr (!UTF8)
            return callback(std::forward<Args>(args)..., readASCIICodePoints, calculateASCIIHash);
        else
            return callback(std::forward<Args>(args)..., readUTF8CodePoints, calculateUTF8Hash);
    }

public:
    void fitTexts(const NamedTexts & texts)
    {
        model.clear();

        model_size = 0;
        for (const auto & [name, text] : texts)
            model_size += text.size() >= N ? text.size() - (N - 1) : 0;

        for (const auto & [name, text] : texts)
        {
            auto processed_corpus_it = model.emplace(
                name,
                ProcessedNgramCorpus{
                    std::unique_ptr<Probability[]>(new Probability[map_size]{}),
                    0u,
                });
            if (!processed_corpus_it.second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad configuration of ngram classifier: duplicate of model name {}", name);

            auto & [processed_corpus, corpus_size] = processed_corpus_it.first->second;
            corpus_size = text.size() >= N ? text.size() - (N - 1) : 0;

            std::unique_ptr<NgramCount[]> corpus(new NgramCount[map_size]{});
            dispatchSearcher(calculateNgramStats, text.data(), text.size(), corpus.get());

            for (size_t i = 0; i < map_size; i++)
                processed_corpus[i] = std::logf((corpus[i] + alpha) / (alpha * model_size + corpus_size));
        }
    }

    std::unordered_map<std::string, Probability> predict(const char * data, const size_t size) const
    {
        std::unordered_map<std::string, Probability> result;

        std::unique_ptr<NgramCount[]> prediction_corpus(new NgramCount[map_size]{});
        dispatchSearcher(calculateNgramStats, data, size, prediction_corpus.get());

        for (const auto & [corpus_name, corpus] : model)
        {
            Probability bayes_probability = std::logf(corpus.size) - std::logf(model_size);

            for (size_t i = 0; i < map_size; i++)
                bayes_probability += prediction_corpus.get()[i] * corpus.corpus[i];

            result.emplace(corpus_name, bayes_probability);
        }

        return result;
    }
};

struct NameNgramClassify
{
    static constexpr auto name = "ngramClassify";
};
struct NameNgramClassifyUTF8
{
    static constexpr auto name = "ngramClassifyUTF8";
};

using NaiveBayesClassifierASCII = NaiveBayesClassifier<4, UInt8, false>;
using NaiveBayesClassifierUTF8 = NaiveBayesClassifier<3, UInt32, true>;

using NgramMostProbableClassifierASCII = NgramMostProbableClassifier<NaiveBayesClassifierASCII, NameNgramClassify>;
using NgramMostProbableClassifierUTF8 = NgramMostProbableClassifier<NaiveBayesClassifierUTF8, NameNgramClassifyUTF8>;

REGISTER_FUNCTION(NgramClassifiers)
{
    factory.registerFunction<NgramMostProbableClassifierASCII>();
    factory.registerFunction<NgramMostProbableClassifierUTF8>();
}

}
