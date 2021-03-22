#include "memcpy.h"

#include <atomic>
#include <cstdint>
#include <cmath>

#include <cpuid.h>


#define ALWAYS_INLINE __attribute__((__always_inline__))
#define NO_INLINE __attribute__((__noinline__))
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))


namespace
{

using memcpy_type = void * (*)(void * __restrict, const void * __restrict, size_t);

#include "memcpy_medium_every_unroll_vec_type.inl.h"


void * memcpy_erms(void * dst, const void * src, size_t size)
{
    asm volatile (
        "rep movsb"
        : "=D"(dst), "=S"(src), "=c"(size)
        : "0"(dst), "1"(src), "2"(size)
        : "memory");
    return dst;
}


ALWAYS_INLINE uint32_t rdtsc()
{
    uint32_t low;
    uint32_t high;
    __asm__ __volatile__ ("rdtsc" : "=a"(low), "=d"(high));
    return low;
}


struct VariantWithStatistics
{
    std::atomic<memcpy_type> func{nullptr};
    std::atomic<size_t> time = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> count = 0;
    std::atomic<size_t> kind = 0;

    /// Less is better.
    double score() const
    {
        double loaded_time = time.load(std::memory_order_relaxed);
        double loaded_bytes = bytes.load(std::memory_order_relaxed);
        double loaded_count = count.load(std::memory_order_relaxed);

        double mean = loaded_time / loaded_bytes;
        double sigma = mean / sqrt(loaded_count);

        return mean + sigma;
    }

    bool operator< (const VariantWithStatistics & rhs) const
    {
        return score() < rhs.score();
    }

    void smooth()
    {
        time.store(1 + time.load(std::memory_order_relaxed) / 2, std::memory_order_relaxed);
        bytes.store(1 + bytes.load(std::memory_order_relaxed) / 2, std::memory_order_relaxed);
        count.store(1 + count.load(std::memory_order_relaxed) / 2, std::memory_order_relaxed);
    }

    constexpr VariantWithStatistics(size_t kind_, memcpy_type func_) : func(func_), kind(kind_) {}
    constexpr VariantWithStatistics() : func(inline_memcpy), kind(0) {}

    void set(size_t kind_, memcpy_type func_)
    {
        func.store(func_, std::memory_order_relaxed);
        kind.store(kind_, std::memory_order_relaxed);
    }
};


/// NOTE: I would like to use Thompson Sampling, but it's too heavy.

struct MultipleVariantsWithStatistics
{
    std::atomic<memcpy_type> selected_variant{inline_memcpy};

    static constexpr size_t num_cells = 4;

    VariantWithStatistics variants[num_cells]{};

    std::atomic<size_t> count = 0;
    std::atomic<size_t> exploration_count = 0;

    static constexpr size_t probability_distribution_buckets = 256;
    std::atomic<size_t> exploration_probability_threshold = 0;

    static constexpr size_t num_explorations_before_optimize = num_cells * 16;

    MultipleVariantsWithStatistics()
    {
        init();
    }

    ALWAYS_INLINE void * call(void * __restrict dst, const void * __restrict src, size_t size)
    {
        size_t current_count = count++;

        if (LIKELY(current_count % probability_distribution_buckets < exploration_probability_threshold))
        {
            /// Exploitation mode.
            return selected_variant.load(std::memory_order_relaxed)(dst, src, size);
        }
        else
        {
            /// Exploration mode.
            return explore(dst, src, size);
        }
    }

    NO_INLINE void * explore(void * __restrict dst, const void * __restrict src, size_t size)
    {
        size_t current_exploration_count = exploration_count++;

        size_t hash = current_exploration_count;
        hash *= 0xff51afd7ed558ccdULL;
        hash ^= hash >> 33;

        VariantWithStatistics & variant = variants[hash % num_cells];

        uint32_t time1 = rdtsc();
        void * ret = variant.func.load(std::memory_order_relaxed)(dst, src, size);
        uint32_t time2 = rdtsc();
        uint32_t time = time2 - time1;

        if (time < size)
        {
            ++variant.count;
            variant.bytes += size;
            variant.time += time;
        }

        if (current_exploration_count == num_explorations_before_optimize)
        {
            double exploration_probability = 1.0 - double(exploration_probability_threshold.load(std::memory_order_relaxed))
                / probability_distribution_buckets;

            exploration_probability /= 1.5;

            exploration_probability_threshold.store(
                std::min<size_t>(probability_distribution_buckets - 1,
                    probability_distribution_buckets * (1.0 - exploration_probability)),
                std::memory_order_relaxed);

            exploration_count.store(0, std::memory_order_relaxed);

            size_t best_variant = 0;
            double best_score = 1e9;

            for (size_t i = 0; i < num_cells; ++i)
            {
                double score = variants[i].score();
                variants[i].smooth();

                if (score < best_score)
                {
                    best_score = score;
                    best_variant = i;
                }
            }
            selected_variant.store(variants[best_variant].func.load(std::memory_order_relaxed));

            // std::cerr << variants[best_variant].kind << " ";
        }

        return ret;
    }

    void init()
    {
        uint32_t eax;
        uint32_t ebx;
        uint32_t ecx;
        uint32_t edx;

        __cpuid(1, eax, ebx, ecx, edx);
        bool have_avx = ecx & bit_AVX;

        bool have_avx512 = false;
        if (have_avx)
        {
            __cpuid(0, eax, ebx, ecx, edx);
            bool have_leaf7 = eax >= 7;

            if (have_leaf7)
            {
                __cpuid(7, eax, ebx, ecx, edx);
                have_avx512 = ebx & bit_AVX512F;
            }
        }

        size_t idx = 0;

        variants[idx].set(idx, memcpy_erms);

        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled4_sse);

        if (have_avx)
        {
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled4_avx);
        }

        if (have_avx512)
        {
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled4_avx512);
        }

        ++idx;
        size_t num_variants = idx;
        while (idx < num_cells)
        {
            size_t copy_from = idx % num_variants;
            variants[idx].set(copy_from, variants[copy_from].func.load(std::memory_order_relaxed));
            ++idx;
        }
    }
};


MultipleVariantsWithStatistics variants __attribute__((init_priority(101)));

}


extern "C" uint8_t * memcpy(uint8_t * __restrict dst, const uint8_t * __restrict src, size_t size)
{
    uint8_t * ret = dst;

    if (size <= 16)
    {
        if (size >= 8)
        {
            __builtin_memcpy(dst + size - 8, src + size - 8, 8);
            __builtin_memcpy(dst, src, 8);
        }
        else if (size >= 4)
        {
            __builtin_memcpy(dst + size - 4, src + size - 4, 4);
            __builtin_memcpy(dst, src, 4);
        }
        else if (size >= 2)
        {
            __builtin_memcpy(dst + size - 2, src + size - 2, 2);
            __builtin_memcpy(dst, src, 2);
        }
        else if (size >= 1)
        {
            *dst = *src;
        }
    }
    else if (size <= 128)
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16)));

        while (size > 16)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
            dst += 16;
            src += 16;
            size -= 16;
        }
    }
    else if (size < 30000)
    {
        /// Align destination to 16 bytes boundary.
        size_t padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

        if (padding > 0)
        {
            __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
            _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
            dst += padding;
            src += padding;
            size -= padding;
        }

        /// Aligned unrolled copy.
        __m128i c0, c1, c2, c3, c4, c5, c6, c7;

        while (size >= 128)
        {
            c0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 0);
            c1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 1);
            c2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 2);
            c3 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 3);
            c4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 4);
            c5 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 5);
            c6 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 6);
            c7 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 7);
            src += 128;
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 0), c0);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 1), c1);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 2), c2);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 3), c3);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 4), c4);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 5), c5);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 6), c6);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 7), c7);
            dst += 128;

            size -= 128;
        }

        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16)));

        while (size > 16)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
            dst += 16;
            src += 16;
            size -= 16;
        }
    }
    else
    {
        return static_cast<uint8_t*>(variants.call(dst, src, size));
    }

    return ret;
}

