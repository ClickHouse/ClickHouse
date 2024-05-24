#include <Common/StringUtils.h>

#include <Common/TargetSpecific.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif


namespace impl
{

bool startsWith(const std::string & s, const char * prefix, size_t prefix_size)
{
    return s.size() >= prefix_size && 0 == memcmp(s.data(), prefix, prefix_size);
}

bool endsWith(const std::string & s, const char * suffix, size_t suffix_size)
{
    return s.size() >= suffix_size && 0 == memcmp(s.data() + s.size() - suffix_size, suffix, suffix_size);
}

}

DECLARE_DEFAULT_CODE(
bool isAllASCII(const UInt8 * data, size_t size)
{
    UInt8 mask = 0;
    for (size_t i = 0; i < size; ++i)
        mask |= data[i];

    return !(mask & 0x80);
})

DECLARE_SSE42_SPECIFIC_CODE(
/// Copy from https://github.com/lemire/fastvalidate-utf-8/blob/master/include/simdasciicheck.h
bool isAllASCII(const UInt8 * data, size_t size)
{
    __m128i masks = _mm_setzero_si128();

    size_t i = 0;
    for (; i + 16 <= size; i += 16)
    {
        __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + i));
        masks = _mm_or_si128(masks, bytes);
    }
    int mask = _mm_movemask_epi8(masks);

    UInt8 tail_mask = 0;
    for (; i < size; i++)
        tail_mask |= data[i];

    mask |= (tail_mask & 0x80);
    return !mask;
})

DECLARE_AVX2_SPECIFIC_CODE(
bool isAllASCII(const UInt8 * data, size_t size)
{
    __m256i masks = _mm256_setzero_si256();

    size_t i = 0;
    for (; i + 32 <= size; i += 32)
    {
        __m256i bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
        masks = _mm256_or_si256(masks, bytes);
    }
    int mask = _mm256_movemask_epi8(masks);

    UInt8 tail_mask = 0;
    for (; i < size; i++)
        tail_mask |= data[i];

    mask |= (tail_mask & 0x80);
    return !mask;
})

bool isAllASCII(const UInt8 * data, size_t size)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(DB::TargetArch::AVX2))
        return TargetSpecific::AVX2::isAllASCII(data, size);
    if (isArchSupported(DB::TargetArch::SSE42))
        return TargetSpecific::SSE42::isAllASCII(data, size);
#endif
    return TargetSpecific::Default::isAllASCII(data, size);
}
