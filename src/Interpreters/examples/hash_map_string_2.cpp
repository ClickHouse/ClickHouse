#include <iostream>
#include <iomanip>
#include <vector>

#include <Common/Stopwatch.h>

//#define DBMS_HASH_MAP_COUNT_COLLISIONS
#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <base/types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Compression/CompressedReadBuffer.h>
#include <base/StringRef.h>
#include <Common/HashTable/HashMap.h>
#include <Interpreters/AggregationCommon.h>

#ifdef __SSE4_1__
    #include <smmintrin.h>
#endif


/** Do this:
for file in MobilePhoneModel PageCharset Params URLDomain UTMSource Referer URL Title; do
    for size in 30000 100000 300000 1000000 5000000; do
        echo
        BEST_METHOD=0
        BEST_RESULT=0
        for method in {1..12}; do
            echo -ne $file $size $method '';
            TOTAL_ELEMS=0
            for i in {0..1000}; do
                TOTAL_ELEMS=$(( $TOTAL_ELEMS + $size ))
                if [[ $TOTAL_ELEMS -gt 25000000 ]]; then break; fi
                ./hash_map_string_2 $size $method < ${file}.bin 2>&1 |
                    grep HashMap | grep -oE '[0-9\.]+ elem';
            done | awk -W interactive '{ if ($1 > x) { x = $1 }; printf(".") } END { print x }' | tee /tmp/hash_map_string_2_res;
            CUR_RESULT=$(cat /tmp/hash_map_string_2_res | tr -d '.')
            if [[ $CUR_RESULT -gt $BEST_RESULT ]]; then
                BEST_METHOD=$method
                BEST_RESULT=$CUR_RESULT
            fi;
        done;
    echo Best: $BEST_METHOD - $BEST_RESULT
    done;
done
*/


#define DefineStringRef(STRUCT) \
\
struct STRUCT : public StringRef {}; \
\
namespace ZeroTraits \
{ \
    template <> \
    inline bool check<STRUCT>(STRUCT x) { return 0 == x.size; } /* NOLINT */ \
 \
    template <> \
    inline void set<STRUCT>(STRUCT & x) { x.size = 0; } /* NOLINT */ \
} \
 \
template <> \
struct DefaultHash<STRUCT> \
{ \
    size_t operator() (STRUCT x) const /* NOLINT */ \
    { \
        return CityHash_v1_0_2::CityHash64(x.data, x.size);  \
    } \
};

DefineStringRef(StringRef_Compare1_Ptrs)
DefineStringRef(StringRef_Compare1_Index)
DefineStringRef(StringRef_CompareMemcmp)
DefineStringRef(StringRef_Compare8_1_byUInt64)
DefineStringRef(StringRef_Compare16_1_byMemcmp)
DefineStringRef(StringRef_Compare16_1_byUInt64_logicAnd)
DefineStringRef(StringRef_Compare16_1_byUInt64_bitAnd)

#ifdef __SSE4_1__
DefineStringRef(StringRef_Compare16_1_byIntSSE)
DefineStringRef(StringRef_Compare16_1_byFloatSSE)
DefineStringRef(StringRef_Compare16_1_bySSE4)
DefineStringRef(StringRef_Compare16_1_bySSE4_wide)
DefineStringRef(StringRef_Compare16_1_bySSE_wide)
#endif

DefineStringRef(StringRef_CompareAlwaysTrue)
DefineStringRef(StringRef_CompareAlmostAlwaysTrue)


inline bool operator==(StringRef_Compare1_Ptrs lhs, StringRef_Compare1_Ptrs rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    const char * pos1 = lhs.data;
    const char * pos2 = rhs.data;

    const char * end1 = pos1 + lhs.size;

    while (pos1 < end1)
    {
        if (*pos1 != *pos2)
            return false;

        ++pos1;
        ++pos2;
    }

    return true;
}

inline bool operator==(StringRef_Compare1_Index lhs, StringRef_Compare1_Index rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    for (size_t i = 0; i < lhs.size; ++i)
        if (lhs.data[i] != rhs.data[i])
            return false;

    return true;
}

inline bool operator==(StringRef_CompareMemcmp lhs, StringRef_CompareMemcmp rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    return 0 == memcmp(lhs.data, rhs.data, lhs.size);
}


inline bool operator==(StringRef_Compare8_1_byUInt64 lhs, StringRef_Compare8_1_byUInt64 rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    const char * p1 = lhs.data;
    const char * p2 = rhs.data;
    size_t size = lhs.size;

    const char * p1_end = p1 + size;
    const char * p1_end_8 = p1 + size / 8 * 8;

    while (p1 < p1_end_8)
    {
        if (reinterpret_cast<const UInt64 *>(p1)[0] != reinterpret_cast<const UInt64 *>(p2)[0])
            return false;

        p1 += 8;
        p2 += 8;
    }

    while (p1 < p1_end)
    {
        if (*p1 != *p2)
            return false;

        ++p1;
        ++p2;
    }

    return true;
}


inline bool compare_byMemcmp(const char * p1, const char * p2)
{
    return 0 == memcmp(p1, p2, 16);
}

inline bool compare_byUInt64_logicAnd(const char * p1, const char * p2)
{
    return reinterpret_cast<const UInt64 *>(p1)[0] == reinterpret_cast<const UInt64 *>(p2)[0]
        && reinterpret_cast<const UInt64 *>(p1)[1] == reinterpret_cast<const UInt64 *>(p2)[1];
}

inline bool compare_byUInt64_bitAnd(const char * p1, const char * p2)
{
    return (reinterpret_cast<const UInt64 *>(p1)[0] == reinterpret_cast<const UInt64 *>(p2)[0])
         & (reinterpret_cast<const UInt64 *>(p1)[1] == reinterpret_cast<const UInt64 *>(p2)[1]);
}

#ifdef __SSE4_1__

inline bool compare_byIntSSE(const char * p1, const char * p2)
{
    return 0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))));
}

inline bool compare_byFloatSSE(const char * p1, const char * p2)
{
    return !_mm_movemask_ps(_mm_cmpneq_ps(                    /// Looks like incorrect while comparing subnormal floats.
        _mm_loadu_ps(reinterpret_cast<const float *>(p1)),
        _mm_loadu_ps(reinterpret_cast<const float *>(p2))));
}

#endif


template <bool compare(const char *, const char *)>
inline bool memequal(const char * p1, const char * p2, size_t size)
{
//    const char * p1_end = p1 + size;
    const char * p1_end_16 = p1 + size / 16 * 16;

    while (p1 < p1_end_16)
    {
        if (!compare(p1, p2))
            return false;

        p1 += 16;
        p2 += 16;
    }

/*    while (p1 < p1_end)
    {
        if (*p1 != *p2)
            return false;

        ++p1;
        ++p2;
    }*/

    switch (size % 16)
    {
        case 15: if (p1[14] != p2[14]) return false; [[fallthrough]];
        case 14: if (p1[13] != p2[13]) return false; [[fallthrough]];
        case 13: if (p1[12] != p2[12]) return false; [[fallthrough]];
        case 12: if (reinterpret_cast<const UInt32 *>(p1)[2] == reinterpret_cast<const UInt32 *>(p2)[2]) goto l8; else return false;
        case 11: if (p1[10] != p2[10]) return false; [[fallthrough]];
        case 10: if (p1[9] != p2[9]) return false; [[fallthrough]];
        case 9: if (p1[8] != p2[8]) return false;
    l8: [[fallthrough]];
        case 8: return reinterpret_cast<const UInt64 *>(p1)[0] == reinterpret_cast<const UInt64 *>(p2)[0];
        case 7: if (p1[6] != p2[6]) return false; [[fallthrough]];
        case 6: if (p1[5] != p2[5]) return false; [[fallthrough]];
        case 5: if (p1[4] != p2[4]) return false; [[fallthrough]];
        case 4: return reinterpret_cast<const UInt32 *>(p1)[0] == reinterpret_cast<const UInt32 *>(p2)[0];
        case 3: if (p1[2] != p2[2]) return false; [[fallthrough]];
        case 2: return reinterpret_cast<const UInt16 *>(p1)[0] == reinterpret_cast<const UInt16 *>(p2)[0];
        case 1: if (p1[0] != p2[0]) return false; [[fallthrough]];
        case 0: break;
    }

    return true;
}


#ifdef __SSE4_1__

inline bool memequal_sse41(const char * p1, const char * p2, size_t size)
{
//    const char * p1_end = p1 + size;
    const char * p1_end_16 = p1 + size / 16 * 16;

    __m128i zero16 = _mm_setzero_si128();

    while (p1 < p1_end_16)
    {
        if (!_mm_testc_si128(
            zero16,
            _mm_xor_si128(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2)))))
            return false;

        p1 += 16;
        p2 += 16;
    }

/*    while (p1 < p1_end)
    {
        if (*p1 != *p2)
            return false;

        ++p1;
        ++p2;
    }*/

    switch (size % 16)
    {
        case 15: if (p1[14] != p2[14]) return false; [[fallthrough]];
        case 14: if (p1[13] != p2[13]) return false; [[fallthrough]];
        case 13: if (p1[12] != p2[12]) return false; [[fallthrough]];
        case 12: if (reinterpret_cast<const UInt32 *>(p1)[2] == reinterpret_cast<const UInt32 *>(p2)[2]) goto l8; else return false;
        case 11: if (p1[10] != p2[10]) return false; [[fallthrough]];
        case 10: if (p1[9] != p2[9]) return false; [[fallthrough]];
        case 9: if (p1[8] != p2[8]) return false;
    l8: [[fallthrough]];
        case 8: return reinterpret_cast<const UInt64 *>(p1)[0] == reinterpret_cast<const UInt64 *>(p2)[0];
        case 7: if (p1[6] != p2[6]) return false; [[fallthrough]];
        case 6: if (p1[5] != p2[5]) return false; [[fallthrough]];
        case 5: if (p1[4] != p2[4]) return false; [[fallthrough]];
        case 4: return reinterpret_cast<const UInt32 *>(p1)[0] == reinterpret_cast<const UInt32 *>(p2)[0];
        case 3: if (p1[2] != p2[2]) return false; [[fallthrough]];
        case 2: return reinterpret_cast<const UInt16 *>(p1)[0] == reinterpret_cast<const UInt16 *>(p2)[0];
        case 1: if (p1[0] != p2[0]) return false; [[fallthrough]];
        case 0: break;
    }

    return true;
}


inline bool memequal_sse41_wide(const char * p1, const char * p2, size_t size)
{
    __m128i zero16 = _mm_setzero_si128();
//    const char * p1_end = p1 + size;

    while (size >= 64)
    {
        if (_mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[0]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[0])))
            && _mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[1]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[1])))
            && _mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[2]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[2])))
            && _mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[3]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[3]))))
        {
            p1 += 64;
            p2 += 64;
            size -= 64;
        }
        else
            return false;
    }

    switch ((size % 64) / 16)
    {
        case 3:
            if (!_mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[2]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[2]))))
                return false;
            [[fallthrough]];
        case 2:
            if (!_mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[1]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[1]))))
                return false;
            [[fallthrough]];
        case 1:
            if (!_mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[0]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[0]))))
                return false;
    }

    p1 += (size % 64) / 16 * 16;
    p2 += (size % 64) / 16 * 16;

/*

    if (size >= 32)
    {
        if (_mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[0]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[0])))
            & _mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[1]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[1]))))
        {
            p1 += 32;
            p2 += 32;
            size -= 32;
        }
        else
            return false;
    }

    if (size >= 16)
    {
        if (_mm_testc_si128(
                zero16,
                _mm_xor_si128(
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p1)[0]),
                    _mm_loadu_si128(&reinterpret_cast<const __m128i *>(p2)[0]))))
        {
            p1 += 16;
            p2 += 16;
            size -= 16;
        }
        else
            return false;
    }*/

    switch (size % 16)
    {
        case 15: if (p1[14] != p2[14]) return false; [[fallthrough]];
        case 14: if (p1[13] != p2[13]) return false; [[fallthrough]];
        case 13: if (p1[12] != p2[12]) return false; [[fallthrough]];
        case 12: if (reinterpret_cast<const UInt32 *>(p1)[2] == reinterpret_cast<const UInt32 *>(p2)[2]) goto l8; else return false;
        case 11: if (p1[10] != p2[10]) return false; [[fallthrough]];
        case 10: if (p1[9] != p2[9]) return false; [[fallthrough]];
        case 9: if (p1[8] != p2[8]) return false;
    l8: [[fallthrough]];
        case 8: return reinterpret_cast<const UInt64 *>(p1)[0] == reinterpret_cast<const UInt64 *>(p2)[0];
        case 7: if (p1[6] != p2[6]) return false; [[fallthrough]];
        case 6: if (p1[5] != p2[5]) return false; [[fallthrough]];
        case 5: if (p1[4] != p2[4]) return false; [[fallthrough]];
        case 4: return reinterpret_cast<const UInt32 *>(p1)[0] == reinterpret_cast<const UInt32 *>(p2)[0];
        case 3: if (p1[2] != p2[2]) return false; [[fallthrough]];
        case 2: return reinterpret_cast<const UInt16 *>(p1)[0] == reinterpret_cast<const UInt16 *>(p2)[0];
        case 1: if (p1[0] != p2[0]) return false; [[fallthrough]];
        case 0: break;
    }

    return true;
}


inline bool memequal_sse_wide(const char * p1, const char * p2, size_t size)
{
    while (size >= 64)
    {
        if (   compare_byIntSSE(p1,      p2)
            && compare_byIntSSE(p1 + 16, p2 + 16)
            && compare_byIntSSE(p1 + 32, p2 + 32)
            && compare_byIntSSE(p1 + 48, p2 + 48))
        {
            p1 += 64;
            p2 += 64;
            size -= 64;
        }
        else
            return false;
    }

    switch ((size % 64) / 16)
    {
        case 3: if (!compare_byIntSSE(p1 + 32, p2 + 32)) return false; [[fallthrough]];
        case 2: if (!compare_byIntSSE(p1 + 16, p2 + 16)) return false; [[fallthrough]];
        case 1: if (!compare_byIntSSE(p1     , p2     )) return false;
    }

    p1 += (size % 64) / 16 * 16;
    p2 += (size % 64) / 16 * 16;

    switch (size % 16)
    {
        case 15: if (p1[14] != p2[14]) return false; [[fallthrough]];
        case 14: if (p1[13] != p2[13]) return false; [[fallthrough]];
        case 13: if (p1[12] != p2[12]) return false; [[fallthrough]];
        case 12: if (reinterpret_cast<const UInt32 *>(p1)[2] == reinterpret_cast<const UInt32 *>(p2)[2]) goto l8; else return false;
        case 11: if (p1[10] != p2[10]) return false; [[fallthrough]];
        case 10: if (p1[9] != p2[9]) return false; [[fallthrough]];
        case 9: if (p1[8] != p2[8]) return false;
    l8: [[fallthrough]];
        case 8: return reinterpret_cast<const UInt64 *>(p1)[0] == reinterpret_cast<const UInt64 *>(p2)[0];
        case 7: if (p1[6] != p2[6]) return false; [[fallthrough]];
        case 6: if (p1[5] != p2[5]) return false; [[fallthrough]];
        case 5: if (p1[4] != p2[4]) return false; [[fallthrough]];
        case 4: return reinterpret_cast<const UInt32 *>(p1)[0] == reinterpret_cast<const UInt32 *>(p2)[0];
        case 3: if (p1[2] != p2[2]) return false; [[fallthrough]];
        case 2: return reinterpret_cast<const UInt16 *>(p1)[0] == reinterpret_cast<const UInt16 *>(p2)[0];
        case 1: if (p1[0] != p2[0]) return false; [[fallthrough]];
        case 0: break;
    }

    return true;
}

#endif


#define Op(METHOD) \
inline bool operator==(StringRef_Compare16_1_ ## METHOD lhs, StringRef_Compare16_1_ ## METHOD rhs) \
{ \
    if (lhs.size != rhs.size) \
        return false; \
\
    if (lhs.size == 0) \
        return true; \
\
    return memequal<compare_  ## METHOD>(lhs.data, rhs.data, lhs.size); \
}

Op(byMemcmp)
Op(byUInt64_logicAnd)
Op(byUInt64_bitAnd)

#ifdef __SSE4_1__

Op(byIntSSE)
Op(byFloatSSE)


inline bool operator==(StringRef_Compare16_1_bySSE4 lhs, StringRef_Compare16_1_bySSE4 rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    return memequal_sse41(lhs.data, rhs.data, lhs.size);
}

inline bool operator==(StringRef_Compare16_1_bySSE4_wide lhs, StringRef_Compare16_1_bySSE4_wide rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    return memequal_sse41_wide(lhs.data, rhs.data, lhs.size);
}

inline bool operator==(StringRef_Compare16_1_bySSE_wide lhs, StringRef_Compare16_1_bySSE_wide rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    return memequal_sse_wide(lhs.data, rhs.data, lhs.size);
}

#endif


inline bool operator==(StringRef_CompareAlwaysTrue, StringRef_CompareAlwaysTrue)
{
    return true;
}

inline bool operator==(StringRef_CompareAlmostAlwaysTrue lhs, StringRef_CompareAlmostAlwaysTrue rhs)
{
    return lhs.size == rhs.size;
}


using Value = UInt64;


template <typename Key>
void NO_INLINE bench(const std::vector<StringRef> & data, const char * name)
{
    Stopwatch watch;

    using Map = HashMapWithSavedHash<Key, Value, DefaultHash<Key>>;

    Map map;
    typename Map::LookupResult it;
    bool inserted;

    for (const auto & value : data)
    {
        map.emplace(static_cast<const Key &>(value), it, inserted);
        if (inserted)
            it->getMapped() = 0;
        ++it->getMapped();
    }

    watch.stop();
    std::cerr << std::fixed << std::setprecision(2)
        << "HashMap (" << name << "). Size: " << map.size()
        << ", elapsed: " << watch.elapsedSeconds()
        << " (" << data.size() / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
        << ", collisions: " << map.getCollisions()
#endif
        << std::endl;
}


int main(int argc, char ** argv)
{
    if (argc < 3)
    {
        std::cerr << "Usage: program n m\n";
        return 1;
    }

    size_t n = std::stol(argv[1]);
    size_t m = std::stol(argv[2]);

    DB::Arena pool;
    std::vector<StringRef> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(StringRef) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        std::string tmp;
        for (size_t i = 0; i < n && !in2.eof(); ++i)
        {
            DB::readStringBinary(tmp, in2);
            data[i] = StringRef(pool.insert(tmp.data(), tmp.size()), tmp.size());
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (!m || m == 1) bench<StringRef_Compare1_Ptrs>                (data, "StringRef_Compare1_Ptrs");
    if (!m || m == 2) bench<StringRef_Compare1_Index>               (data, "StringRef_Compare1_Index");
    if (!m || m == 3) bench<StringRef_CompareMemcmp>                (data, "StringRef_CompareMemcmp");
    if (!m || m == 4) bench<StringRef_Compare8_1_byUInt64>          (data, "StringRef_Compare8_1_byUInt64");
    if (!m || m == 5) bench<StringRef_Compare16_1_byMemcmp>         (data, "StringRef_Compare16_1_byMemcmp");
    if (!m || m == 6) bench<StringRef_Compare16_1_byUInt64_logicAnd>(data, "StringRef_Compare16_1_byUInt64_logicAnd");
    if (!m || m == 7) bench<StringRef_Compare16_1_byUInt64_bitAnd>  (data, "StringRef_Compare16_1_byUInt64_bitAnd");
#ifdef __SSE4_1__
    if (!m || m == 8) bench<StringRef_Compare16_1_byIntSSE>         (data, "StringRef_Compare16_1_byIntSSE");
    if (!m || m == 9) bench<StringRef_Compare16_1_byFloatSSE>       (data, "StringRef_Compare16_1_byFloatSSE");
    if (!m || m == 10) bench<StringRef_Compare16_1_bySSE4>          (data, "StringRef_Compare16_1_bySSE4");
    if (!m || m == 11) bench<StringRef_Compare16_1_bySSE4_wide>     (data, "StringRef_Compare16_1_bySSE4_wide");
    if (!m || m == 12) bench<StringRef_Compare16_1_bySSE_wide>      (data, "StringRef_Compare16_1_bySSE_wide");
#endif
    if (!m || m == 100) bench<StringRef_CompareAlwaysTrue>          (data, "StringRef_CompareAlwaysTrue");
    if (!m || m == 101) bench<StringRef_CompareAlmostAlwaysTrue>    (data, "StringRef_CompareAlmostAlwaysTrue");

    if (!m || m == 111) bench<StringRef>                            (data, "StringRef");

    /// 10 > 8, 9
    /// 1, 2, 5 - bad

    return 0;
}
