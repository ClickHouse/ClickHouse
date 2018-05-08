#include <iostream>
#include <iomanip>
#include <vector>

#include <Common/Stopwatch.h>

#include <farmhash.h>
#include <metrohash.h>

#define DBMS_HASH_MAP_COUNT_COLLISIONS
#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <Core/Types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/CompressedReadBuffer.h>
#include <common/StringRef.h>
#include <Common/HashTable/HashMap.h>
#include <Interpreters/AggregationCommon.h>

#if __SSE4_1__
    #include <smmintrin.h>
#endif


/** Do this:
for file in MobilePhoneModel PageCharset Params URLDomain UTMSource Referer URL Title; do
    for size in 30000 100000 300000 1000000 5000000; do
        echo
        BEST_METHOD=0
        BEST_RESULT=0
        for method in {1..11}; do
            echo -ne $file $size $method '';
            TOTAL_ELEMS=0
            for i in {0..1000}; do
                TOTAL_ELEMS=$(( $TOTAL_ELEMS + $size ))
                if [[ $TOTAL_ELEMS -gt 25000000 ]]; then break; fi
                ./hash_map_string_3 $size $method < ${file}.bin 2>&1 |
                    grep HashMap | grep -oE '[0-9\.]+ elem';
            done | awk -W interactive '{ if ($1 > x) { x = $1 }; printf(".") } END { print x }' | tee /tmp/hash_map_string_3_res;
            CUR_RESULT=$(cat /tmp/hash_map_string_3_res | tr -d '.')
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
    inline bool check<STRUCT>(STRUCT x) { return nullptr == x.data; } \
 \
    template <> \
    inline void set<STRUCT>(STRUCT & x) { x.data = nullptr; } \
}; \
 \
template <> \
struct DefaultHash<STRUCT> \
{ \
    size_t operator() (STRUCT x) const \
    { \
        return CityHash_v1_0_2::CityHash64(x.data, x.size);  \
    } \
};


DefineStringRef(StringRef_CompareMemcmp)
DefineStringRef(StringRef_CompareAlwaysTrue)


inline bool operator==(StringRef_CompareMemcmp lhs, StringRef_CompareMemcmp rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

    return 0 == memcmp(lhs.data, rhs.data, lhs.size);
}

inline bool operator==(StringRef_CompareAlwaysTrue, StringRef_CompareAlwaysTrue)
{
    return true;
}


#define mix(h) ({                   \
    (h) ^= (h) >> 23;               \
    (h) *= 0x2127599bf4325c37ULL;   \
    (h) ^= (h) >> 47; })

struct FastHash64
{
    size_t operator() (StringRef x) const
    {
        const char * buf = x.data;
        size_t len = x.size;

        const uint64_t    m = 0x880355f21e6d1965ULL;
        const uint64_t *pos = reinterpret_cast<const uint64_t *>(buf);
        const uint64_t *end = pos + (len / 8);
        const unsigned char *pos2;
        uint64_t h = len * m;
        uint64_t v;

        while (pos != end)
        {
            v = *pos++;
            h ^= mix(v);
            h *= m;
        }

        pos2 = reinterpret_cast<const unsigned char*>(pos);
        v = 0;

        switch (len & 7)
        {
            case 7: v ^= static_cast<uint64_t>(pos2[6]) << 48; [[fallthrough]];
            case 6: v ^= static_cast<uint64_t>(pos2[5]) << 40; [[fallthrough]];
            case 5: v ^= static_cast<uint64_t>(pos2[4]) << 32; [[fallthrough]];
            case 4: v ^= static_cast<uint64_t>(pos2[3]) << 24; [[fallthrough]];
            case 3: v ^= static_cast<uint64_t>(pos2[2]) << 16; [[fallthrough]];
            case 2: v ^= static_cast<uint64_t>(pos2[1]) << 8; [[fallthrough]];
            case 1: v ^= static_cast<uint64_t>(pos2[0]);
                h ^= mix(v);
                h *= m;
        }

        return mix(h);
    }
};


struct FNV1a
{
    size_t operator() (StringRef x) const
    {
        size_t res = 0xcbf29ce484222325ULL;

        const char * pos = x.data;
        const char * end = x.data + x.size;

        for (; pos < end; ++pos)
        {
            res *= 1099511628211ULL;
            res ^= *pos;
        }

        return res;
    }
};


#if __SSE4_1__

struct CrapWow
{
    size_t operator() (StringRef x) const
    {
        const char * key = x.data;
        size_t len = x.size;
        size_t seed = 0;

        const UInt64 m = 0x95b47aa3355ba1a1, n = 0x8a970be7488fda55;
        UInt64 hash;
        // 3 = m, 4 = n
        // r12 = h, r13 = k, ecx = seed, r12 = key
        asm(
            "leaq (%%rcx,%4), %%r13\n"
            "movq %%rdx, %%r14\n"
            "movq %%rcx, %%r15\n"
            "movq %%rcx, %%r12\n"
            "addq %%rax, %%r13\n"
            "andq $0xfffffffffffffff0, %%rcx\n"
            "jz QW%=\n"
            "addq %%rcx, %%r14\n\n"
            "negq %%rcx\n"
        "XW%=:\n"
            "movq %4, %%rax\n"
            "mulq (%%r14,%%rcx)\n"
            "xorq %%rax, %%r12\n"
            "xorq %%rdx, %%r13\n"
            "movq %3, %%rax\n"
            "mulq 8(%%r14,%%rcx)\n"
            "xorq %%rdx, %%r12\n"
            "xorq %%rax, %%r13\n"
            "addq $16, %%rcx\n"
            "jnz XW%=\n"
        "QW%=:\n"
            "movq %%r15, %%rcx\n"
            "andq $8, %%r15\n"
            "jz B%=\n"
            "movq %4, %%rax\n"
            "mulq (%%r14)\n"
            "addq $8, %%r14\n"
            "xorq %%rax, %%r12\n"
            "xorq %%rdx, %%r13\n"
        "B%=:\n"
            "andq $7, %%rcx\n"
            "jz F%=\n"
            "movq $1, %%rdx\n"
            "shlq $3, %%rcx\n"
            "movq %3, %%rax\n"
            "shlq %%cl, %%rdx\n"
            "addq $-1, %%rdx\n"
            "andq (%%r14), %%rdx\n"
            "mulq %%rdx\n"
            "xorq %%rdx, %%r12\n"
            "xorq %%rax, %%r13\n"
        "F%=:\n"
            "leaq (%%r13,%4), %%rax\n"
            "xorq %%r12, %%rax\n"
            "mulq %4\n"
            "xorq %%rdx, %%rax\n"
            "xorq %%r12, %%rax\n"
            "xorq %%r13, %%rax\n"
            : "=a"(hash), "=c"(key), "=d"(key)
            : "r"(m), "r"(n), "a"(seed), "c"(len), "d"(key)
            : "%r12", "%r13", "%r14", "%r15", "cc"
        );
        return hash;
    }
};

#endif


struct SimpleHash
{
    size_t operator() (StringRef x) const
    {
        const char * pos = x.data;
        size_t size = x.size;

        const char * end = pos + size;

        size_t res = 0;

        if (size == 0)
            return 0;

        if (size < 8)
        {
#if __SSE4_1__
            return hashLessThan8(x.data, x.size);
#endif
        }

        while (pos + 8 < end)
        {
            uint64_t word = *reinterpret_cast<const uint64_t *>(pos);
            res = intHash64(word ^ res);

            pos += 8;
        }

        uint64_t word = *reinterpret_cast<const uint64_t *>(end - 8);
        res = intHash64(word ^ res);

        return res;
    }
};


struct VerySimpleHash
{
    size_t operator() (StringRef x) const
    {
        const char * pos = x.data;
        size_t size = x.size;

        const char * end = pos + size;

        size_t res = 0;

        if (size == 0)
            return 0;

        if (size < 8)
        {
#if __SSE4_1__
            return hashLessThan8(x.data, x.size);
#endif
        }

        while (pos + 8 < end)
        {
            res ^= reinterpret_cast<const uint64_t *>(pos)[0];
            res ^= res >> 33;
            res *= 0xff51afd7ed558ccdULL;

            pos += 8;
        }

        res ^= *reinterpret_cast<const uint64_t *>(end - 8);
        res ^= res >> 33;
        res *= 0xc4ceb9fe1a85ec53ULL;
        res ^= res >> 33;

        return res;
    }
};


struct FarmHash64
{
    size_t operator() (StringRef x) const
    {
        return farmhash::Hash64(x.data, x.size);
    }
};


template <void metrohash64(const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * out)>
struct MetroHash64
{
    size_t operator() (StringRef x) const
    {
        union
        {
            uint64_t u64;
            std::uint8_t u8[sizeof(u64)];
        };

        metrohash64(reinterpret_cast<const std::uint8_t *>(x.data), x.size, 0, u8);

        return u64;
    }
};


#if __SSE4_1__

/*struct CRC32Hash
{
    size_t operator() (StringRef x) const
    {
        const char * pos = x.data;
        size_t size = x.size;

        if (size == 0)
            return 0;

        if (size < 8)
        {
            return hashLessThan8(x.data, x.size);
        }

        const char * end = pos + size;
        size_t res = -1ULL;

        do
        {
            uint64_t word = *reinterpret_cast<const uint64_t *>(pos);
            res = _mm_crc32_u64(res, word);

            pos += 8;
        } while (pos + 8 < end);

        uint64_t word = *reinterpret_cast<const uint64_t *>(end - 8);
        res = _mm_crc32_u64(res, word);

        return res;
    }
};*/


struct CRC32ILPHash
{
    size_t operator() (StringRef x) const
    {
        const char * pos = x.data;
        size_t size = x.size;

        if (size == 0)
            return 0;

        if (size < 16)
        {
            return hashLessThan16(x.data, x.size);
        }

        const char * end = pos + size;
        const char * end_16 = pos + size / 16 * 16;
        size_t res0 = -1ULL;
        size_t res1 = -1ULL;

        do
        {
            uint64_t word0 = reinterpret_cast<const uint64_t *>(pos)[0];
            uint64_t word1 = reinterpret_cast<const uint64_t *>(pos)[1];
            res0 = _mm_crc32_u64(res0, word0);
            res1 = _mm_crc32_u64(res1, word1);

            pos += 16;
        } while (pos < end_16);

        uint64_t word0 = *reinterpret_cast<const uint64_t *>(end - 8);
        uint64_t word1 = *reinterpret_cast<const uint64_t *>(end - 16);

    /*    return HashLen16(Rotate(word0 - word1, 43) + Rotate(res0, 30) + res1,
            word0 + Rotate(word1 ^ k3, 20) - res0 + size);*/

        res0 = _mm_crc32_u64(res0, word0);
        res1 = _mm_crc32_u64(res1, word1);

        return hashLen16(res0, res1);
    }
};

#endif


using Value = uint64_t;


template <typename Key, typename Hash>
void NO_INLINE bench(const std::vector<StringRef> & data, const char * name)
{
    Stopwatch watch;

    using Map = HashMapWithSavedHash<Key, Value, Hash>;

    Map map;
    typename Map::iterator it;
    bool inserted;

    for (size_t i = 0, size = data.size(); i < size; ++i)
    {
        map.emplace(static_cast<const Key &>(data[i]), it, inserted);
        if (inserted)
            it->second = 0;
        ++it->second;
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

    size_t n = atoi(argv[1]);
    size_t m = atoi(argv[2]);

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

    if (!m || m == 1) bench<StringRef, StringRefHash64>(data, "StringRef_CityHash64");
    if (!m || m == 2) bench<StringRef, FastHash64>     (data, "StringRef_FastHash64");
    if (!m || m == 3) bench<StringRef, SimpleHash>     (data, "StringRef_SimpleHash");
    if (!m || m == 4) bench<StringRef, FNV1a>          (data, "StringRef_FNV1a");

#if __SSE4_1__
    if (!m || m == 5) bench<StringRef, CrapWow>        (data, "StringRef_CrapWow");
    if (!m || m == 6) bench<StringRef, CRC32Hash>      (data, "StringRef_CRC32Hash");
    if (!m || m == 7) bench<StringRef, CRC32ILPHash>   (data, "StringRef_CRC32ILPHash");
#endif

    if (!m || m == 8) bench<StringRef, VerySimpleHash> (data, "StringRef_VerySimpleHash");
    if (!m || m == 9) bench<StringRef, FarmHash64>     (data, "StringRef_FarmHash64");
    if (!m || m == 10) bench<StringRef, MetroHash64<metrohash64_1>>(data, "StringRef_MetroHash64_1");
    if (!m || m == 11) bench<StringRef, MetroHash64<metrohash64_2>>(data, "StringRef_MetroHash64_2");

    return 0;
}
