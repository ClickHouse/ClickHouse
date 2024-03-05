#include <iostream>
#include <iomanip>
#include <vector>

#include <unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

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

#pragma clang diagnostic ignored "-Wgnu-anonymous-struct"


struct CompactStringRef
{
    union
    {
        const char * data_mixed = nullptr;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnested-anon-types"
        struct
        {
            char dummy[6];
            UInt16 size;
        };
#pragma clang diagnostic pop
    };

    CompactStringRef(const char * data_, size_t size_)
    {
        data_mixed = data_;
        size = size_;
    }

    CompactStringRef(const unsigned char * data_, size_t size_) : CompactStringRef(reinterpret_cast<const char *>(data_), size_) {}
    explicit CompactStringRef(const std::string & s) : CompactStringRef(s.data(), s.size()) {}
    CompactStringRef() = default;

    const char * data() const { return reinterpret_cast<const char *>(reinterpret_cast<intptr_t>(data_mixed) & 0x0000FFFFFFFFFFFFULL); }

    std::string toString() const { return std::string(data(), size); }
};

inline bool operator==(CompactStringRef lhs, CompactStringRef rhs)
{
    if (lhs.size != rhs.size)
        return false;

    const char * lhs_data = lhs.data();
    const char * rhs_data = rhs.data();
    for (size_t pos = lhs.size - 1; pos < lhs.size; --pos)
        if (lhs_data[pos] != rhs_data[pos])
            return false;

    return true;
}

namespace ZeroTraits
{
    template <>
    inline bool check<CompactStringRef>(CompactStringRef x) { return nullptr == x.data_mixed; }

    template <>
    inline void set<CompactStringRef>(CompactStringRef & x) { x.data_mixed = nullptr; }
}

template <>
struct DefaultHash<CompactStringRef>
{
    size_t operator() (CompactStringRef x) const
    {
        return CityHash_v1_0_2::CityHash64(x.data(), x.size);
    }
};


static inline UInt64 mix(UInt64 h)
{
    h ^= h >> 23;
    h *= 0x2127599bf4325c37ULL;
    h ^= h >> 47;
    return h;
}

struct FastHash64
{
    size_t operator() (CompactStringRef x) const
    {
        const char * buf = x.data();
        size_t len = x.size;

        const UInt64    m = 0x880355f21e6d1965ULL;
        const UInt64 *pos = reinterpret_cast<const UInt64 *>(buf);
        const UInt64 *end = pos + (len / 8);
        const unsigned char *pos2;
        UInt64 h = len * m;
        UInt64 v;

        while (pos != end)
        {
            v = *pos++;
            h ^= mix(v);
            h *= m;
        }

        pos2 = reinterpret_cast<const unsigned char*>(pos);
        v = 0;

        switch (len & 7) // NOLINT(bugprone-switch-missing-default-case)
        {
            case 7: v ^= static_cast<UInt64>(pos2[6]) << 48; [[fallthrough]];
            case 6: v ^= static_cast<UInt64>(pos2[5]) << 40; [[fallthrough]];
            case 5: v ^= static_cast<UInt64>(pos2[4]) << 32; [[fallthrough]];
            case 4: v ^= static_cast<UInt64>(pos2[3]) << 24; [[fallthrough]];
            case 3: v ^= static_cast<UInt64>(pos2[2]) << 16; [[fallthrough]];
            case 2: v ^= static_cast<UInt64>(pos2[1]) << 8; [[fallthrough]];
            case 1: v ^= static_cast<UInt64>(pos2[0]);
                h ^= mix(v);
                h *= m;
        }

        return mix(h);
    }
};


#if defined(__x86_64__)
struct CrapWow
{
    size_t operator() (CompactStringRef x) const
    {
        const char * key = x.data();
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
    size_t operator() (CompactStringRef x) const
    {
        const char * pos = x.data();
        size_t size = x.size;

        const char * end = pos + size;

        size_t res = 0;

        if (size == 0)
            return 0;

        if (size < 8)
        {
            memcpy(reinterpret_cast<char *>(&res), pos, size);
            return intHash64(res);
        }

        while (pos + 8 < end)
        {
            UInt64 word = *reinterpret_cast<const UInt64 *>(pos);
            res = intHash64(word ^ res);

            pos += 8;
        }

        UInt64 word = *reinterpret_cast<const UInt64 *>(end - 8);
        res = intHash64(word ^ res);

        return res;
    }
};


using Key = CompactStringRef;
using Value = UInt64;


struct Grower : public HashTableGrower<>
{
    /// The state of this structure is enough to get the buffer size of the hash table.

    /// Defines the initial size of the hash table.
    static const size_t initial_size_degree = 16;
    Grower() { size_degree = initial_size_degree; }

    size_t max_fill = (1ULL << initial_size_degree) * 9 / 10;

    /// The size of the hash table in the cells.
    size_t bufSize() const               { return 1ULL << size_degree; }

    size_t maxFill() const               { return max_fill /*1 << (size_degree - 1)*/; }
    size_t mask() const                  { return bufSize() - 1; }

    /// From the hash value, get the cell number in the hash table.
    size_t place(size_t x) const         { return x & mask(); }

    /// The next cell in the collision resolution chain.
    size_t next(size_t pos) const        { ++pos; return pos & mask(); }

    /// Whether the hash table is sufficiently full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const    { return elems > maxFill(); }

    /// Increase the size of the hash table.
    void increaseSize()
    {
        size_degree += size_degree >= 23 ? 1 : 2;
        max_fill = (1ULL << size_degree) * 9 / 10;
    }

    /// Set the buffer size by the number of elements in the hash table. Used when deserializing a hash table.
    [[noreturn]] static void set(size_t /*num_elems*/)
    {
        throw Poco::Exception(__PRETTY_FUNCTION__);
    }
};


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
    std::vector<Key> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        std::string tmp;
        for (size_t i = 0; i < n && !in2.eof(); ++i)
        {
            DB::readStringBinary(tmp, in2);
            data[i] = Key(pool.insert(tmp.data(), tmp.size()), tmp.size());
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (!m || m == 1)
    {
        Stopwatch watch;

        //using Map = HashMap<Key, Value>;

        /// Saving the hash accelerates the resize by about 2 times, and the overall performance by 6-8%.
        using Map = HashMapWithSavedHash<Key, Value, DefaultHash<Key>, Grower>;

        Map map;
        Map::LookupResult it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
                it->getMapped() = 0;
            ++it->getMapped();
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap (CityHash64). Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }

    if (!m || m == 2)
    {
        Stopwatch watch;

        using Map = HashMapWithSavedHash<Key, Value, FastHash64, Grower>;

        Map map;
        Map::LookupResult it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
                it->getMapped() = 0;
            ++it->getMapped();
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap (FastHash64). Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }

#if defined(__x86_64__)
    if (!m || m == 3)
    {
        Stopwatch watch;

        using Map = HashMapWithSavedHash<Key, Value, CrapWow, Grower>;

        Map map;
        Map::LookupResult it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
                it->getMapped() = 0;
            ++it->getMapped();
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap (CrapWow). Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }
#endif

    if (!m || m == 4)
    {
        Stopwatch watch;

        using Map = HashMapWithSavedHash<Key, Value, SimpleHash, Grower>;

        Map map;
        Map::LookupResult it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
                it->getMapped() = 0;
            ++it->getMapped();
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap (SimpleHash). Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }

    if (!m || m == 5)
    {
        Stopwatch watch;

        std::unordered_map<Key, Value, DefaultHash<Key>> map;
        for (size_t i = 0; i < n; ++i)
            ++map[data[i]];

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "std::unordered_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (!m || m == 6)
    {
        Stopwatch watch;

        google::dense_hash_map<Key, Value, DefaultHash<Key>> map;
        map.set_empty_key(Key("\0", 1));
        for (size_t i = 0; i < n; ++i)
              ++map[data[i]];

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "google::dense_hash_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (!m || m == 7)
    {
        Stopwatch watch;

        google::sparse_hash_map<Key, Value, DefaultHash<Key>> map;
        for (size_t i = 0; i < n; ++i)
            ++map[data[i]];

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "google::sparse_hash_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    return 0;
}
