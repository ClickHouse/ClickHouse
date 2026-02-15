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
#include <Common/HashTable/HashMap.h>
#include <Interpreters/AggregationCommon.h>


struct SmallStringView
{
    size_t size = 0;

    union
    {
        const char * data_big;
        char data_small[12];
    };

    bool isSmall() const { return size <= 12; }

    const char * data() const
    {
        return isSmall() ? data_small : data_big;
    }

    SmallStringView(const char * data_, size_t size_)
    {
        size = size_;

        if (isSmall())
            memcpy(data_small, data_, size_);
        else
            data_big = data_;
    }

    SmallStringView(const unsigned char * data_, size_t size_) : SmallStringView(reinterpret_cast<const char *>(data_), size_) {}
    explicit SmallStringView(const std::string & s) : SmallStringView(s.data(), s.size()) {}
    SmallStringView() = default;

    std::string toString() const { return std::string(data(), size); }
};


inline bool operator==(SmallStringView lhs, SmallStringView rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
    return memequalWide(lhs.data(), rhs.data(), lhs.size);
#else
    return 0 == memcmp(lhs.data(), rhs.data(), lhs.size);
#endif
}


namespace ZeroTraits
{
    template <>
    inline bool check<SmallStringView>(SmallStringView x) { return x.size == 0; }

    template <>
    inline void set<SmallStringView>(SmallStringView & x) { x.size = 0; }
}

template <>
struct DefaultHash<SmallStringView>
{
    size_t operator() (SmallStringView x) const
    {
        return DefaultHash<std::string_view>()(std::string_view(x.data(), x.size));
    }
};


using Value = UInt64;


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
    std::vector<std::string_view> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(SmallStringView) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        std::string tmp;
        for (size_t i = 0; i < n && !in2.eof(); ++i)
        {
            DB::readStringBinary(tmp, in2);
            data[i] = std::string_view(pool.insert(tmp.data(), tmp.size()), tmp.size());
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << static_cast<double>(n) / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (!m || m == 1)
    {
        Stopwatch watch;

        using Map = HashMapWithSavedHash<std::string_view, Value>;

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
            << "HashMap (std::string_view). Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << static_cast<double>(n) / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }

    if (!m || m == 2)
    {
        Stopwatch watch;

        using Map = HashMapWithSavedHash<SmallStringView, Value>;

        Map map;
        Map::LookupResult it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(SmallStringView(data[i].data(), data[i].size()), it, inserted);
            if (inserted)
                it->getMapped() = 0;
            ++it->getMapped();
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap (SmallStringView). Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << static_cast<double>(n) / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }

    return 0;
}
