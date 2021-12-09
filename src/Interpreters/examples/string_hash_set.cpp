#include <iomanip>
#include <iostream>
#include <vector>
#include <Compression/CompressedReadBuffer.h>
#include <base/types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/StringHashSet.h>
#include <Common/Stopwatch.h>
#include <base/StringRef.h>

/// NOTE: see string_hash_map.cpp for usage example

template <typename Set>
void NO_INLINE bench(const std::vector<StringRef> & data, DB::Arena & pool, const char * name)
{
    std::cerr << "method " << name << std::endl;
    for (auto t = 0ul; t < 7; ++t)
    {
        Stopwatch watch;
        Set set;
        typename Set::LookupResult it;
        bool inserted;

        for (const auto & value : data)
        {
            if constexpr (std::is_same_v<StringHashSet<>, Set>)
                set.emplace(DB::ArenaKeyHolder{value, pool}, inserted);
            else
                set.emplace(DB::ArenaKeyHolder{value, pool}, it, inserted);
        }
        watch.stop();

        std::cerr << "arena-memory " << pool.size() + set.getBufferSizeInBytes() << std::endl;
        std::cerr << "single-run " << std::setprecision(3)
                  << watch.elapsedSeconds() << std::endl;
    }
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

    DB::Arena pool(128 * 1024 * 1024);
    std::vector<StringRef> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(StringRef) << std::endl;

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
        std::cerr << std::fixed << std::setprecision(2) << "Vector. Size: " << n << ", elapsed: " << watch.elapsedSeconds() << " ("
                  << n / watch.elapsedSeconds() << " elem/sec.)" << std::endl;
    }

    if (!m || m == 1)
        bench<StringHashSet<>>(data, pool, "StringHashSet");
    if (!m || m == 2)
        bench<HashSetWithSavedHash<StringRef>>(data, pool, "HashSetWithSavedHash");
    if (!m || m == 3)
        bench<HashSet<StringRef>>(data, pool, "HashSet");
    return 0;
}
