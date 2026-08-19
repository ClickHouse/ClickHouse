#include <iostream>
#include <iomanip>
#include <vector>

#include <unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

//#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <Common/Stopwatch.h>
#include <AggregateFunctions/UniquesHashSet.h>

#include <base/types.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Common/HashTable/TwoLevelHashTable.h>
#include <Common/HashTable/HashMap.h>


using Key = UInt64;
using Value = UInt64;


int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "Usage: program n\n";
        return 1;
    }

    size_t n = std::stol(argv[1]);

    std::vector<Key> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        in2.readStrict(reinterpret_cast<char*>(data.data()), sizeof(data[0]) * n);

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    {
        Stopwatch watch;

        std::cerr << sizeof(HashMapCell<Key, Value, DefaultHash<Key>>) << std::endl;

        using Map = TwoLevelHashTable<Key, HashMapCell<Key, Value, DefaultHash<Key>>, DefaultHash<Key>, HashTableGrower<8>, HashTableAllocator>;

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
            << "HashMap. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;

        size_t sum_counts = 0;
        size_t elems = 0;
        for (const auto & kv : map)
        {
            sum_counts += kv.getMapped();
            ++elems;
        }

        std::cerr << "sum_counts: " << sum_counts << ", elems: " << elems << std::endl;
    }

    {
        Stopwatch watch;

        using Map = TwoLevelHashTable<Key, HashMapCell<Key, Value, DefaultHash<Key>>, DefaultHash<Key>, HashTableGrower<8>, HashTableAllocator>;
        //using Map = HashMap<Key, Value, UniquesHashSetDefaultHash>;

        Map map;
        Map::LookupResult it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(i, it, inserted);
            if (inserted)
                it->getMapped() = 0;
            ++it->getMapped();
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;

        size_t sum_counts = 0;
        size_t elems = 0;
        for (const auto & kv : map)
        {
            sum_counts += kv.getMapped();
            ++elems;

            if (kv.getKey() > n)
                std::cerr << kv.getKey() << std::endl;
        }

        std::cerr << "sum_counts: " << sum_counts << ", elems: " << elems << std::endl;

        if (sum_counts != n)
            std::cerr << "Error!" << std::endl;
    }

    return 0;
}
