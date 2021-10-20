#include <iomanip>
#include <iostream>
#include <vector>

#include <Common/Stopwatch.h>

#define DBMS_HASH_MAP_COUNT_COLLISIONS
#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <Compression/CompressedReadBuffer.h>
#include <common/types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>

/** Do this:
for file in ResolutionWidth ResolutionDepth; do
    for size in 30000 100000 300000 1000000 5000000; do
        echo
        BEST_METHOD=0
        BEST_RESULT=0
        for method in {1..2}; do
            echo -ne $file $size $method '';
            TOTAL_ELEMS=0
            for i in {0..1000}; do
                TOTAL_ELEMS=$(( $TOTAL_ELEMS + $size ))
                if [[ $TOTAL_ELEMS -gt 25000000 ]]; then break; fi
                ./hash_map_lookup $size $method < ${file}.bin 2>&1 |
                    grep HashMap | grep -oE '[0-9\.]+ elem';
            done | awk -W interactive '{ if ($1 > x) { x = $1 }; printf(".") } END { print x }' | tee /tmp/hash_map_lookup_res;
            CUR_RESULT=$(cat /tmp/hash_map_lookup_res | tr -d '.')
            if [[ $CUR_RESULT -gt $BEST_RESULT ]]; then
                BEST_METHOD=$method
                BEST_RESULT=$CUR_RESULT
            fi;
        done;
        echo Best: $BEST_METHOD - $BEST_RESULT
    done;
done
*/


template <typename Map>
void NO_INLINE bench(const std::vector<UInt16> & data, const char * name)
{
    Map map;

    Stopwatch watch;
    for (auto value : data)
    {
        typename Map::LookupResult it;
        bool inserted;

        map.emplace(value, it, inserted);
        if (inserted)
            it->getMapped() = 1;
        else
            ++it->getMapped();
    }

    for (auto value : data)
    {
        auto it = map.find(value);
        auto curr = ++it;
        if (curr)
            curr->getMapped();
    }
    watch.stop();
    std::cerr << std::fixed << std::setprecision(2) << "HashMap (" << name << "). Size: " << map.size()
              << ", elapsed: " << watch.elapsedSeconds() << " (" << data.size() / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
              << ", collisions: " << map.getCollisions()
#endif
              << std::endl;
}

template <typename Map>
void insert(Map & map, StringRef & k)
{
    bool inserted;
    typename Map::LookupResult it;
    map.emplace(k, it, inserted, nullptr);
    if (inserted)
        it->getMapped() = 1;
    else
        ++it->getMapped();
    std::cout << map.find(k)->getMapped() << std::endl;
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

    std::vector<UInt16> data(n);

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);
        for (size_t i = 0; i < n && !in2.eof(); ++i)
        {
            DB::readBinary(data[i], in2);
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2) << "Vector. Size: " << n << ", elapsed: " << watch.elapsedSeconds() << " ("
                  << n / watch.elapsedSeconds() << " elem/sec.)" << std::endl;
    }

    using OldLookup = HashMap<UInt16, UInt8, TrivialHash, HashTableFixedGrower<16>>;
    using NewLookup = FixedHashMap<UInt16, UInt8>;

    if (!m || m == 1)
        bench<OldLookup>(data, "Old Lookup");
    if (!m || m == 2)
        bench<NewLookup>(data, "New Lookup");
    return 0;
}
