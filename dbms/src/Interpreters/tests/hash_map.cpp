#include <iostream>
#include <iomanip>
#include <vector>

#include <unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

#include <Common/Stopwatch.h>
/*
#define DBMS_HASH_MAP_COUNT_COLLISIONS
*/
#include <Core/Types.h>
#include <Core/Row.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <Common/HashTable/HashMap.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


/** The test checks the speed of hash tables, simulating their use for aggregation.
  * The first argument specifies the number of elements to be inserted.
  * The second argument can be a number from 1 to 4 - the number of the data structure being tested.
  * This is important, because if you run all the tests one by one, the results will be incorrect.
  * (Due to the peculiarities of the work of the allocator, the first test takes advantage.)
  *
  * Depending on USE_AUTO_ARRAY, one of the structures is selected as the value.
  * USE_AUTO_ARRAY = 0 - uses std::vector (hard-copy structure, sizeof = 24 bytes).
  * USE_AUTO_ARRAY = 1 - uses AutoArray (a structure specially designed for such cases, sizeof = 8 bytes).
  *
  * That is, the test also allows you to compare AutoArray and std::vector.
  *
  * If USE_AUTO_ARRAY = 0, then HashMap confidently overtakes all.
  * If USE_AUTO_ARRAY = 1, then HashMap is slightly less serious (20%) ahead of google::dense_hash_map.
  *
  * When using HashMap, AutoArray has a rather serious (40%) advantage over std::vector.
  * And when using other hash tables, AutoArray even more seriously overtakes std::vector
  *  (up to three and a half times in the case of std::unordered_map and google::sparse_hash_map).
  *
  * HashMap, unlike google::dense_hash_map, much more depends on the quality of the hash function.
  *
  * PS. Measure everything yourself, otherwise I'm almost confused.
  *
  * PPS. Now the aggregation does not use an array of aggregate functions as values.
  * States of aggregate functions were separated from the interface to manipulate them, and put in the pool.
  * But in this test, there was something similar to the old scenario of using hash tables in the aggregation.
  */

#define USE_AUTO_ARRAY    0


struct AlternativeHash
{
    size_t operator() (UInt64 x) const
    {
        x ^= x >> 23;
        x *= 0x2127599bf4325c37ULL;
        x ^= x >> 47;

        return x;
    }
};


#if defined(__x86_64__)

struct CRC32Hash_
{
    size_t operator() (UInt64 x) const
    {
        UInt64 crc = -1ULL;
        asm("crc32q %[x], %[crc]\n" : [crc] "+r" (crc) : [x] "rm" (x));
        return crc;
    }
};

#endif


int main(int argc, char ** argv)
{
    using namespace DB;

    using Key = UInt64;

#if USE_AUTO_ARRAY
    using Value = AutoArray<IAggregateFunction*>;
#else
    using Value = std::vector<IAggregateFunction*>;
#endif

    size_t n = argc < 2 ? 10000000 : atoi(argv[1]);
    //size_t m = atoi(argv[2]);

    AggregateFunctionFactory factory;
    DataTypes data_types_empty;
    DataTypes data_types_uint64;
    data_types_uint64.push_back(std::make_shared<DataTypeUInt64>());

    std::vector<Key> data(n);
    Value value;

    AggregateFunctionPtr func_count = factory.get("count", data_types_empty);
    AggregateFunctionPtr func_avg = factory.get("avg", data_types_uint64);
    AggregateFunctionPtr func_uniq = factory.get("uniq", data_types_uint64);

    #define INIT                \
    {                            \
        value.resize(3);        \
                                \
        value[0] = func_count.get();\
        value[1] = func_avg.get();    \
        value[2] = func_uniq.get();    \
    }

    INIT;

#ifndef USE_AUTO_ARRAY
    #undef INIT
    #define INIT
#endif

    Row row(1);
    row[0] = UInt64(0);

    std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
    /*    for (size_t i = 0; i < n; ++i)
            data[i] = rand() % m;

        for (size_t i = 0; i < n; i += 10)
            data[i] = 0;*/

        ReadBufferFromFile in1("UniqID.bin");
        CompressedReadBuffer in2(in1);

        in2.readStrict(reinterpret_cast<char*>(&data[0]), sizeof(data[0]) * n);

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (argc < 3 || atoi(argv[2]) == 1)
    {
        Stopwatch watch;

        HashMap<Key, Value> map;
        HashMap<Key, Value>::iterator it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
            {
                new(&it->second) Value(std::move(value));
                INIT;
            }
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }

    if (argc < 3 || atoi(argv[2]) == 2)
    {
        Stopwatch watch;

        using Map = HashMap<Key, Value, AlternativeHash>;
        Map map;
        Map::iterator it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
            {
                new(&it->second) Value(std::move(value));
                INIT;
            }
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap, AlternativeHash. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }

#if defined(__x86_64__)
    if (argc < 3 || atoi(argv[2]) == 3)
    {
        Stopwatch watch;

        using Map = HashMap<Key, Value, CRC32Hash_>;
        Map map;
        Map::iterator it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
            {
                new(&it->second) Value(std::move(value));
                INIT;
            }
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "HashMap, CRC32Hash. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            << ", collisions: " << map.getCollisions()
#endif
            << std::endl;
    }
#endif

    if (argc < 3 || atoi(argv[2]) == 4)
    {
        Stopwatch watch;

        std::unordered_map<Key, Value, DefaultHash<Key>> map;
        std::unordered_map<Key, Value, DefaultHash<Key>>::iterator it;
        for (size_t i = 0; i < n; ++i)
        {
            it = map.insert(std::make_pair(data[i], std::move(value))).first;
            INIT;
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "std::unordered_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (argc < 3 || atoi(argv[2]) == 5)
    {
        Stopwatch watch;

        google::dense_hash_map<Key, Value, DefaultHash<Key>> map;
        google::dense_hash_map<Key, Value, DefaultHash<Key>>::iterator it;
        map.set_empty_key(-1ULL);
        for (size_t i = 0; i < n; ++i)
        {
            it = map.insert(std::make_pair(data[i], std::move(value))).first;
            INIT;
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "google::dense_hash_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (argc < 3 || atoi(argv[2]) == 6)
    {
        Stopwatch watch;

        google::sparse_hash_map<Key, Value, DefaultHash<Key>> map;
        google::sparse_hash_map<Key, Value, DefaultHash<Key>>::iterator it;
        for (size_t i = 0; i < n; ++i)
        {
            map.insert(std::make_pair(data[i], std::move(value)));
            INIT;
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "google::sparse_hash_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    return 0;
}
