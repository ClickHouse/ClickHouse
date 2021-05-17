#include <vector>
#include <list>
#include <map>
#include <random>
#include <pcg_random.hpp>

#include <Common/Stopwatch.h>
#include <Common/HashTable/LRUHashMap.h>

#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>

template<class Key, class Value>
class LRUHashMapBasic
{
public:
    using key_type = Key;
    using value_type = Value;
    using list_type = std::list<std::pair<key_type, value_type>>;
    using map_type = std::unordered_map<key_type, typename list_type::iterator>;

    LRUHashMapBasic(size_t max_size_, bool preallocated = false)
        : hash_map(preallocated ? max_size_ : 32)
        , max_size(max_size_)
    {
    }

    template<typename ...Args>
    std::pair<Value *, bool> emplace(const Key &key, Args &&... args)
    {
        auto it = hash_map.find(key);

        if (it == hash_map.end())
        {
            if (size() >= max_size)
            {
                auto iterator_to_remove = list.begin();

                auto & key_to_remove = iterator_to_remove->first;
                hash_map.erase(key_to_remove);

                list.erase(iterator_to_remove);
            }


            Value value(std::forward<Args>(args)...);
            auto node = std::make_pair(key, std::move(value));

            list.push_back(std::move(node));

            auto inserted_iterator = --list.end();

            hash_map[key] = inserted_iterator;

            return std::make_pair(&inserted_iterator->second, true);
        }
        else
        {
            auto & iterator_in_list_to_update = it->second;

            list.splice(list.end(), list, iterator_in_list_to_update);
            iterator_in_list_to_update = --list.end();

            return std::make_pair(&iterator_in_list_to_update->second, false);
        }
    }

    value_type & operator[](const key_type & key)
    {
        auto [it, _] = emplace(key);
        return *it;
    }

    size_t getMaxSize() const
    {
        return max_size;
    }

    size_t size() const
    {
        return hash_map.size();
    }

    bool empty() const
    {
        return hash_map.empty();
    }

    bool contains(const Key & key)
    {
        return hash_map.find(key) != hash_map.end();
    }

    void clear()
    {
        hash_map.clear();
        list.clear();
    }

private:
    map_type hash_map;
    list_type list;
    size_t max_size;
};

// std::vector<UInt64> generateNumbersToInsert(size_t numbers_to_insert_size)
// {
//     std::vector<UInt64> numbers;
//     numbers.reserve(numbers_to_insert_size);

//     std::random_device rd;
//     pcg64 gen(rd());

//     UInt64 min = std::numeric_limits<UInt64>::min();
//     UInt64 max = std::numeric_limits<UInt64>::max();

//     auto distribution = std::uniform_int_distribution<>(min, max);

//     for (size_t i = 0; i < numbers_to_insert_size; ++i)
//     {
//         UInt64 number = distribution(gen);
//         numbers.emplace_back(number);
//     }

//     return numbers;
// }

// void testInsertElementsIntoHashMap(size_t map_size, const std::vector<UInt64> & numbers_to_insert, bool preallocated)
// {
//     size_t numbers_to_insert_size = numbers_to_insert.size();
//     std::cout << "TestInsertElementsIntoHashMap preallocated map size: " << map_size << " numbers to insert size: " << numbers_to_insert_size;
//     std::cout << std::endl;

//     HashMap<int, int> hash_map(preallocated ? map_size : 32);

//     Stopwatch watch;

//     for (size_t i = 0; i < numbers_to_insert_size; ++i)
//         hash_map.insert({ numbers_to_insert[i], numbers_to_insert[i] });

//     std::cout << "Inserted in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;

//     UInt64 summ = 0;

//     for (size_t i = 0; i < numbers_to_insert_size; ++i)
//     {
//         auto * it = hash_map.find(numbers_to_insert[i]);

//         if (it)
//             summ += it->getMapped();
//     }

//     std::cout << "Calculated summ: " << summ << " in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;
// }

// void testInsertElementsIntoStandardMap(size_t map_size, const std::vector<UInt64> & numbers_to_insert, bool preallocated)
// {
//     size_t numbers_to_insert_size = numbers_to_insert.size();
//     std::cout << "TestInsertElementsIntoStandardMap map size: " << map_size << " numbers to insert size: " << numbers_to_insert_size;
//     std::cout << std::endl;

//     std::unordered_map<int, int> hash_map(preallocated ? map_size : 32);

//     Stopwatch watch;

//     for (size_t i = 0; i < numbers_to_insert_size; ++i)
//         hash_map.insert({ numbers_to_insert[i], numbers_to_insert[i] });

//     std::cout << "Inserted in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;

//     UInt64 summ = 0;

//     for (size_t i = 0; i < numbers_to_insert_size; ++i)
//     {
//         auto it = hash_map.find(numbers_to_insert[i]);

//         if (it != hash_map.end())
//             summ += it->second;
//     }

//     std::cout << "Calculated summ: " << summ << " in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;
// }

template <typename Key, typename Map>
static void NO_INLINE test(const Key * data, size_t size, const std::string & name)
{
    size_t cache_size = size / 10;
    Map cache(cache_size);
    Stopwatch watch;

    for (size_t i = 0; i < size; ++i)
        ++cache[data[i]];

    watch.stop();

    std::cerr << name
        << ":\nElapsed: " << watch.elapsedSeconds()
        << " (" << size / watch.elapsedSeconds() << " elem/sec.)"
        << ", map size: " << cache.size() << "\n";
}

template <typename Key>
static void NO_INLINE testForType(size_t method, size_t rows_size)
{
    std::cerr << std::fixed << std::setprecision(3);

    std::vector<Key> data(rows_size);

    {
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);
        in2.readStrict(reinterpret_cast<char*>(data.data()), sizeof(data[0]) * rows_size);
    }

    if (method == 0)
    {
        test<Key, LRUHashMap<Key, UInt64>>(data.data(), data.size(), "CH HashMap");
    }
    else if (method == 1)
    {
        test<Key, LRUHashMapBasic<Key, UInt64>>(data.data(), data.size(), "BasicLRU");
    }
}

int main(int argc, char ** argv)
{
    (void)(argc);
    (void)(argv);

    // size_t hash_map_size = 1200000;
    // size_t numbers_to_insert_size = 12000000;
    // std::vector<UInt64> numbers = generateNumbersToInsert(numbers_to_insert_size);

    // std::cout << "Test insert into HashMap preallocated=0" << std::endl;
    // testInsertElementsIntoHashMap(hash_map_size, numbers, true);
    // std::cout << std::endl;

    // std::cout << "Test insert into HashMap preallocated=1" << std::endl;
    // testInsertElementsIntoHashMap(hash_map_size, numbers, true);
    // std::cout << std::endl;

    // std::cout << "Test LRUHashMap preallocated=0" << std::endl;
    // testInsertIntoEmptyCache<LRUHashMap<UInt64, UInt64>>(hash_map_size, numbers, false);
    // std::cout << std::endl;

    // std::cout << "Test LRUHashMap preallocated=1" << std::endl;
    // testInsertIntoEmptyCache<LRUHashMap<UInt64, UInt64>>(hash_map_size, numbers, true);
    // std::cout << std::endl;

    // std::cout << "Test LRUHashMapBasic preallocated=0" << std::endl;
    // testInsertIntoEmptyCache<LRUHashMapBasic<UInt64, UInt64>>(hash_map_size, numbers, false);
    // std::cout << std::endl;

    // std::cout << "Test LRUHashMapBasic preallocated=1" << std::endl;
    // testInsertIntoEmptyCache<LRUHashMapBasic<UInt64, UInt64>>(hash_map_size, numbers, true);
    // std::cout << std::endl;

    if (argc < 4)
    {
        std::cerr << "Usage: program method column_type_name rows_count < input_column.bin \n";
        return 1;
    }

    size_t method = std::stoull(argv[1]);
    std::string type_name = std::string(argv[2]);
    size_t n = std::stoull(argv[3]);

    if (type_name == "UInt8")
        testForType<UInt8>(method, n);
    else if (type_name == "UInt16")
        testForType<UInt16>(method, n);
    else if (type_name == "UInt32")
        testForType<UInt32>(method, n);
    else if (type_name == "UInt64")
        testForType<UInt64>(method, n);
    else if (type_name == "Int8")
        testForType<Int8>(method, n);
    else if (type_name == "Int16")
        testForType<Int16>(method, n);
    else if (type_name == "Int32")
        testForType<Int32>(method, n);
    else if (type_name == "Int64")
        testForType<Int64>(method, n);
    else
        std::cerr << "Unexpected type passed " << type_name << std::endl;

    return 0;
}
