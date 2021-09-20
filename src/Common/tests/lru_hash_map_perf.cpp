#include <vector>
#include <list>
#include <map>
#include <random>
#include <pcg_random.hpp>

#include <Common/Stopwatch.h>
#include <Common/HashTable/LRUHashMap.h>

template<class Key, class Value>
class LRUHashMapBasic
{
public:
    using key_type = Key;
    using value_type = Value;
    using list_type = std::list<key_type>;
    using node = std::pair<value_type, typename list_type::iterator>;
    using map_type = std::unordered_map<key_type, node, DefaultHash<Key>>;

    LRUHashMapBasic(size_t max_size_, bool preallocated)
        : hash_map(preallocated ? max_size_ : 32)
        , max_size(max_size_)
    {
    }

    void insert(const Key &key, const Value &value)
    {
        auto it = hash_map.find(key);

        if (it == hash_map.end())
        {
            if (size() >= max_size)
            {
                auto iterator_to_remove = list.begin();

                hash_map.erase(*iterator_to_remove);
                list.erase(iterator_to_remove);
            }

            list.push_back(key);
            hash_map[key] = std::make_pair(value, --list.end());
        }
        else
        {
            auto & [value_to_update, iterator_in_list_to_update] = it->second;

            list.splice(list.end(), list, iterator_in_list_to_update);

            iterator_in_list_to_update = list.end();
            value_to_update = value;
        }
    }

    value_type & get(const key_type &key)
    {
        auto iterator_in_map = hash_map.find(key);
        assert(iterator_in_map != hash_map.end());

        auto & [value_to_return, iterator_in_list_to_update] = iterator_in_map->second;

        list.splice(list.end(), list, iterator_in_list_to_update);
        iterator_in_list_to_update = list.end();

        return value_to_return;
    }

    const value_type & get(const key_type & key) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->get(key);
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

std::vector<UInt64> generateNumbersToInsert(size_t numbers_to_insert_size)
{
    std::vector<UInt64> numbers;
    numbers.reserve(numbers_to_insert_size);

    std::random_device rd;
    pcg64 gen(rd());

    UInt64 min = std::numeric_limits<UInt64>::min();
    UInt64 max = std::numeric_limits<UInt64>::max();

    auto distribution = std::uniform_int_distribution<>(min, max);

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
    {
        UInt64 number = distribution(gen);
        numbers.emplace_back(number);
    }

    return numbers;
}

void testInsertElementsIntoHashMap(size_t map_size, const std::vector<UInt64> & numbers_to_insert, bool preallocated)
{
    size_t numbers_to_insert_size = numbers_to_insert.size();
    std::cout << "TestInsertElementsIntoHashMap preallocated map size: " << map_size << " numbers to insert size: " << numbers_to_insert_size;
    std::cout << std::endl;

    HashMap<int, int> hash_map(preallocated ? map_size : 32);

    Stopwatch watch;

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
        hash_map.insert({ numbers_to_insert[i], numbers_to_insert[i] });

    std::cout << "Inserted in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;

    UInt64 summ = 0;

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
    {
        auto * it = hash_map.find(numbers_to_insert[i]);

        if (it)
            summ += it->getMapped();
    }

    std::cout << "Calculated summ: " << summ << " in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;
}

void testInsertElementsIntoStandardMap(size_t map_size, const std::vector<UInt64> & numbers_to_insert, bool preallocated)
{
    size_t numbers_to_insert_size = numbers_to_insert.size();
    std::cout << "TestInsertElementsIntoStandardMap map size: " << map_size << " numbers to insert size: " << numbers_to_insert_size;
    std::cout << std::endl;

    std::unordered_map<int, int> hash_map(preallocated ? map_size : 32);

    Stopwatch watch;

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
        hash_map.insert({ numbers_to_insert[i], numbers_to_insert[i] });

    std::cout << "Inserted in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;

    UInt64 summ = 0;

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
    {
        auto it = hash_map.find(numbers_to_insert[i]);

        if (it != hash_map.end())
            summ += it->second;
    }

    std::cout << "Calculated summ: " << summ << " in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;
}

template<typename LRUCache>
UInt64 testInsertIntoEmptyCache(size_t map_size, const std::vector<UInt64> & numbers_to_insert, bool preallocated)
{
    size_t numbers_to_insert_size = numbers_to_insert.size();
    std::cout << "Test testInsertPreallocated preallocated map size: " << map_size << " numbers to insert size: " << numbers_to_insert_size;
    std::cout << std::endl;

    LRUCache cache(map_size, preallocated);
    Stopwatch watch;

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
    {
        cache.insert(numbers_to_insert[i], numbers_to_insert[i]);
    }

    std::cout << "Inserted in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;

    UInt64 summ = 0;

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
        if (cache.contains(numbers_to_insert[i]))
            summ += cache.get(numbers_to_insert[i]);

    std::cout << "Calculated summ: " << summ << " in " << watch.elapsedMilliseconds() << " milliseconds" << std::endl;

    return summ;
}

int main(int argc, char ** argv)
{
    (void)(argc);
    (void)(argv);

    size_t hash_map_size = 1200000;
    size_t numbers_to_insert_size = 12000000;
    std::vector<UInt64> numbers = generateNumbersToInsert(numbers_to_insert_size);

    std::cout << "Test insert into HashMap preallocated=0" << std::endl;
    testInsertElementsIntoHashMap(hash_map_size, numbers, true);
    std::cout << std::endl;

    std::cout << "Test insert into HashMap preallocated=1" << std::endl;
    testInsertElementsIntoHashMap(hash_map_size, numbers, true);
    std::cout << std::endl;

    std::cout << "Test LRUHashMap preallocated=0" << std::endl;
    testInsertIntoEmptyCache<LRUHashMap<UInt64, UInt64>>(hash_map_size, numbers, false);
    std::cout << std::endl;

    std::cout << "Test LRUHashMap preallocated=1" << std::endl;
    testInsertIntoEmptyCache<LRUHashMap<UInt64, UInt64>>(hash_map_size, numbers, true);
    std::cout << std::endl;

    std::cout << "Test LRUHashMapBasic preallocated=0" << std::endl;
    testInsertIntoEmptyCache<LRUHashMapBasic<UInt64, UInt64>>(hash_map_size, numbers, false);
    std::cout << std::endl;

    std::cout << "Test LRUHashMapBasic preallocated=1" << std::endl;
    testInsertIntoEmptyCache<LRUHashMapBasic<UInt64, UInt64>>(hash_map_size, numbers, true);
    std::cout << std::endl;

    return 0;
}
