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

    explicit LRUHashMapBasic(size_t max_size_, bool preallocated = false)
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

        auto & iterator_in_list_to_update = it->second;

        list.splice(list.end(), list, iterator_in_list_to_update);
        iterator_in_list_to_update = --list.end();

        return std::make_pair(&iterator_in_list_to_update->second, false);
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
