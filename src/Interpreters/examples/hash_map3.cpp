#define DBMS_HASH_MAP_DEBUG_RESIZES
#define DBMS_HASH_MAP_COUNT_COLLISIONS

#include <iostream>
#include <cstring>
#include <cstdlib>

#include <base/types.h>
#include <Common/Exception.h>

#include <IO/ReadHelpers.h>

#include <Common/HashTable/HashMap.h>


template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
class HashMapWithDump : public HashMap<Key, Mapped, Hash, Grower, Allocator>
{
public:
    void dump() const
    {
        for (size_t i = 0; i < this->grower.bufSize(); ++i)
        {
            if (this->buf[i].isZero(*this))
                std::cerr << "[    ]";
            else
                std::cerr << '[' << this->buf[i].getValue().first.data() << ", " << this->buf[i].getValue().second << ']';
        }
        std::cerr << std::endl;
    }
};


struct SimpleHash
{
    size_t operator() (UInt64 x) const { return x; }
    size_t operator() (std::string_view x) const { return DB::parse<UInt64>(x.data()); } /// NOLINT(bugprone-suspicious-stringview-data-usage)
};

struct Grower : public HashTableGrower<2>
{
    void increaseSize()
    {
        ++size_degree;
    }
};

int main(int, char **)
{
    using Map = HashMapWithDump<
        std::string_view,
        UInt64,
        SimpleHash,
        Grower,
        HashTableAllocatorWithStackMemory<
            4 * sizeof(HashMapCell<std::string_view, UInt64, SimpleHash>)>>;

    Map map;

    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    map[std::string_view("1", 1)] = 1;
    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    map[std::string_view("9", 1)] = 1;
    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    std::cerr << "Collisions: " << map.getCollisions() << std::endl;
    map[std::string_view("3", 1)] = 2;
    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    std::cerr << "Collisions: " << map.getCollisions() << std::endl;

    for (auto x : map)
        std::cerr << x.getKey() << " -> " << x.getMapped() << std::endl;

    return 0;
}
