#include <iostream>
#include <iomanip>
#include <vector>

#include <unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

#include <Common/Stopwatch.h>

//#define DBMS_HASH_MAP_COUNT_COLLISIONS
#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <Core/Types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <Common/HashTable/HashMap.h>


using Key = UInt64;
using Value = UInt64;

struct CellWithoutZeroWithSavedHash : public HashMapCell<Key, Value, DefaultHash<Key> >
{
//    size_t saved_hash;

    static constexpr bool need_zero_value_storage = false;

    CellWithoutZeroWithSavedHash() : HashMapCell() {}
    CellWithoutZeroWithSavedHash(const Key & key_, const State & state) : HashMapCell(key_, state) {}
    CellWithoutZeroWithSavedHash(const value_type & value_, const State & state) : HashMapCell(value_, state) {}

/*    bool keyEquals(const Key & key_) const { return value.first == key_; }
    bool keyEquals(const CellWithoutZeroWithSavedHash & other) const { return saved_hash == other.saved_hash && value.first == other.value.first; }

    void setHash(size_t hash_value) { saved_hash = hash_value; }
    size_t getHash(const DefaultHash<Key> & hash) const { return saved_hash; }*/
};

struct Grower : public HashTableGrower<>
{
    /// The state of this structure is enough to get the buffer size of the hash table.

    /// Specifies the initial size of the hash table.
    static const size_t initial_size_degree = 16;
    Grower() { size_degree = initial_size_degree; }

//    size_t max_fill = (1 << initial_size_degree) * 0.9;

    /// The size of the hash table in the cells.
    size_t bufSize() const                { return 1 << size_degree; }

    size_t maxFill() const                { return 1 << (size_degree - 1); }
//    size_t maxFill() const                { return max_fill; }

    size_t mask() const                    { return bufSize() - 1; }

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
//        max_fill = (1 << size_degree) * 0.9;
    }

    /// Set the buffer size by the number of elements in the hash table. Used when deserializing a hash table.
    void set(size_t num_elems)
    {
        throw Poco::Exception(__PRETTY_FUNCTION__);
    }
};


int main(int argc, char ** argv)
{
    size_t n = atoi(argv[1]);
    size_t m = atoi(argv[2]);

    std::vector<Key> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        in2.readStrict(reinterpret_cast<char*>(&data[0]), sizeof(data[0]) * n);

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (m == 1)
    {
        Stopwatch watch;

//        using Map = HashMap<Key, Value>;

        /// Due to `WithoutZero`, it's faster by 0.7% (if not fits into L3-cache) - 2.3% (if fits into L3-cache).
        using Map = HashMapTable<Key, CellWithoutZeroWithSavedHash, DefaultHash<Key>, Grower>;

        Map map;
        Map::iterator it;
        bool inserted;

        for (size_t i = 0; i < n; ++i)
        {
            map.emplace(data[i], it, inserted);
            if (inserted)
                it->second = 0;
            ++it->second;
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

    if (m == 2)
    {
        Stopwatch watch;

        std::unordered_map<Key, Value, DefaultHash<Key> > map;
        for (size_t i = 0; i < n; ++i)
            ++map[data[i]];

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "std::unordered_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (m == 3)
    {
        Stopwatch watch;

        google::dense_hash_map<Key, Value, DefaultHash<Key> > map;
        map.set_empty_key(-1ULL);
        for (size_t i = 0; i < n; ++i)
              ++map[data[i]];

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "google::dense_hash_map. Size: " << map.size()
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

    if (m == 4)
    {
        Stopwatch watch;

        google::sparse_hash_map<Key, Value, DefaultHash<Key> > map;
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
