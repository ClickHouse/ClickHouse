#include <iostream>
#include <iomanip>
#include <mutex>
#include <atomic>
#include <immintrin.h>

//#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
//#include <Common/HashTable/HashTableWithSmallLocks.h>
//#include <Common/HashTable/HashTableMerge.h>

#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>

#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>

/// Usage example:
/// for file in UserID URLHash RefererHash WatchID Title SearchPhrase URLDomain ClientIP RegionID; do echo -e "\n---------------------- $file ----------------------\n"; for method in 22 506 507; do ./src/Common/examples/parallel_aggregation 90000000 64 $method < src/Common/examples/${file}.bin; done; done


using ThreadFromGlobalPoolSimple = ThreadFromGlobalPoolImpl</* propagate_opentelemetry_context= */ false, /* global_trace_collector_allowed= */ false>;
using SimpleThreadPool = ThreadPoolImpl<ThreadFromGlobalPoolSimple>;

using Key = UInt64;
using Value = UInt64;

using Source = std::vector<Key>;

using Map = HashMap<Key, Value>;
using MapTwoLevel = TwoLevelHashMap<Key, Value>;


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

struct SmallLock
{
    std::atomic<int> locked {false};

    bool tryLock()
    {
        int expected = 0;
        return locked.compare_exchange_strong(expected, 1, std::memory_order_acquire);
    }

    void lock()
    {
        while (!tryLock())
        {
            /// Spin with backoff
            while (locked.load(std::memory_order_relaxed))
                _mm_pause();
        }
    }

    void unlock()
    {
        locked.store(0, std::memory_order_release);
    }
};

struct __attribute__((__aligned__(64))) AlignedSmallLock : public SmallLock
{
    char dummy[64 - sizeof(SmallLock)];
};

/// A hash map cell that includes an embedded spinlock for concurrent access
template <typename Key, typename Mapped, typename Hash>
struct HashMapCellWithLock
{
    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = Mapped;
    using key_type = Key;

    value_type value;
    mutable SmallLock lock;

    HashMapCellWithLock() = default;

    /// Copy constructor - copies value, initializes lock to unlocked state
    HashMapCellWithLock(const HashMapCellWithLock & other) : value(other.value) {}

    /// Copy assignment - copies value, resets lock
    HashMapCellWithLock & operator=(const HashMapCellWithLock & other)
    {
        value = other.value;
        lock.locked.store(0, std::memory_order_relaxed);
        return *this;
    }

    const Key & getKey() const { return value.first; }
    Mapped & getMapped() { return value.second; }
    const Mapped & getMapped() const { return value.second; }

    bool keyEquals(const Key & key_) const { return value.first == key_; }
    bool isZero() const { return value.first == Key{}; }
    void setZero() { value.first = Key{}; }

    size_t getHash(const Hash & hash_func) const { return hash_func(value.first); }
};

/// A concurrent hash map with per-cell spinlocks
template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
class HashMapWithSmallLocks
{
public:
    using Cell = HashMapCellWithLock<Key, Mapped, Hash>;
    static constexpr size_t INITIAL_SIZE_DEGREE = 22;  /// 4M cells, suitable for up to ~2M unique keys

private:
    std::vector<Cell> buf;
    size_t size_degree;
    size_t m_size = 0;
    Hash hash_func;
    mutable SmallLock size_lock;

    size_t mask() const { return (1ULL << size_degree) - 1; }
    size_t place(size_t hash_value) const { return hash_value & mask(); }

public:
    HashMapWithSmallLocks() : size_degree(INITIAL_SIZE_DEGREE)
    {
        buf.resize(1ULL << size_degree);
    }

    size_t size() const { return m_size; }

    size_t hash(const Key & key) const { return hash_func(key); }

    /// Try to emplace a key. Returns true if we could lock and process, false if lock was busy.
    /// If successful, sets 'found' to the cell and 'inserted' to whether it was a new insertion.
    bool tryEmplace(const Key & key, Cell *& found, bool & inserted)
    {
        size_t hash_value = hash_func(key);
        size_t place_value = place(hash_value);

        /// Linear probing
        for (size_t i = 0; i < (1ULL << size_degree); ++i)
        {
            Cell & cell = buf[place_value];

            if (!cell.lock.tryLock())
                return false;  /// Couldn't get lock, caller should handle locally

            if (cell.isZero())
            {
                /// Empty cell - insert here
                cell.value.first = key;
                cell.value.second = Mapped{};
                found = &cell;
                inserted = true;
                size_lock.lock();
                ++m_size;
                size_lock.unlock();
                cell.lock.unlock();
                return true;
            }
            else if (cell.keyEquals(key))
            {
                /// Found existing key
                found = &cell;
                inserted = false;
                cell.lock.unlock();
                return true;
            }
            else
            {
                /// Collision - try next slot
                cell.lock.unlock();
                place_value = (place_value + 1) & mask();
            }
        }

        /// Table is full - this shouldn't happen in practice
        return false;
    }

    /// Blocking emplace - always succeeds (spins until lock acquired)
    void emplace(const Key & key, Cell *& found, bool & inserted)
    {
        size_t hash_value = hash_func(key);
        size_t place_value = place(hash_value);

        /// Linear probing with locking
        while (true)
        {
            Cell & cell = buf[place_value];

            cell.lock.lock();

            if (cell.isZero())
            {
                /// Empty cell - insert here
                cell.value.first = key;
                cell.value.second = Mapped{};
                found = &cell;
                inserted = true;
                size_lock.lock();
                ++m_size;
                size_lock.unlock();
                cell.lock.unlock();
                return;
            }
            else if (cell.keyEquals(key))
            {
                /// Found existing key
                found = &cell;
                inserted = false;
                cell.lock.unlock();
                return;
            }
            else
            {
                /// Collision - try next slot
                cell.lock.unlock();
                place_value = (place_value + 1) & mask();
            }
        }
    }

    /// Increment value for key, using tryEmplace
    bool tryIncrement(const Key & key)
    {
        Cell * found;
        bool inserted;
        if (tryEmplace(key, found, inserted))
        {
            found->lock.lock();
            ++found->getMapped();
            found->lock.unlock();
            return true;
        }
        return false;
    }

    /// Blocking increment
    void increment(const Key & key)
    {
        Cell * found;
        bool inserted;
        emplace(key, found, inserted);
        found->lock.lock();
        ++found->getMapped();
        found->lock.unlock();
    }

    /// Access operator (creates default if not exists)
    Mapped & operator[](const Key & key)
    {
        Cell * found;
        bool inserted;
        emplace(key, found, inserted);
        return found->getMapped();
    }

    /// Simple iterator for reading results (not thread-safe during modifications)
    class iterator
    {
        Cell * ptr;
        Cell * end;
    public:
        iterator(Cell * ptr_, Cell * end_) : ptr(ptr_), end(end_)
        {
            while (ptr < end && ptr->isZero())
                ++ptr;
        }
        iterator & operator++()
        {
            ++ptr;
            while (ptr < end && ptr->isZero())
                ++ptr;
            return *this;
        }
        bool operator!=(const iterator & other) const { return ptr != other.ptr; }
        Cell & operator*() const { return *ptr; }
        Cell * operator->() const { return ptr; }
    };

    iterator begin() { return iterator(buf.data(), buf.data() + buf.size()); }
    iterator end() { return iterator(buf.data() + buf.size(), buf.data() + buf.size()); }
};


/// ==================== Robin Hood Hash Map ====================
/// Robin Hood hashing reduces variance in probe lengths by "stealing from the rich":
/// When inserting, if we find an element that is closer to its ideal position than
/// we are to ours, we swap and continue inserting the displaced element.
template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
class RobinHoodHashMap
{
public:
    static constexpr size_t INITIAL_SIZE_DEGREE = 22;
    static constexpr uint8_t EMPTY_MARKER = 0xFF;
    static constexpr size_t MAX_PROBE_DISTANCE = 127;  /// Max value that fits in probe_distance field

    struct Cell
    {
        Key key;
        Mapped value;
        uint8_t probe_distance;  /// Distance from ideal position (0xFF = empty)

        Cell() : probe_distance(EMPTY_MARKER) {}

        bool isEmpty() const { return probe_distance == EMPTY_MARKER; }
        void setEmpty() { probe_distance = EMPTY_MARKER; }
    };

private:
    std::vector<Cell> buf;
    size_t size_degree;
    size_t m_size = 0;
    Hash hash_func;

    size_t mask() const { return (1ULL << size_degree) - 1; }
    size_t capacity() const { return 1ULL << size_degree; }
    size_t idealPosition(size_t hash_value) const { return hash_value & mask(); }

    void resize()
    {
        std::vector<Cell> old_buf = std::move(buf);
        ++size_degree;
        buf.resize(1ULL << size_degree);
        m_size = 0;

        for (auto & cell : old_buf)
        {
            if (!cell.isEmpty())
            {
                Cell * result;
                bool inserted;
                emplaceNoResize(cell.key, result, inserted);
                if (inserted)
                    result->value = cell.value;
                else
                    result->value += cell.value;  /// Shouldn't happen, but handle it
            }
        }
    }

    void emplaceNoResize(const Key & key, Cell *& result, bool & inserted)
    {
        size_t hash_value = hash_func(key);
        size_t pos = idealPosition(hash_value);
        size_t dist = 0;

        Key insert_key = key;
        Mapped insert_value{};
        bool is_new = true;
        Cell * new_cell_result = nullptr;

        while (true)
        {
            Cell & cell = buf[pos];

            if (cell.isEmpty())
            {
                cell.key = insert_key;
                cell.value = insert_value;
                cell.probe_distance = static_cast<uint8_t>(std::min(dist, MAX_PROBE_DISTANCE));
                if (is_new)
                {
                    ++m_size;
                    result = &cell;
                    inserted = true;
                }
                return;
            }

            if (cell.key == insert_key && is_new)
            {
                result = &cell;
                inserted = false;
                return;
            }

            /// Robin Hood: steal from the rich (smaller probe distance)
            if (cell.probe_distance < dist)
            {
                if (is_new)
                {
                    new_cell_result = &cell;
                    is_new = false;
                }

                std::swap(insert_key, cell.key);
                std::swap(insert_value, cell.value);
                uint8_t old_dist = cell.probe_distance;
                cell.probe_distance = static_cast<uint8_t>(std::min(dist, MAX_PROBE_DISTANCE));
                dist = old_dist;
            }

            pos = (pos + 1) & mask();
            ++dist;
        }

        /// If we get here with a new key, set result
        if (new_cell_result)
        {
            result = new_cell_result;
            inserted = true;
            ++m_size;
        }
    }

public:
    RobinHoodHashMap() : size_degree(INITIAL_SIZE_DEGREE)
    {
        buf.resize(1ULL << size_degree);
    }

    explicit RobinHoodHashMap(size_t initial_size_degree) : size_degree(initial_size_degree)
    {
        buf.resize(1ULL << size_degree);
    }

    size_t size() const { return m_size; }

    /// Prefetch for a key
    void prefetch(const Key & key) const
    {
        size_t hash_value = hash_func(key);
        size_t pos = idealPosition(hash_value);
        __builtin_prefetch(&buf[pos]);
    }

    Cell * find(const Key & key)
    {
        size_t hash_value = hash_func(key);
        size_t pos = idealPosition(hash_value);
        size_t dist = 0;

        while (dist <= MAX_PROBE_DISTANCE + m_size)  /// Safety bound
        {
            Cell & cell = buf[pos];

            if (cell.isEmpty())
                return nullptr;

            if (cell.probe_distance < dist && dist <= MAX_PROBE_DISTANCE)
                return nullptr;  /// Key would have been here if it existed

            if (cell.key == key)
                return &cell;

            pos = (pos + 1) & mask();
            ++dist;
        }
        return nullptr;
    }

    void emplace(const Key & key, Cell *& result, bool & inserted)
    {
        /// Resize if load factor > 0.75
        if (m_size * 4 >= capacity() * 3)
            resize();

        size_t hash_value = hash_func(key);
        size_t pos = idealPosition(hash_value);
        size_t dist = 0;

        Key insert_key = key;
        Mapped insert_value{};
        bool is_new = true;
        Cell * new_cell_result = nullptr;

        while (true)
        {
            Cell & cell = buf[pos];

            if (cell.isEmpty())
            {
                cell.key = insert_key;
                cell.value = insert_value;
                cell.probe_distance = static_cast<uint8_t>(std::min(dist, MAX_PROBE_DISTANCE));
                if (is_new)
                {
                    ++m_size;
                    result = &cell;
                    inserted = true;
                }
                else
                {
                    result = new_cell_result;
                    inserted = true;
                }
                return;
            }

            if (cell.key == insert_key && is_new)
            {
                result = &cell;
                inserted = false;
                return;
            }

            /// Robin Hood: steal from the rich (smaller probe distance)
            if (cell.probe_distance < dist)
            {
                if (is_new)
                {
                    new_cell_result = &cell;
                    ++m_size;
                    is_new = false;
                }

                std::swap(insert_key, cell.key);
                std::swap(insert_value, cell.value);
                size_t old_dist = cell.probe_distance;
                cell.probe_distance = static_cast<uint8_t>(std::min(dist, MAX_PROBE_DISTANCE));
                dist = old_dist;
            }

            pos = (pos + 1) & mask();
            ++dist;
        }
    }

    Mapped & operator[](const Key & key)
    {
        Cell * cell;
        bool inserted;
        emplace(key, cell, inserted);
        return cell->value;
    }

    /// Iterator
    class iterator
    {
        Cell * ptr;
        Cell * end_ptr;
    public:
        iterator(Cell * p, Cell * e) : ptr(p), end_ptr(e)
        {
            while (ptr < end_ptr && ptr->isEmpty())
                ++ptr;
        }
        iterator & operator++()
        {
            ++ptr;
            while (ptr < end_ptr && ptr->isEmpty())
                ++ptr;
            return *this;
        }
        bool operator!=(const iterator & other) const { return ptr != other.ptr; }
        Cell & operator*() const { return *ptr; }
        Cell * operator->() const { return ptr; }

        const Key & getKey() const { return ptr->key; }
        Mapped & getMapped() { return ptr->value; }
    };

    iterator begin() { return iterator(buf.data(), buf.data() + buf.size()); }
    iterator end() { return iterator(buf.data() + buf.size(), buf.data() + buf.size()); }
};


/// ==================== Swiss Table Hash Map ====================
/// Swiss Table uses SIMD to probe multiple slots at once.
/// Metadata array stores control bytes (7 bits of hash + empty/deleted markers).
/// Groups of 16 control bytes are searched simultaneously using SSE/AVX.
template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
class SwissTableHashMap
{
public:
    static constexpr size_t INITIAL_SIZE_DEGREE = 22;
    static constexpr size_t GROUP_SIZE = 16;  /// SSE width
    static constexpr int8_t CTRL_EMPTY = -128;    /// 0b10000000
    static constexpr int8_t CTRL_DELETED = -2;    /// 0b11111110

    struct Slot
    {
        Key key;
        Mapped value;
    };

private:
    std::vector<int8_t> ctrl;   /// Control bytes (metadata)
    std::vector<Slot> slots;
    size_t size_degree;
    size_t capacity;
    size_t m_size = 0;
    Hash hash_func;

    /// Extract H1 (for position) and H2 (for control byte) from hash
    static size_t H1(size_t hash) { return hash >> 7; }
    static int8_t H2(size_t hash) { return hash & 0x7F; }  /// 7 bits, always positive

    size_t mask() const { return capacity - 1; }

    /// Find matching positions in a group using SIMD
    uint32_t matchGroup(const int8_t * group, int8_t h2) const
    {
        __m128i ctrl_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(group));
        __m128i match_vec = _mm_set1_epi8(h2);
        __m128i cmp = _mm_cmpeq_epi8(ctrl_vec, match_vec);
        return static_cast<uint32_t>(_mm_movemask_epi8(cmp));
    }

    /// Find empty positions in a group
    uint32_t matchEmpty(const int8_t * group) const
    {
        __m128i ctrl_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(group));
        __m128i empty_vec = _mm_set1_epi8(CTRL_EMPTY);
        __m128i cmp = _mm_cmpeq_epi8(ctrl_vec, empty_vec);
        return static_cast<uint32_t>(_mm_movemask_epi8(cmp));
    }

    /// Find empty or deleted positions
    uint32_t matchEmptyOrDeleted(const int8_t * group) const
    {
        __m128i ctrl_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(group));
        /// Empty and deleted both have high bit set
        return static_cast<uint32_t>(_mm_movemask_epi8(ctrl_vec));
    }

public:
    SwissTableHashMap() : size_degree(INITIAL_SIZE_DEGREE)
    {
        capacity = 1ULL << size_degree;
        ctrl.resize(capacity + GROUP_SIZE, CTRL_EMPTY);  /// Extra group for wraparound
        slots.resize(capacity);
    }

    explicit SwissTableHashMap(size_t initial_size_degree) : size_degree(initial_size_degree)
    {
        capacity = 1ULL << size_degree;
        ctrl.resize(capacity + GROUP_SIZE, CTRL_EMPTY);
        slots.resize(capacity);
    }

    size_t size() const { return m_size; }

    void prefetch(const Key & key) const
    {
        size_t hash = hash_func(key);
        size_t pos = H1(hash) & mask();
        __builtin_prefetch(&ctrl[pos]);
        __builtin_prefetch(&slots[pos]);
    }

    Slot * find(const Key & key)
    {
        size_t hash = hash_func(key);
        int8_t h2 = H2(hash);
        size_t pos = H1(hash) & mask();

        while (true)
        {
            const int8_t * group = &ctrl[pos];
            uint32_t matches = matchGroup(group, h2);

            while (matches)
            {
                int bit = __builtin_ctz(matches);
                size_t idx = (pos + bit) & mask();
                if (slots[idx].key == key)
                    return &slots[idx];
                matches &= matches - 1;  /// Clear lowest bit
            }

            /// Check if group has any empty slots (search complete)
            if (matchEmpty(group))
                return nullptr;

            pos = (pos + GROUP_SIZE) & mask();
        }
    }

    void emplace(const Key & key, Slot *& result, bool & inserted)
    {
        size_t hash = hash_func(key);
        int8_t h2 = H2(hash);
        size_t pos = H1(hash) & mask();

        while (true)
        {
            const int8_t * group = &ctrl[pos];

            /// First check for existing key
            uint32_t matches = matchGroup(group, h2);
            while (matches)
            {
                int bit = __builtin_ctz(matches);
                size_t idx = (pos + bit) & mask();
                if (slots[idx].key == key)
                {
                    result = &slots[idx];
                    inserted = false;
                    return;
                }
                matches &= matches - 1;
            }

            /// Check for empty slot to insert
            uint32_t empty_mask = matchEmptyOrDeleted(group);
            if (empty_mask)
            {
                int bit = __builtin_ctz(empty_mask);
                size_t idx = (pos + bit) & mask();
                ctrl[idx] = h2;
                /// Handle wraparound mirror
                if (idx < GROUP_SIZE)
                    ctrl[capacity + idx] = h2;
                slots[idx].key = key;
                slots[idx].value = Mapped{};
                result = &slots[idx];
                inserted = true;
                ++m_size;
                return;
            }

            pos = (pos + GROUP_SIZE) & mask();
        }
    }

    Mapped & operator[](const Key & key)
    {
        Slot * slot;
        bool inserted;
        emplace(key, slot, inserted);
        return slot->value;
    }

    /// Iterator
    class iterator
    {
        const int8_t * ctrl_ptr;
        const int8_t * ctrl_end;
        Slot * slot_base;
        size_t idx;
    public:
        iterator(const int8_t * c, const int8_t * ce, Slot * s, size_t /* cap */)
            : ctrl_ptr(c), ctrl_end(ce), slot_base(s), idx(0)
        {
            while (ctrl_ptr + idx < ctrl_end && ctrl_ptr[idx] < 0)  /// Skip empty/deleted
                ++idx;
        }
        iterator(const int8_t * ce) : ctrl_ptr(ce), ctrl_end(ce), slot_base(nullptr), idx(0) {}

        iterator & operator++()
        {
            ++idx;
            while (ctrl_ptr + idx < ctrl_end && ctrl_ptr[idx] < 0)
                ++idx;
            return *this;
        }
        bool operator!=(const iterator & other) const
        {
            return (ctrl_ptr + idx) != (other.ctrl_ptr + other.idx);
        }
        Slot & operator*() const { return slot_base[idx]; }
        Slot * operator->() const { return &slot_base[idx]; }

        const Key & getKey() const { return slot_base[idx].key; }
        Mapped & getMapped() { return slot_base[idx].value; }
    };

    iterator begin()
    {
        return iterator(ctrl.data(), ctrl.data() + capacity, slots.data(), capacity);
    }
    iterator end()
    {
        return iterator(ctrl.data() + capacity);
    }
};


/// ==================== Two-Level Robin Hood Hash Map ====================
/// Partitions keys into 256 buckets for parallel merging capability.
template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
class TwoLevelRobinHoodHashMap
{
public:
    static constexpr size_t NUM_BUCKETS = 256;
    static constexpr size_t BUCKET_SIZE_DEGREE = 14;  /// 16K cells per bucket

    using BucketMap = RobinHoodHashMap<Key, Mapped, Hash>;
    using Cell = typename BucketMap::Cell;

    std::vector<BucketMap> impls;

private:
    Hash hash_func;

public:
    TwoLevelRobinHoodHashMap()
    {
        impls.reserve(NUM_BUCKETS);
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls.emplace_back(BUCKET_SIZE_DEGREE);
    }

    static size_t getBucketFromHash(size_t hash_value)
    {
        return (hash_value >> (64 - 8)) & 0xFF;  /// Top 8 bits
    }

    size_t hash(const Key & key) const { return hash_func(key); }

    size_t size() const
    {
        size_t result = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            result += impls[i].size();
        return result;
    }

    void prefetch(const Key & key) const
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        impls[bucket].prefetch(key);
    }

    Cell * find(const Key & key)
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        return impls[bucket].find(key);
    }

    void emplace(const Key & key, Cell *& result, bool & inserted)
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        impls[bucket].emplace(key, result, inserted);
    }

    Mapped & operator[](const Key & key)
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        return impls[bucket][key];
    }
};


/// ==================== Two-Level Swiss Table Hash Map ====================
/// Partitions keys into 256 buckets for parallel merging capability.
template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
class TwoLevelSwissTableHashMap
{
public:
    static constexpr size_t NUM_BUCKETS = 256;
    static constexpr size_t BUCKET_SIZE_DEGREE = 14;  /// 16K slots per bucket

    using BucketMap = SwissTableHashMap<Key, Mapped, Hash>;
    using Slot = typename BucketMap::Slot;

    std::vector<BucketMap> impls;

private:
    Hash hash_func;

public:
    TwoLevelSwissTableHashMap()
    {
        impls.reserve(NUM_BUCKETS);
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls.emplace_back(BUCKET_SIZE_DEGREE);
    }

    static size_t getBucketFromHash(size_t hash_value)
    {
        return (hash_value >> (64 - 8)) & 0xFF;  /// Top 8 bits
    }

    size_t hash(const Key & key) const { return hash_func(key); }

    size_t size() const
    {
        size_t result = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            result += impls[i].size();
        return result;
    }

    void prefetch(const Key & key) const
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        impls[bucket].prefetch(key);
    }

    Slot * find(const Key & key)
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        return impls[bucket].find(key);
    }

    void emplace(const Key & key, Slot *& result, bool & inserted)
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        impls[bucket].emplace(key, result, inserted);
    }

    Mapped & operator[](const Key & key)
    {
        size_t hash_value = hash_func(key);
        size_t bucket = getBucketFromHash(hash_value);
        return impls[bucket][key];
    }
};


using Mutex = std::mutex;

using MapSmallLocks = HashMapWithSmallLocks<Key, Value>;
using MapRobinHood = RobinHoodHashMap<Key, Value>;
using MapSwiss = SwissTableHashMap<Key, Value>;
using MapTwoLevelRobinHood = TwoLevelRobinHoodHashMap<Key, Value>;
using MapTwoLevelSwiss = TwoLevelSwissTableHashMap<Key, Value>;


void aggregate1(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

/// Aggregation with prefetching - compute hash and prefetch ahead before processing
void aggregate1Prefetch(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 16;

    auto it = begin;

    /// Prefetch initial elements
    for (size_t i = 0; i < PREFETCH_LOOKAHEAD && it + i < end; ++i)
        map.prefetch(*(it + i));

    /// Main loop with prefetching
    for (; it + PREFETCH_LOOKAHEAD < end; ++it)
    {
        map.prefetch(*(it + PREFETCH_LOOKAHEAD));
        ++map[*it];
    }

    /// Process remaining elements
    for (; it != end; ++it)
        ++map[*it];
}

/// Aggregation with batch prefetching - prefetch multiple elements ahead
void aggregate1BatchPrefetch(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t BATCH_SIZE = 16;

    auto it = begin;

    while (it + BATCH_SIZE <= end)
    {
        /// Prefetch all elements in the batch first
        for (size_t i = 0; i < BATCH_SIZE; ++i)
            map.prefetch(*(it + i));

        /// Then process all elements in the batch
        for (size_t i = 0; i < BATCH_SIZE; ++i)
            ++map[*(it + i)];

        it += BATCH_SIZE;
    }

    /// Process remaining elements
    for (; it != end; ++it)
        ++map[*it];
}

void aggregate12(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    Map::LookupResult found = nullptr;
    auto prev_it = end;
    for (auto it = begin; it != end; ++it)
    {
        if (prev_it != end && *it == *prev_it)
        {
            assert(found != nullptr);
            ++found->getMapped();
            continue;
        }
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted);
        assert(found != nullptr);
        ++found->getMapped();
    }
}

void aggregate2(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

/// TwoLevel aggregation with prefetching
void aggregate2Prefetch(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 16;

    auto it = begin;

    /// Prefetch initial elements
    for (size_t i = 0; i < PREFETCH_LOOKAHEAD && it + i < end; ++i)
        map.prefetch(*(it + i));

    /// Main loop with prefetching
    for (; it + PREFETCH_LOOKAHEAD < end; ++it)
    {
        map.prefetch(*(it + PREFETCH_LOOKAHEAD));
        ++map[*it];
    }

    /// Process remaining elements
    for (; it != end; ++it)
        ++map[*it];
}

/// TwoLevel batch prefetch - prefetch multiple elements ahead
void aggregate2BatchPrefetch(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t BATCH_SIZE = 16;

    auto it = begin;

    while (it + BATCH_SIZE <= end)
    {
        /// Prefetch all elements in the batch first
        for (size_t i = 0; i < BATCH_SIZE; ++i)
            map.prefetch(*(it + i));

        /// Then process all elements in the batch
        for (size_t i = 0; i < BATCH_SIZE; ++i)
            ++map[*(it + i)];

        it += BATCH_SIZE;
    }

    /// Process remaining elements
    for (; it != end; ++it)
        ++map[*it];
}

/// TwoLevel with prefetching and sequential keys optimization
void aggregate2PrefetchSeq(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 16;

    MapTwoLevel::LookupResult found = nullptr;
    Key prev_key {};
    bool first = true;

    auto it = begin;

    /// Prefetch initial elements
    for (size_t i = 0; i < PREFETCH_LOOKAHEAD && it + i < end; ++i)
        map.prefetch(*(it + i));

    /// Main loop with prefetching and sequential key optimization
    for (; it + PREFETCH_LOOKAHEAD < end; ++it)
    {
        map.prefetch(*(it + PREFETCH_LOOKAHEAD));

        if (!first && *it == prev_key)
        {
            ++found->getMapped();
            continue;
        }
        first = false;
        prev_key = *it;

        bool inserted;
        map.emplace(*it, found, inserted);
        ++found->getMapped();
    }

    /// Process remaining elements
    for (; it != end; ++it)
    {
        if (!first && *it == prev_key)
        {
            ++found->getMapped();
            continue;
        }
        first = false;
        prev_key = *it;

        bool inserted;
        map.emplace(*it, found, inserted);
        ++found->getMapped();
    }
}

void aggregate22(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    MapTwoLevel::LookupResult found = nullptr;
    auto prev_it = end;
    bool first = true;
    for (auto it = begin; it != end; ++it)
    {
        if (!first && *it == *prev_it)
        {
            assert(found != nullptr);
            ++found->getMapped();
            continue;
        }
        first = false;
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted);
        assert(found != nullptr);
        ++found->getMapped();
    }
}

void merge2(MapTwoLevel * maps, size_t num_threads, size_t bucket)
{
    for (size_t i = 1; i < num_threads; ++i)
        for (auto it = maps[i].impls[bucket].begin(); it != maps[i].impls[bucket].end(); ++it)
            maps[0].impls[bucket][it->getKey()] += it->getMapped();
}

/// Merge with prefetching
void merge2Prefetch(MapTwoLevel * maps, size_t num_threads, size_t bucket)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 8;

    for (size_t i = 1; i < num_threads; ++i)
    {
        auto & src_map = maps[i].impls[bucket];
        auto & dst_map = maps[0].impls[bucket];

        auto it = src_map.begin();
        auto end = src_map.end();

        /// Prefetch initial elements
        auto prefetch_it = it;
        for (size_t j = 0; j < PREFETCH_LOOKAHEAD && prefetch_it != end; ++j, ++prefetch_it)
            dst_map.prefetch(prefetch_it->getKey());

        /// Main loop with prefetching
        for (; prefetch_it != end; ++it, ++prefetch_it)
        {
            dst_map.prefetch(prefetch_it->getKey());
            dst_map[it->getKey()] += it->getMapped();
        }

        /// Process remaining elements
        for (; it != end; ++it)
            dst_map[it->getKey()] += it->getMapped();
    }
}

void aggregate3(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        auto * found = local_map.find(*it);

        if (found)
            ++found->getMapped();
        else if (local_map.size() < threshold)
            ++local_map[*it];    /// TODO You could do one lookup, not two.
        else
        {
            if (mutex.try_lock())
            {
                ++global_map[*it];
                mutex.unlock();
            }
            else
                ++local_map[*it];
        }
    }
}

void aggregate33(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        Map::LookupResult found;
        bool inserted;
        local_map.emplace(*it, found, inserted);
        ++found->getMapped();

        if (inserted && local_map.size() == threshold)
        {
            std::lock_guard<Mutex> lock(mutex);
            for (auto & value_type : local_map)
                global_map[value_type.getKey()] += value_type.getMapped();

            local_map.clear();
        }
    }
}

void aggregate4(Map & local_map, MapTwoLevel & global_map, Mutex * mutexes, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;
    static constexpr size_t block_size = 8192;

    auto it = begin;
    while (it != end)
    {
        auto block_end = std::min(end, it + block_size);

        if (local_map.size() < threshold)
        {
            for (; it != block_end; ++it)
                ++local_map[*it];
        }
        else
        {
            for (; it != block_end; ++it)
            {
                auto * found = local_map.find(*it);

                if (found)
                    ++found->getMapped();
                else
                {
                    size_t hash_value = global_map.hash(*it);
                    size_t bucket = MapTwoLevel::getBucketFromHash(hash_value);

                    if (mutexes[bucket].try_lock())
                    {
                        ++global_map.impls[bucket][*it];
                        mutexes[bucket].unlock();
                    }
                    else
                        ++local_map[*it];
                }
            }
        }
    }
}

/// Aggregate using local map + shared map with small locks
/// If key is in local map, increment there.
/// If local map is small enough, insert there.
/// Otherwise try to insert into global map (with per-cell locking).
/// If lock contention, fall back to local map.
void aggregate5(Map & local_map, MapSmallLocks & global_map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        auto * found = local_map.find(*it);

        if (found)
            ++found->getMapped();
        else if (local_map.size() < threshold)
            ++local_map[*it];
        else
        {
            MapSmallLocks::Cell * cell;
            bool inserted;

            if (global_map.tryEmplace(*it, cell, inserted))
            {
                cell->lock.lock();
                ++cell->getMapped();
                cell->lock.unlock();
            }
            else
                ++local_map[*it];
        }
    }
}

/// Aggregate directly into shared map with small locks (no local map)
void aggregate6(MapSmallLocks & global_map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        global_map.increment(*it);
}

/// Aggregate with local map overflow to shared map (blocking version)
void aggregate7(Map & local_map, MapSmallLocks & global_map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        auto * found = local_map.find(*it);

        if (found)
            ++found->getMapped();
        else if (local_map.size() < threshold)
            ++local_map[*it];
        else
        {
            /// Blocking insert into global map
            global_map.increment(*it);
        }
    }
}

/// ==================== Robin Hood Aggregation ====================
void aggregateRobinHood(MapRobinHood & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

void aggregateRobinHoodPrefetch(MapRobinHood & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 16;

    auto it = begin;

    for (size_t i = 0; i < PREFETCH_LOOKAHEAD && it + i < end; ++i)
        map.prefetch(*(it + i));

    for (; it + PREFETCH_LOOKAHEAD < end; ++it)
    {
        map.prefetch(*(it + PREFETCH_LOOKAHEAD));
        ++map[*it];
    }

    for (; it != end; ++it)
        ++map[*it];
}

/// ==================== Swiss Table Aggregation ====================
void aggregateSwiss(MapSwiss & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

void aggregateSwissPrefetch(MapSwiss & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 16;

    auto it = begin;

    for (size_t i = 0; i < PREFETCH_LOOKAHEAD && it + i < end; ++i)
        map.prefetch(*(it + i));

    for (; it + PREFETCH_LOOKAHEAD < end; ++it)
    {
        map.prefetch(*(it + PREFETCH_LOOKAHEAD));
        ++map[*it];
    }

    for (; it != end; ++it)
        ++map[*it];
}

/// ==================== Two-Level Robin Hood Aggregation ====================
void aggregateTwoLevelRobinHood(MapTwoLevelRobinHood & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

void aggregateTwoLevelRobinHoodPrefetch(MapTwoLevelRobinHood & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 16;

    auto it = begin;

    for (size_t i = 0; i < PREFETCH_LOOKAHEAD && it + i < end; ++i)
        map.prefetch(*(it + i));

    for (; it + PREFETCH_LOOKAHEAD < end; ++it)
    {
        map.prefetch(*(it + PREFETCH_LOOKAHEAD));
        ++map[*it];
    }

    for (; it != end; ++it)
        ++map[*it];
}

void mergeTwoLevelRobinHood(MapTwoLevelRobinHood * maps, size_t num_threads, size_t bucket)
{
    for (size_t i = 1; i < num_threads; ++i)
        for (auto it = maps[i].impls[bucket].begin(); it != maps[i].impls[bucket].end(); ++it)
            maps[0].impls[bucket][it.getKey()] += it.getMapped();
}

/// ==================== Two-Level Swiss Table Aggregation ====================
void aggregateTwoLevelSwiss(MapTwoLevelSwiss & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

void aggregateTwoLevelSwissPrefetch(MapTwoLevelSwiss & map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t PREFETCH_LOOKAHEAD = 16;

    auto it = begin;

    for (size_t i = 0; i < PREFETCH_LOOKAHEAD && it + i < end; ++i)
        map.prefetch(*(it + i));

    for (; it + PREFETCH_LOOKAHEAD < end; ++it)
    {
        map.prefetch(*(it + PREFETCH_LOOKAHEAD));
        ++map[*it];
    }

    for (; it != end; ++it)
        ++map[*it];
}

void mergeTwoLevelSwiss(MapTwoLevelSwiss * maps, size_t num_threads, size_t bucket)
{
    for (size_t i = 1; i < num_threads; ++i)
        for (auto it = maps[i].impls[bucket].begin(); it != maps[i].impls[bucket].end(); ++it)
            maps[0].impls[bucket][it.getKey()] += it.getMapped();
}


int main(int argc, char ** argv)
{
    size_t n = std::stol(argv[1]);
    size_t num_threads = std::stol(argv[2]);
    size_t method = argc <= 3 ? 0 : std::stol(argv[3]);

    std::cerr << std::fixed << std::setprecision(3);

    SimpleThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);

    Source data(n);

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        in2.readStrict(reinterpret_cast<char*>(data.data()), sizeof(data[0]) * n);

        watch.stop();
        std::cerr
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl << std::endl;
    }

    if (!method || method == 1)
    {
        std::cerr << "Method 1 (single-level, parallel aggregate, serial merge):\n";
        /** Option 1.
          * In different threads, we aggregate independently into different hash tables.
          * Then merge them together.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate1(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it->getKey()] += it->getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 12)
    {
        std::cerr << "Method 12 (single-level, sequential keys optimization, serial merge):\n";
        /** The same, but with optimization for consecutive identical values.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate12(
                                    maps[i],
                                    data.begin() + (data.size() * i) / num_threads,
                                    data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it->getKey()] += it->getMapped();

        watch.stop();

        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in \033[1m" << time_total << "\033[0m"
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

#if 0
    if (!method || method == 11)
    {
        std::cerr << "Method 11 (single-level, parallel aggregate, parallel merge with mutex):\n";
        /** Option 11.
          * Same as option 1, but with merge, the order of the cycles is changed,
          *  which potentially can give better cache locality.
          *
          * In practice, it is much worse.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate1(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        std::vector<Map::iterator> iterators(num_threads);
        for (size_t i = 1; i < num_threads; ++i)
            iterators[i] = maps[i].begin();

        while (true)
        {
            bool finish = true;
            for (size_t i = 1; i < num_threads; ++i)
            {
                if (iterators[i] == maps[i].end())
                    continue;

                finish = false;
                maps[0][iterators[i]->getKey()] += iterators[i]->getMapped();
                ++iterators[i];
            }

            if (finish)
                break;
        }

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in \033[1m" << time_total << "\033[0m"
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }
#endif

    if (!method || method == 2)
    {
        std::cerr << "Method 2 (two-level, parallel aggregate, parallel merge by bucket):\n";
        /** Option 2.
          * In different threads, we aggregate independently into different two-level hash tables.
          * Then merge them together, parallelizing by the first level buckets.
          * When using hash tables of large sizes (10 million elements or more),
          *  and a large number of threads (8-32), the merge is a bottleneck,
          *  and has a performance advantage of 4 times.
          */

        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate2(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { merge2(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 22)
    {
        std::cerr << "Method 22 (two-level, sequential keys optimization, parallel merge):\n";
        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate22(
                                    maps[i],
                                    data.begin() + (data.size() * i) / num_threads,
                                    data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { merge2(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in \033[1m" << time_total << "\033[0m"
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 3)
    {
        std::cerr << "Method 3 (local + global map with mutex, serial merge):\n";
        /** Option 3.
          * In different threads, we aggregate independently into different hash tables,
          *  until their size becomes large enough.
          * If the size of the local hash table is large, and there is no element in it,
          *  then we insert it into one global hash table, protected by mutex,
          *  and if mutex failed to capture, then insert it into the local one.
          * Then merge all the local hash tables to the global one.
          * This method is bad - a lot of contention.
          */

        std::vector<Map> local_maps(num_threads);
        Map global_map;
        Mutex mutex;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate3(
                local_maps[i],
                global_map,
                mutex,
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << global_map.size() << std::endl;
        size_before_merge += global_map.size();

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getKey()] += it->getMapped();

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

    if (!method || method == 33)
    {
        std::cerr << "Method 33 (local + global map with mutex, sequential keys):\n";
        /** Option 33.
         * In different threads, we aggregate independently into different hash tables,
         *  until their size becomes large enough.
         * Then we insert the data to the global hash table, protected by mutex, and continue.
         */

        std::vector<Map> local_maps(num_threads);
        Map global_map;
        Mutex mutex;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate33(
                local_maps[i],
                global_map,
                mutex,
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << global_map.size() << std::endl;
        size_before_merge += global_map.size();

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getKey()] += it->getMapped();

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in \033[1m" << time_total << "\033[0m"
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

    if (!method || method == 4)
    {
        std::cerr << "Method 4 (local + global two-level with mutex, parallel merge):\n";
        /** Option 4.
          * In different threads, we aggregate independently into different hash tables,
          *  until their size becomes large enough.
          * If the size of the local hash table is large, and there is no element in it,
          *  then insert it into one of 256 global hash tables, each of which is under its mutex.
          * Then merge all local hash tables into the global one.
          * This method is not so bad with a lot of threads, but worse than the second one.
          */

        std::vector<Map> local_maps(num_threads);
        MapTwoLevel global_map;
        std::vector<Mutex> mutexes(MapTwoLevel::NUM_BUCKETS);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate4(
                local_maps[i],
                global_map,
                mutexes.data(),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;

        size_t sum_size = global_map.size();
        std::cerr << "Size (global): " << sum_size << std::endl;
        size_before_merge += sum_size;

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getKey()] += it->getMapped();

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

    if (!method || method == 100)
    {
        std::cerr << "Method 100 (Prefetch single-level):\n";
        /** Option 100.
          * Same as method 1, but with prefetching during aggregation.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate1Prefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it->getKey()] += it->getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 101)
    {
        std::cerr << "Method 101 (Batch prefetch single-level):\n";
        /** Option 101.
          * Same as method 1, but with batch prefetching and pre-computed hashes.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate1BatchPrefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it->getKey()] += it->getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 200)
    {
        std::cerr << "Method 200 (Prefetch two-level):\n";
        /** Option 200.
          * Same as method 2, but with prefetching during aggregation.
          */

        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate2Prefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { merge2(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 201)
    {
        std::cerr << "Method 201 (Batch prefetch two-level):\n";
        /** Option 201.
          * Same as method 2, but with batch prefetching during aggregation.
          */

        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate2BatchPrefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { merge2(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 202)
    {
        std::cerr << "Method 202 (Batch prefetch two-level with prefetch merge):\n";
        /** Option 202.
          * Batch prefetch aggregation and prefetching merge.
          */

        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate2BatchPrefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { merge2Prefetch(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 203)
    {
        std::cerr << "Method 203 (Prefetch + sequential keys two-level):\n";
        /** Option 203.
          * Two-level with prefetching and sequential keys optimization.
          */

        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate2PrefetchSeq(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { merge2(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

#if 0
    if (!method || method == 5)
    {
        std::cerr << "Method 5 (Local map + shared map with small locks, tryLock):\n";
        /** Option 5.
          * In different threads, we aggregate independently into different hash tables,
          *  until their size becomes large enough.
          * If the size of the local hash table is large and there is no element in it,
          *  then insert it into one global hash table containing small latches in each cell,
          *  and if the latch can not be captured, then insert it into the local one.
          * Then merge all local hash tables into the global one.
          */

        std::vector<Map> local_maps(num_threads);
        MapSmallLocks global_map;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate5(
                local_maps[i],
                global_map,
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << global_map.size() << std::endl;
        size_before_merge += global_map.size();

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getKey()] += it->getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

    if (!method || method == 6)
    {
        std::cerr << "Method 6 (Direct to shared map with small locks):\n";
        /** Option 6.
          * All threads write directly to a shared hash map with per-cell spinlocks.
          * No local maps, all contention handled by the locks.
          */

        MapSmallLocks global_map;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate6(
                global_map,
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_total = watch.elapsedSeconds();
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

    if (!method || method == 7)
    {
        std::cerr << "Method 7 (Local map + shared map with small locks, blocking):\n";
        /** Option 7.
          * Same as method 5, but uses blocking lock instead of tryLock.
          */

        std::vector<Map> local_maps(num_threads);
        MapSmallLocks global_map;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate7(
                local_maps[i],
                global_map,
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << global_map.size() << std::endl;
        size_before_merge += global_map.size();

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getKey()] += it->getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }
#endif

    if (!method || method == 300)
    {
        std::cerr << "Method 300 (Robin Hood hash table):\n";
        /** Option 300.
          * Independent aggregation into Robin Hood hash maps, then sequential merge.
          */

        std::vector<MapRobinHood> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateRobinHood(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it.getKey()] += it.getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 301)
    {
        std::cerr << "Method 301 (Robin Hood with prefetch):\n";
        /** Option 301.
          * Robin Hood with prefetching during aggregation.
          */

        std::vector<MapRobinHood> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateRobinHoodPrefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it.getKey()] += it.getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

#if 0
    if (!method || method == 400)
    {
        std::cerr << "Method 400 (Swiss Table hash table):\n";
        /** Option 400.
          * Independent aggregation into Swiss Table hash maps, then sequential merge.
          */

        std::vector<MapSwiss> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateSwiss(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it.getKey()] += it.getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 401)
    {
        std::cerr << "Method 401 (Swiss Table with prefetch):\n";
        /** Option 401.
          * Swiss Table with prefetching during aggregation.
          */

        std::vector<MapSwiss> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateSwissPrefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it.getKey()] += it.getMapped();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }
#endif
    if (!method || method == 302)
    {
        std::cerr << "Method 302 (Two-Level Robin Hood):\n";
        /** Option 302.
          * Two-level Robin Hood with parallel merge.
          */

        std::vector<MapTwoLevelRobinHood> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateTwoLevelRobinHood(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevelRobinHood::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { mergeTwoLevelRobinHood(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 303)
    {
        std::cerr << "Method 303 (Two-Level Robin Hood with prefetch):\n";
        /** Option 303.
          * Two-level Robin Hood with prefetching and parallel merge.
          */

        std::vector<MapTwoLevelRobinHood> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateTwoLevelRobinHoodPrefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevelRobinHood::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { mergeTwoLevelRobinHood(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }
#if 0
    if (!method || method == 402)
    {
        std::cerr << "Method 402 (Two-Level Swiss Table):\n";
        /** Option 402.
          * Two-level Swiss Table with parallel merge.
          */

        std::vector<MapTwoLevelSwiss> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateTwoLevelSwiss(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevelSwiss::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { mergeTwoLevelSwiss(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 403)
    {
        std::cerr << "Method 403 (Two-Level Swiss Table with prefetch):\n";
        /** Option 403.
          * Two-level Swiss Table with prefetching and parallel merge.
          */

        std::vector<MapTwoLevelSwiss> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregateTwoLevelSwissPrefetch(
                maps[i],
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (unsigned i = 0; i < MapTwoLevelSwiss::NUM_BUCKETS; ++i)
            pool.scheduleOrThrowOnError([&, i] { mergeTwoLevelSwiss(maps.data(), num_threads, i); });

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }
#endif
    /// ==================== Partitioning-based algorithms ====================
    /// Use CRC32 hash + fastrange for partitioning, then aggregate per-partition.

    if (!method || method == 500)
    {
        std::cerr << "Method 500 (Serial partition + parallel aggregate):\n";
        /** Option 500.
          * Single-threaded partitioning using CRC32 + fastrange,
          * then parallel aggregation of each partition.
          */

        /// Partition buffers - one per thread
        std::vector<std::vector<Key>> partitions(num_threads);
        for (auto & p : partitions)
            p.reserve(data.size() / num_threads * 2);  /// Reserve with some slack

        Stopwatch watch;

        /// Serial partitioning using CRC32 hash + fastrange
        for (const auto & key : data)
        {
            UInt32 hash = static_cast<UInt32>(intHashCRC32(key));
            size_t partition = (static_cast<UInt64>(hash) * num_threads) >> 32;  /// fastrange
            partitions[partition].push_back(key);
        }

        watch.stop();
        double time_partitioned = watch.elapsedSeconds();
        std::cerr
            << "Partitioned in " << time_partitioned
            << " (" << n / time_partitioned << " elem/sec.)"
            << std::endl;

        std::cerr << "Partition sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
            std::cerr << (i == 0 ? "" : ", ") << partitions[i].size();
        std::cerr << std::endl;

        /// Now aggregate each partition in parallel
        std::vector<Map> maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = maps[i];
                for (const auto & key : partitions[i])
                    ++map[key];
            });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        /// No merge needed - each key goes to exactly one partition
        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += maps[i].size();

        double time_total = time_partitioned + time_aggregated;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    if (!method || method == 501)
    {
        std::cerr << "Method 501 (Parallel partition + parallel aggregate):\n";
        /** Option 501.
          * Parallel partitioning: each thread partitions its chunk into thread-local buffers.
          * Then parallel aggregation: each thread aggregates all keys from its partition
          * (gathered from all thread-local buffers).
          */

        /// Thread-local partition buffers: partitions[thread_id][partition_id]
        std::vector<std::vector<std::vector<Key>>> thread_partitions(num_threads);
        for (auto & tp : thread_partitions)
        {
            tp.resize(num_threads);
            for (auto & p : tp)
                p.reserve(data.size() / (num_threads * num_threads) * 2);
        }

        Stopwatch watch;

        /// Parallel partitioning
        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & my_partitions = thread_partitions[i];

                for (auto it = begin; it != end; ++it)
                {
                    UInt32 hash = static_cast<UInt32>(intHashCRC32(*it));
                    size_t partition = (static_cast<UInt64>(hash) * num_threads) >> 32;
                    my_partitions[partition].push_back(*it);
                }
            });

        pool.wait();

        watch.stop();
        double time_partitioned = watch.elapsedSeconds();
        std::cerr
            << "Partitioned in " << time_partitioned
            << " (" << n / time_partitioned << " elem/sec.)"
            << std::endl;

        /// Now aggregate: each thread processes all keys destined for its partition
        std::vector<Map> maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = maps[i];
                /// Gather keys from all threads' partition i
                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & key : thread_partitions[t][i])
                        ++map[key];
                }
            });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += maps[i].size();

        double time_total = time_partitioned + time_aggregated;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    if (!method || method == 502)
    {
        std::cerr << "Method 502 (Parallel partition + parallel aggregate with prefetch):\n";
        /** Option 502.
          * Same as 501 but with prefetching during aggregation.
          */

        std::vector<std::vector<std::vector<Key>>> thread_partitions(num_threads);
        for (auto & tp : thread_partitions)
        {
            tp.resize(num_threads);
            for (auto & p : tp)
                p.reserve(data.size() / (num_threads * num_threads) * 2);
        }

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & my_partitions = thread_partitions[i];

                for (auto it = begin; it != end; ++it)
                {
                    UInt32 hash = static_cast<UInt32>(intHashCRC32(*it));
                    size_t partition = (static_cast<UInt64>(hash) * num_threads) >> 32;
                    my_partitions[partition].push_back(*it);
                }
            });

        pool.wait();

        watch.stop();
        double time_partitioned = watch.elapsedSeconds();
        std::cerr
            << "Partitioned in " << time_partitioned
            << " (" << n / time_partitioned << " elem/sec.)"
            << std::endl;

        std::vector<Map> maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = maps[i];
                static constexpr size_t PREFETCH_LOOKAHEAD = 16;

                /// Flatten all keys for this partition for easier prefetching
                std::vector<Key> all_keys;
                size_t total = 0;
                for (size_t t = 0; t < num_threads; ++t)
                    total += thread_partitions[t][i].size();
                all_keys.reserve(total);
                for (size_t t = 0; t < num_threads; ++t)
                    all_keys.insert(all_keys.end(), thread_partitions[t][i].begin(), thread_partitions[t][i].end());

                /// Aggregate with prefetching
                auto it = all_keys.begin();
                auto end = all_keys.end();

                for (size_t j = 0; j < PREFETCH_LOOKAHEAD && it + j < end; ++j)
                    map.prefetch(*(it + j));

                for (; it + PREFETCH_LOOKAHEAD < end; ++it)
                {
                    map.prefetch(*(it + PREFETCH_LOOKAHEAD));
                    ++map[*it];
                }

                for (; it != end; ++it)
                    ++map[*it];
            });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += maps[i].size();

        double time_total = time_partitioned + time_aggregated;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    if (!method || method == 503)
    {
        std::cerr << "Method 503 (Parallel partition with histogram + parallel aggregate):\n";
        /** Option 503.
          * Two-phase partitioning with histogram for better memory allocation:
          * 1. First pass: count elements per partition (histogram)
          * 2. Allocate exact-sized buffers
          * 3. Second pass: scatter elements to partitions
          * 4. Parallel aggregation
          */

        Stopwatch watch;

        /// Phase 1: Parallel histogram computation
        std::vector<std::vector<size_t>> histograms(num_threads, std::vector<size_t>(num_threads, 0));

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & my_histogram = histograms[i];

                for (auto it = begin; it != end; ++it)
                {
                    UInt32 hash = static_cast<UInt32>(intHashCRC32(*it));
                    size_t partition = (static_cast<UInt64>(hash) * num_threads) >> 32;
                    ++my_histogram[partition];
                }
            });

        pool.wait();

        watch.stop();
        double time_histogram = watch.elapsedSeconds();

        /// Compute total sizes per partition and allocate
        std::vector<std::vector<std::vector<Key>>> thread_partitions(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
        {
            thread_partitions[i].resize(num_threads);
            for (size_t p = 0; p < num_threads; ++p)
                thread_partitions[i][p].reserve(histograms[i][p]);
        }

        /// Phase 2: Parallel scatter
        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & my_partitions = thread_partitions[i];

                for (auto it = begin; it != end; ++it)
                {
                    UInt32 hash = static_cast<UInt32>(intHashCRC32(*it));
                    size_t partition = (static_cast<UInt64>(hash) * num_threads) >> 32;
                    my_partitions[partition].push_back(*it);
                }
            });

        pool.wait();

        watch.stop();
        double time_partitioned = time_histogram + watch.elapsedSeconds();
        std::cerr
            << "Partitioned in " << time_partitioned
            << " (" << n / time_partitioned << " elem/sec.)"
            << std::endl;

        /// Phase 3: Parallel aggregation
        std::vector<Map> maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = maps[i];
                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & key : thread_partitions[t][i])
                        ++map[key];
                }
            });

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += maps[i].size();

        double time_total = time_partitioned + time_aggregated;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    if (!method || method == 504)
    {
        std::cerr << "Method 504 (L1-cache local map + deferred partitioning):\n";
        /** Option 504.
          * Hybrid approach: small local hash table (L1 cache) + deferred partitioning.
          *
          * Phase 1: Each thread processes its chunk:
          *   - Lookup in small local hash table (fits in L1 cache)
          *   - If found: increment
          *   - If not found and table not full: insert
          *   - If not found and table full: defer to partition bucket
          *
          * Phase 2: Each thread aggregates all deferred keys for its partition
          *
          * Phase 3: Merge local hash tables into partitioned results
          *
          * Result: N perfectly partitioned hash tables, no final merge needed.
          */

        /// L1 cache is typically 32KB, with 16-byte entries we can fit ~2K entries
        /// Use power of 2 for fast modulo: 2048 entries = 32KB
        static constexpr size_t LOCAL_MAP_MAX_SIZE = 2048;

        /// Small fixed-size hash table for L1 cache locality
        struct LocalEntry
        {
            Key key;
            Value value;
            bool occupied;
        };

        struct LocalMap
        {
            std::array<LocalEntry, LOCAL_MAP_MAX_SIZE * 2> entries{};  /// 2x for open addressing
            size_t size = 0;

            bool tryInsertOrIncrement(Key key, size_t hash)
            {
                constexpr size_t MASK = LOCAL_MAP_MAX_SIZE * 2 - 1;
                size_t pos = hash & MASK;
                for (size_t probe = 0; probe < 16; ++probe)  /// Limited probing
                {
                    auto & entry = entries[pos];
                    if (!entry.occupied)
                    {
                        if (size >= LOCAL_MAP_MAX_SIZE)
                            return false;  /// Table full, defer this key
                        entry.key = key;
                        entry.value = 1;
                        entry.occupied = true;
                        ++size;
                        return true;
                    }
                    if (entry.key == key)
                    {
                        ++entry.value;
                        return true;
                    }
                    pos = (pos + 1) & MASK;
                }
                return false;  /// Too many collisions, defer
            }
        };

        /// Phase 1 data structures
        /// local_maps[thread_id] = small hash table for frequent keys
        /// deferred[thread_id][partition_id] = keys to process later
        std::vector<LocalMap> local_maps(num_threads);
        std::vector<std::vector<std::vector<Key>>> deferred(num_threads);
        for (auto & d : deferred)
        {
            d.resize(num_threads);
            for (auto & p : d)
                p.reserve(data.size() / (num_threads * num_threads));
        }

        Stopwatch watch;

        /// Phase 1: Parallel aggregation with local cache + deferred partitioning
        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & local_map = local_maps[i];
                auto & my_deferred = deferred[i];

                for (auto it = begin; it != end; ++it)
                {
                    Key key = *it;
                    size_t hash = DefaultHash<Key>()(key);

                    /// Try local map first
                    if (!local_map.tryInsertOrIncrement(key, hash))
                    {
                        /// Local map full or too many collisions, defer to partition
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        my_deferred[partition].push_back(key);
                    }
                }
            });

        pool.wait();

        watch.stop();
        double time_phase1 = watch.elapsedSeconds();

        size_t total_local = 0;
        size_t total_deferred = 0;
        for (size_t i = 0; i < num_threads; ++i)
        {
            total_local += local_maps[i].size;
            for (size_t p = 0; p < num_threads; ++p)
                total_deferred += deferred[i][p].size();
        }

        std::cerr
            << "Phase 1 (local + defer) in " << time_phase1
            << " (" << n / time_phase1 << " elem/sec.)"
            << " local_keys=" << total_local << " deferred_keys=" << total_deferred
            << std::endl;

        /// Phase 2: Each thread aggregates deferred keys for its partition
        std::vector<Map> partitioned_maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = partitioned_maps[i];
                /// Gather all deferred keys from all threads for partition i
                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & key : deferred[t][i])
                        ++map[key];
                }
            });

        pool.wait();

        watch.stop();
        double time_phase2 = watch.elapsedSeconds();
        std::cerr
            << "Phase 2 (aggregate deferred) in " << time_phase2
            << " (" << total_deferred / time_phase2 << " elem/sec.)"
            << std::endl;

        /// Phase 3: Merge local maps into partitioned results
        /// Use per-partition mutexes to avoid race conditions
        std::vector<std::mutex> partition_mutexes(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & local_map = local_maps[i];
                /// For each entry in local map, merge into correct partition
                for (const auto & entry : local_map.entries)
                {
                    if (entry.occupied)
                    {
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(entry.key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        std::lock_guard lock(partition_mutexes[partition]);
                        partitioned_maps[partition][entry.key] += entry.value;
                    }
                }
            });

        pool.wait();

        watch.stop();
        double time_phase3 = watch.elapsedSeconds();
        std::cerr
            << "Phase 3 (merge local to partitioned) in " << time_phase3
            << " (" << total_local / time_phase3 << " elem/sec.)"
            << std::endl;

        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += partitioned_maps[i].size();

        double time_total = time_phase1 + time_phase2 + time_phase3;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    if (!method || method == 505)
    {
        std::cerr << "Method 505 (L1-cache local map + deferred partitioning, sequential merge):\n";
        /** Option 505.
          * Same as 504, but Phase 3 merges sequentially to avoid contention.
          * Each thread merges its local map entries, but we process one partition at a time.
          */

        static constexpr size_t LOCAL_MAP_MAX_SIZE = 2048;

        struct LocalEntry
        {
            Key key;
            Value value;
            bool occupied;
        };

        struct LocalMap
        {
            std::array<LocalEntry, LOCAL_MAP_MAX_SIZE * 2> entries{};
            size_t size = 0;

            bool tryInsertOrIncrement(Key key, size_t hash)
            {
                constexpr size_t MASK = LOCAL_MAP_MAX_SIZE * 2 - 1;
                size_t pos = hash & MASK;
                for (size_t probe = 0; probe < 16; ++probe)
                {
                    auto & entry = entries[pos];
                    if (!entry.occupied)
                    {
                        if (size >= LOCAL_MAP_MAX_SIZE)
                            return false;
                        entry.key = key;
                        entry.value = 1;
                        entry.occupied = true;
                        ++size;
                        return true;
                    }
                    if (entry.key == key)
                    {
                        ++entry.value;
                        return true;
                    }
                    pos = (pos + 1) & MASK;
                }
                return false;
            }
        };

        std::vector<LocalMap> local_maps(num_threads);
        std::vector<std::vector<std::vector<Key>>> deferred(num_threads);
        for (auto & d : deferred)
        {
            d.resize(num_threads);
            for (auto & p : d)
                p.reserve(data.size() / (num_threads * num_threads));
        }

        Stopwatch watch;

        /// Phase 1: Parallel aggregation
        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & local_map = local_maps[i];
                auto & my_deferred = deferred[i];

                for (auto it = begin; it != end; ++it)
                {
                    Key key = *it;
                    size_t hash = DefaultHash<Key>()(key);

                    if (!local_map.tryInsertOrIncrement(key, hash))
                    {
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        my_deferred[partition].push_back(key);
                    }
                }
            });

        pool.wait();

        watch.stop();
        double time_phase1 = watch.elapsedSeconds();

        size_t total_local = 0;
        size_t total_deferred = 0;
        for (size_t i = 0; i < num_threads; ++i)
        {
            total_local += local_maps[i].size;
            for (size_t p = 0; p < num_threads; ++p)
                total_deferred += deferred[i][p].size();
        }

        std::cerr
            << "Phase 1 (local + defer) in " << time_phase1
            << " (" << n / time_phase1 << " elem/sec.)"
            << " local_keys=" << total_local << " deferred_keys=" << total_deferred
            << std::endl;

        /// Phase 2: Aggregate deferred (parallel)
        std::vector<Map> partitioned_maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = partitioned_maps[i];
                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & key : deferred[t][i])
                        ++map[key];
                }
            });

        pool.wait();

        watch.stop();
        double time_phase2 = watch.elapsedSeconds();
        std::cerr
            << "Phase 2 (aggregate deferred) in " << time_phase2
            << " (" << total_deferred / time_phase2 << " elem/sec.)"
            << std::endl;

        /// Phase 3: Sequential merge of local maps
        /// Process one partition at a time to avoid contention
        watch.restart();

        for (size_t p = 0; p < num_threads; ++p)
        {
            /// Sequentially merge all local entries belonging to partition p
            for (size_t t = 0; t < num_threads; ++t)
            {
                auto & local_map = local_maps[t];
                for (const auto & entry : local_map.entries)
                {
                    if (entry.occupied)
                    {
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(entry.key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        if (partition == p)
                            partitioned_maps[p][entry.key] += entry.value;
                    }
                }
            }
        }

        watch.stop();
        double time_phase3 = watch.elapsedSeconds();
        std::cerr
            << "Phase 3 (merge local to partitioned) in " << time_phase3
            << " (" << total_local / time_phase3 << " elem/sec.)"
            << std::endl;

        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += partitioned_maps[i].size();

        double time_total = time_phase1 + time_phase2 + time_phase3;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    if (!method || method == 506)
    {
        std::cerr << "Method 506 (L1-cache local map + deferred partitioning, pre-partitioned local):\n";
        /** Option 506.
          * Optimization: during Phase 1, also pre-partition local map entries
          * so Phase 3 merge can be parallel without contention.
          */

        static constexpr size_t LOCAL_MAP_MAX_SIZE = 2048;

        struct LocalEntry
        {
            Key key;
            Value value;
            bool occupied;
        };

        struct LocalMap
        {
            std::array<LocalEntry, LOCAL_MAP_MAX_SIZE * 2> entries{};
            size_t size = 0;

            bool tryInsertOrIncrement(Key key, size_t hash)
            {
                constexpr size_t MASK = LOCAL_MAP_MAX_SIZE * 2 - 1;
                size_t pos = hash & MASK;
                for (size_t probe = 0; probe < 16; ++probe)
                {
                    auto & entry = entries[pos];
                    if (!entry.occupied)
                    {
                        if (size >= LOCAL_MAP_MAX_SIZE)
                            return false;
                        entry.key = key;
                        entry.value = 1;
                        entry.occupied = true;
                        ++size;
                        return true;
                    }
                    if (entry.key == key)
                    {
                        ++entry.value;
                        return true;
                    }
                    pos = (pos + 1) & MASK;
                }
                return false;
            }
        };

        std::vector<LocalMap> local_maps(num_threads);
        std::vector<std::vector<std::vector<Key>>> deferred(num_threads);
        for (auto & d : deferred)
        {
            d.resize(num_threads);
            for (auto & p : d)
                p.reserve(data.size() / (num_threads * num_threads));
        }

        /// Pre-partitioned local entries: local_partitioned[thread][partition] = vector of (key, value)
        std::vector<std::vector<std::vector<std::pair<Key, Value>>>> local_partitioned(num_threads);
        for (auto & lp : local_partitioned)
            lp.resize(num_threads);

        Stopwatch watch;

        /// Phase 1: Parallel aggregation
        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & local_map = local_maps[i];
                auto & my_deferred = deferred[i];

                for (auto it = begin; it != end; ++it)
                {
                    Key key = *it;
                    size_t hash = DefaultHash<Key>()(key);

                    if (!local_map.tryInsertOrIncrement(key, hash))
                    {
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        my_deferred[partition].push_back(key);
                    }
                }

                /// Pre-partition local map entries
                auto & my_local_partitioned = local_partitioned[i];
                for (const auto & entry : local_map.entries)
                {
                    if (entry.occupied)
                    {
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(entry.key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        my_local_partitioned[partition].emplace_back(entry.key, entry.value);
                    }
                }
            });

        pool.wait();

        watch.stop();
        double time_phase1 = watch.elapsedSeconds();

        size_t total_local = 0;
        size_t total_deferred = 0;
        for (size_t i = 0; i < num_threads; ++i)
        {
            total_local += local_maps[i].size;
            for (size_t p = 0; p < num_threads; ++p)
                total_deferred += deferred[i][p].size();
        }

        std::cerr
            << "Phase 1 (local + defer + partition) in " << time_phase1
            << " (" << n / time_phase1 << " elem/sec.)"
            << " local_keys=" << total_local << " deferred_keys=" << total_deferred
            << std::endl;

        /// Phase 2+3 combined: Each thread aggregates deferred + local entries for its partition
        std::vector<Map> partitioned_maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = partitioned_maps[i];

                /// Aggregate deferred keys
                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & key : deferred[t][i])
                        ++map[key];
                }

                /// Merge pre-partitioned local entries
                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & [key, value] : local_partitioned[t][i])
                        map[key] += value;
                }
            });

        pool.wait();

        watch.stop();
        double time_phase2 = watch.elapsedSeconds();
        std::cerr
            << "Phase 2+3 (aggregate all) in " << time_phase2
            << " (" << (total_deferred + total_local) / time_phase2 << " elem/sec.)"
            << std::endl;

        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += partitioned_maps[i].size();

        double time_total = time_phase1 + time_phase2;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    if (!method || method == 507)
    {
        std::cerr << "Method 507 (L2/L3-cache local map + deferred partitioning):\n";
        /** Option 507.
          * Same as 506 but with larger local map (64K entries, ~1MB)
          * to capture more frequent keys in L2/L3 cache.
          */

        static constexpr size_t LOCAL_MAP_MAX_SIZE = 65536;  /// 64K entries, ~1MB

        struct LocalEntry
        {
            Key key;
            Value value;
            bool occupied;
        };

        struct LocalMap
        {
            std::vector<LocalEntry> entries;
            size_t size = 0;

            LocalMap() : entries(LOCAL_MAP_MAX_SIZE * 2) {}

            bool tryInsertOrIncrement(Key key, size_t hash)
            {
                const size_t MASK = LOCAL_MAP_MAX_SIZE * 2 - 1;
                size_t pos = hash & MASK;
                for (size_t probe = 0; probe < 32; ++probe)
                {
                    auto & entry = entries[pos];
                    if (!entry.occupied)
                    {
                        if (size >= LOCAL_MAP_MAX_SIZE)
                            return false;
                        entry.key = key;
                        entry.value = 1;
                        entry.occupied = true;
                        ++size;
                        return true;
                    }
                    if (entry.key == key)
                    {
                        ++entry.value;
                        return true;
                    }
                    pos = (pos + 1) & MASK;
                }
                return false;
            }
        };

        std::vector<LocalMap> local_maps(num_threads);
        std::vector<std::vector<std::vector<Key>>> deferred(num_threads);
        for (auto & d : deferred)
        {
            d.resize(num_threads);
            for (auto & p : d)
                p.reserve(data.size() / (num_threads * num_threads));
        }

        std::vector<std::vector<std::vector<std::pair<Key, Value>>>> local_partitioned(num_threads);
        for (auto & lp : local_partitioned)
            lp.resize(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto begin = data.begin() + (data.size() * i) / num_threads;
                auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
                auto & local_map = local_maps[i];
                auto & my_deferred = deferred[i];

                for (auto it = begin; it != end; ++it)
                {
                    Key key = *it;
                    size_t hash = DefaultHash<Key>()(key);

                    if (!local_map.tryInsertOrIncrement(key, hash))
                    {
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        my_deferred[partition].push_back(key);
                    }
                }

                auto & my_local_partitioned = local_partitioned[i];
                for (const auto & entry : local_map.entries)
                {
                    if (entry.occupied)
                    {
                        UInt32 part_hash = static_cast<UInt32>(intHashCRC32(entry.key));
                        size_t partition = (static_cast<UInt64>(part_hash) * num_threads) >> 32;
                        my_local_partitioned[partition].emplace_back(entry.key, entry.value);
                    }
                }
            });

        pool.wait();

        watch.stop();
        double time_phase1 = watch.elapsedSeconds();

        size_t total_local = 0;
        size_t total_deferred = 0;
        for (size_t i = 0; i < num_threads; ++i)
        {
            total_local += local_maps[i].size;
            for (size_t p = 0; p < num_threads; ++p)
                total_deferred += deferred[i][p].size();
        }

        std::cerr
            << "Phase 1 (local + defer + partition) in " << time_phase1
            << " (" << n / time_phase1 << " elem/sec.)"
            << " local_keys=" << total_local << " deferred_keys=" << total_deferred
            << std::endl;

        std::vector<Map> partitioned_maps(num_threads);

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] {
                auto & map = partitioned_maps[i];

                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & key : deferred[t][i])
                        ++map[key];
                }

                for (size_t t = 0; t < num_threads; ++t)
                {
                    for (const auto & [key, value] : local_partitioned[t][i])
                        map[key] += value;
                }
            });

        pool.wait();

        watch.stop();
        double time_phase2 = watch.elapsedSeconds();
        std::cerr
            << "Phase 2+3 (aggregate all) in " << time_phase2
            << " (" << (total_deferred + total_local) / time_phase2 << " elem/sec.)"
            << std::endl;

        size_t total_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            total_size += partitioned_maps[i].size();

        double time_total = time_phase1 + time_phase2;
        std::cerr
            << "Total in \033[1m" << time_total << "\033[0m"
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << total_size << std::endl << std::endl;
    }

    return 0;
}
