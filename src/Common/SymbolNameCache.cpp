#include <Common/SymbolNameCache.h>
#include <Common/PerCPU.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <new>

namespace DB
{

namespace
{

ALWAYS_INLINE inline void cpuRelax()
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ volatile("yield");
#endif
}

/// The finalizer of MurmurHash3: symbol indices are dense, so the low bits must be mixed with the high ones.
ALWAYS_INLINE inline UInt32 mixKey(UInt32 key)
{
    key ^= key >> 16;
    key *= 0x85ebca6bU;
    key ^= key >> 13;
    key *= 0xc2b2ae35U;
    key ^= key >> 16;
    return key;
}

ALWAYS_INLINE inline size_t getBucketIndex(UInt32 key, size_t bucket_count)
{
    return mixKey(key) & (bucket_count - 1);
}

ALWAYS_INLINE inline UInt32 makeStamp(UInt32 clock, bool used)
{
    return (clock << 1) | static_cast<UInt32>(used);
}

/// Seconds of activity from the stamp to `clock`, modulo 2^31. A stamp ahead of the clock (a thread
/// may write a stamp with a clock value read just before another thread advanced it) counts as age 0.
ALWAYS_INLINE inline UInt32 stampAge(UInt32 stamp, UInt32 clock)
{
    UInt32 age = ((clock << 1) - (stamp & ~1U)) >> 1;
    return age >= (1U << 30) ? 0 : age;
}

ALWAYS_INLINE inline bool stampWasUsed(UInt32 stamp)
{
    return stamp & 1U;
}

/// Only hits count as use: a name that was inserted and never looked up again does not size the table.
ALWAYS_INLINE inline bool isRecentlyUsed(UInt32 stamp, UInt32 clock)
{
    return stampWasUsed(stamp) && stampAge(stamp, clock) <= SymbolNameCache::RECENT_WINDOW_SECONDS;
}

UInt32 readNameSize(const char * block)
{
    UInt32 size;
    memcpy(&size, block, sizeof(size));
    return size;
}

size_t getBucketCountForCapacity(size_t capacity)
{
    return std::bit_ceil(std::max<size_t>(1, (capacity + SymbolNameCache::BUCKET_SIZE - 1) / SymbolNameCache::BUCKET_SIZE));
}

/// Where a key goes in a bucket: a free place, or the victim. Recently used entries are never victims;
/// among the others the never used entry with the oldest stamp goes first, otherwise the used entry
/// with the oldest stamp.
template <typename Bucket>
struct BucketScan
{
    bool present = false;
    size_t free_place = SymbolNameCache::BUCKET_SIZE;
    size_t victim = SymbolNameCache::BUCKET_SIZE;

    BucketScan(const Bucket & bucket, UInt32 key, UInt32 clock)
    {
        UInt32 victim_age = 0;
        bool victim_was_used = true;
        for (size_t i = 0; i < SymbolNameCache::BUCKET_SIZE; ++i)
        {
            if (bucket.keys[i] == key)
            {
                present = true;
                return;
            }
            if (bucket.keys[i] == SymbolNameCache::EMPTY_KEY)
            {
                if (free_place == SymbolNameCache::BUCKET_SIZE)
                    free_place = i;
                continue;
            }
            UInt32 stamp = bucket.stamps[i].load(std::memory_order_relaxed);
            if (isRecentlyUsed(stamp, clock))
                continue;

            UInt32 age = stampAge(stamp, clock);
            bool used = stampWasUsed(stamp);
            if (victim == SymbolNameCache::BUCKET_SIZE || (!used && victim_was_used) || (used == victim_was_used && age > victim_age))
            {
                victim = i;
                victim_age = age;
                victim_was_used = used;
            }
        }
    }

    /// No free place and no victim: every entry is recently used, and only growth makes room.
    bool fullOfRecentlyUsed() const { return free_place == SymbolNameCache::BUCKET_SIZE && victim == SymbolNameCache::BUCKET_SIZE; }
};

}

SymbolNameCache::Bucket::Bucket()
{
    for (size_t i = 0; i < BUCKET_SIZE; ++i)
    {
        keys[i] = EMPTY_KEY;
        stamps[i].store(0, std::memory_order_relaxed);
        names[i] = nullptr;
    }
}

/// A reader inside the table. Several readers may share a slot. The increment and the check of
/// `writer_pending` are sequentially consistent, as are the store of `writer_pending` and the check
/// of the counters by the writer: either the reader sees the writer, or the writer sees the reader.
class SymbolNameCache::ReaderSection
{
public:
    explicit ReaderSection(SymbolNameCache & cache)
        : slot(cache.reader_slots[cache.getReaderSlotIndex()])
    {
        slot.readers.fetch_add(1, std::memory_order_seq_cst);
        entered = !cache.writer_pending.load(std::memory_order_seq_cst);
        if (!entered)
            slot.readers.fetch_sub(1, std::memory_order_release);
    }

    ~ReaderSection()
    {
        if (entered)
            slot.readers.fetch_sub(1, std::memory_order_release);
    }

    ReaderSection(const ReaderSection &) = delete;
    ReaderSection & operator=(const ReaderSection &) = delete;

    bool isEntered() const { return entered; }

private:
    ReaderSlot & slot;
    bool entered = false;
};

/// The right to modify the table, held by one insert or maintenance at a time.
class SymbolNameCache::WriterLock
{
public:
    explicit WriterLock(std::atomic<bool> & flag_)
        : flag(flag_)
        , locked(!flag.load(std::memory_order_relaxed) && !flag.exchange(true, std::memory_order_acquire))
    {
    }

    ~WriterLock()
    {
        if (locked)
            flag.store(false, std::memory_order_release);
    }

    WriterLock(const WriterLock &) = delete;
    WriterLock & operator=(const WriterLock &) = delete;

    bool isLocked() const { return locked; }

private:
    std::atomic<bool> & flag;
    const bool locked;
};

/// Keeps readers out of the table: sets `writer_pending` and waits, with a bounded spin per slot,
/// until no reader is inside. Owns nothing if some slot does not drain within the bound.
class SymbolNameCache::ExclusiveLock
{
public:
    explicit ExclusiveLock(SymbolNameCache & cache_)
        : cache(cache_)
    {
        cache.writer_pending.store(true, std::memory_order_seq_cst);
        for (size_t i = 0; i < cache.num_reader_slots; ++i)
        {
            size_t spins = 0;
            while (cache.reader_slots[i].readers.load(std::memory_order_seq_cst) != 0)
            {
                if (++spins >= WRITER_MAX_SPINS_PER_SLOT)
                    return;
                cpuRelax();
            }
        }
        locked = true;
        cache.writer_passes.fetch_add(1, std::memory_order_relaxed);
    }

    ~ExclusiveLock()
    {
        cache.writer_pending.store(false, std::memory_order_release);
    }

    ExclusiveLock(const ExclusiveLock &) = delete;
    ExclusiveLock & operator=(const ExclusiveLock &) = delete;

    bool isLocked() const { return locked; }

private:
    SymbolNameCache & cache;
    bool locked = false;
};

SymbolNameCache::SymbolNameCache(size_t min_capacity, size_t num_names)
    : num_reader_slots(std::max<size_t>(1, PerCPU::getNumPossibleCPUs()))
    , min_bucket_count(getBucketCountForCapacity(min_capacity))
    , max_bucket_count(std::max(min_bucket_count, getBucketCountForCapacity(2 * num_names)))
    , reader_slots(std::make_unique<ReaderSlot[]>(num_reader_slots))
{
}

SymbolNameCache::~SymbolNameCache()
{
    for (size_t b = 0; b < bucket_count; ++b)
        for (char * name : buckets[b].names)
            delete[] name;
}

void SymbolNameCache::holdReaderSlot(size_t index) noexcept
{
    reader_slots[index].readers.fetch_add(1, std::memory_order_seq_cst);
}

void SymbolNameCache::releaseReaderSlot(size_t index) noexcept
{
    reader_slots[index].readers.fetch_sub(1, std::memory_order_release);
}

size_t SymbolNameCache::getBucketIndexOfKey(UInt32 key) const noexcept
{
    return getBucketIndex(key, bucket_count);
}

size_t SymbolNameCache::getReaderSlotIndex() const noexcept
{
    Int32 cpu = PerCPU::getCurrentCPU();
    return cpu < 0 ? 0 : static_cast<size_t>(cpu) % num_reader_slots;
}

UInt32 SymbolNameCache::advanceClock(UInt64 now_seconds) noexcept
{
    /// Only a second later than any seen before advances the clock, so callers whose clocks differ
    /// slightly do not advance it back and forth; the compare-exchange lets exactly one of them do it.
    UInt64 last = last_caller_second.load(std::memory_order_relaxed);
    if (now_seconds > last && last_caller_second.compare_exchange_strong(last, now_seconds, std::memory_order_relaxed))
        return activity_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    return activity_clock.load(std::memory_order_relaxed);
}

bool SymbolNameCache::canGrow(size_t recently_used) const noexcept
{
    return bucket_count < max_bucket_count && 4 * recently_used >= bucket_count * BUCKET_SIZE;
}

size_t SymbolNameCache::getRecentlyUsedUpperBound() const noexcept
{
    return scan_count.load(std::memory_order_relaxed) + became_recently_used.load(std::memory_order_relaxed);
}

size_t SymbolNameCache::countRecentlyUsed(UInt32 clock) noexcept
{
    /// An entry that becomes recently used during the scan may be counted twice, or not at all if its increment is
    /// lost to the reset below while the scan still reads its old stamp. The estimate can then be short by such
    /// entries until the next scan, which at worst delays one growth.
    became_recently_used.store(0, std::memory_order_relaxed);
    table_scans.fetch_add(1, std::memory_order_relaxed);

    /// Readers may update stamps concurrently, but keys are stable while `writer_active` is held.
    size_t recently_used = 0;
    for (size_t b = 0; b < bucket_count; ++b)
        for (size_t i = 0; i < BUCKET_SIZE; ++i)
            if (buckets[b].keys[i] != EMPTY_KEY && isRecentlyUsed(buckets[b].stamps[i].load(std::memory_order_relaxed), clock))
                ++recently_used;

    scan_count.store(recently_used, std::memory_order_relaxed);
    scan_clock.store(clock, std::memory_order_relaxed);
    return recently_used;
}

size_t SymbolNameCache::getRecentlyUsedForGrowth(UInt32 clock) noexcept
{
    size_t upper_bound = getRecentlyUsedUpperBound();
    if (!canGrow(upper_bound) || scan_clock.load(std::memory_order_relaxed) == clock)
        return upper_bound;
    return countRecentlyUsed(clock);
}

std::optional<size_t> SymbolNameCache::tryGet(UInt32 key, UInt64 now_seconds, char * out, size_t out_size) noexcept
{
    const UInt32 clock = advanceClock(now_seconds);
    if (key == EMPTY_KEY)
        return std::nullopt;

    ReaderSection section(*this);
    if (!section.isEntered() || !buckets)
        return std::nullopt;

    Bucket & bucket = buckets[getBucketIndex(key, bucket_count)];
    for (size_t i = 0; i < BUCKET_SIZE; ++i)
    {
        if (bucket.keys[i] != key)
            continue;

        const char * block = bucket.names[i];
        UInt32 size = readNameSize(block);
        if (size >= out_size)
            return std::nullopt;

        memcpy(out, block + sizeof(UInt32), size);
        out[size] = 0;

        /// The stamp is refreshed rarely, so that hits on a hot entry do not keep writing a shared
        /// cache line. A hit in the second of the insertion is not a reuse: a name looked up several
        /// times in quick succession and never again must not size the table.
        UInt32 stamp = bucket.stamps[i].load(std::memory_order_relaxed);
        UInt32 age = stampAge(stamp, clock);
        bool used = stampWasUsed(stamp);
        if (used ? age >= REFRESH_INTERVAL_SECONDS : age >= 1)
        {
            bucket.stamps[i].store(makeStamp(clock, true), std::memory_order_relaxed);
            if (!used || age > RECENT_WINDOW_SECONDS)
                became_recently_used.fetch_add(1, std::memory_order_relaxed);
        }
        return size;
    }
    return std::nullopt;
}

void SymbolNameCache::add(UInt32 key, std::string_view name, UInt64 now_seconds) noexcept
{
    if (key == EMPTY_KEY || name.size() > UINT32_MAX - sizeof(UInt32))
        return;

    thread_local UInt32 offers = 0;
    if (mixKey(key ^ (++offers * 0x9E3779B9U)) % ADMISSION_ONE_IN != 0)
        return;

    const UInt32 clock = advanceClock(now_seconds);

    /// An insert that would change nothing must not keep readers out.
    {
        ReaderSection section(*this);
        if (!section.isEntered())
            return;
        if (buckets)
        {
            BucketScan<Bucket> scan(buckets[getBucketIndex(key, bucket_count)], key, clock);
            if (scan.present || (scan.fullOfRecentlyUsed() && !canGrow(getRecentlyUsedUpperBound())))
                return;
        }
    }

    WriterLock writer_lock(writer_active);
    if (!writer_lock.isLocked())
        return;

    /// Allocations and the rehash happen before the table is taken exclusively, so that lookups
    /// keep being served meanwhile. After the swap, `new_buckets` holds the old array, which is
    /// freed after readers are let in again.
    std::unique_ptr<Bucket[]> new_buckets;
    size_t new_bucket_count = 0;
    if (!buckets)
    {
        new_bucket_count = min_bucket_count;
        new_buckets.reset(new (std::nothrow) Bucket[new_bucket_count]);
        if (!new_buckets)
            return;
    }
    else
    {
        BucketScan<Bucket> scan(buckets[getBucketIndex(key, bucket_count)], key, clock);
        if (scan.present)
            return;
        if (scan.fullOfRecentlyUsed())
        {
            if (!canGrow(getRecentlyUsedForGrowth(clock)))
                return;

            new_bucket_count = bucket_count * 2;
            new_buckets.reset(new (std::nothrow) Bucket[new_bucket_count]);
            if (!new_buckets)
                return;

            /// Doubling splits every bucket into two, so the entries of an old bucket always fit.
            /// Hits on the old table during the rehash are not carried over to the stamps.
            for (size_t b = 0; b < bucket_count; ++b)
            {
                const Bucket & old_bucket = buckets[b];
                for (size_t i = 0; i < BUCKET_SIZE; ++i)
                {
                    UInt32 old_key = old_bucket.keys[i];
                    if (old_key == EMPTY_KEY)
                        continue;

                    Bucket & new_bucket = new_buckets[getBucketIndex(old_key, new_bucket_count)];
                    size_t place = 0;
                    while (new_bucket.keys[place] != EMPTY_KEY)
                        ++place;

                    new_bucket.keys[place] = old_key;
                    new_bucket.names[place] = old_bucket.names[i];
                    new_bucket.stamps[place].store(old_bucket.stamps[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
                }
            }
        }
    }

    size_t name_block_size = sizeof(UInt32) + name.size();
    std::unique_ptr<char[]> name_block(new (std::nothrow) char[name_block_size]);
    if (!name_block)
        return;
    UInt32 name_size = static_cast<UInt32>(name.size());
    memcpy(name_block.get(), &name_size, sizeof(name_size));
    memcpy(name_block.get() + sizeof(UInt32), name.data(), name.size());

    ExclusiveLock lock(*this);
    if (!lock.isLocked())
        return;

    if (new_buckets)
    {
        std::swap(buckets, new_buckets);
        std::swap(bucket_count, new_bucket_count);
        current_capacity.store(bucket_count * BUCKET_SIZE, std::memory_order_relaxed);
        bytes_allocated.fetch_add((bucket_count - new_bucket_count) * sizeof(Bucket), std::memory_order_relaxed);
    }

    insertExclusive(key, name_block.release(), name_block_size, clock);
}

void SymbolNameCache::insertExclusive(UInt32 key, char * name_block, size_t name_block_size, UInt32 clock) noexcept
{
    Bucket & bucket = buckets[getBucketIndex(key, bucket_count)];
    BucketScan<Bucket> scan(bucket, key, clock);
    if (scan.present || scan.fullOfRecentlyUsed())
    {
        delete[] name_block;
        return;
    }

    size_t place = scan.free_place;
    if (place == BUCKET_SIZE)
    {
        place = scan.victim;
        const char * victim = bucket.names[place];
        bytes_allocated.fetch_sub(sizeof(UInt32) + readNameSize(victim), std::memory_order_relaxed);
        num_entries.fetch_sub(1, std::memory_order_relaxed);
        delete[] victim;
    }

    bucket.keys[place] = key;
    bucket.names[place] = name_block;
    bucket.stamps[place].store(makeStamp(clock, false), std::memory_order_relaxed);
    num_entries.fetch_add(1, std::memory_order_relaxed);
    bytes_allocated.fetch_add(name_block_size, std::memory_order_relaxed);
}

void SymbolNameCache::maintain(UInt64 now_seconds) noexcept
{
    const UInt32 clock = advanceClock(now_seconds);
    UInt32 due = next_maintenance_clock.load(std::memory_order_relaxed);
    if (clock < due)
        return;
    if (!next_maintenance_clock.compare_exchange_strong(due, clock + MAINTENANCE_RETRY_SECONDS, std::memory_order_relaxed))
        return;

    WriterLock writer_lock(writer_active);
    if (!writer_lock.isLocked())
        return;

    if (shrinkIfMostlyIdle(clock))
        next_maintenance_clock.store(clock + MAINTENANCE_INTERVAL_SECONDS, std::memory_order_relaxed);
}

bool SymbolNameCache::shrinkIfMostlyIdle(UInt32 clock) noexcept
{
    if (!buckets || bucket_count <= min_bucket_count)
        return true;

    const size_t recently_used = countRecentlyUsed(clock);
    if (recently_used > bucket_count * BUCKET_SIZE / 16)
        return true;

    /// Built while readers keep using the old table. A recently used entry that does not fit into its
    /// bucket replaces the entry used least recently there, if that one is older.
    size_t new_bucket_count = std::max(min_bucket_count, getBucketCountForCapacity(8 * recently_used));
    std::unique_ptr<Bucket[]> new_buckets(new (std::nothrow) Bucket[new_bucket_count]);
    if (!new_buckets)
        return false;

    size_t new_entries = 0;
    size_t new_name_bytes = 0;
    for (size_t b = 0; b < bucket_count; ++b)
    {
        for (size_t i = 0; i < BUCKET_SIZE; ++i)
        {
            UInt32 key = buckets[b].keys[i];
            UInt32 stamp = buckets[b].stamps[i].load(std::memory_order_relaxed);
            if (key == EMPTY_KEY || !isRecentlyUsed(stamp, clock))
                continue;

            Bucket & new_bucket = new_buckets[getBucketIndex(key, new_bucket_count)];
            size_t place = 0;
            while (place < BUCKET_SIZE && new_bucket.keys[place] != EMPTY_KEY)
                ++place;

            if (place == BUCKET_SIZE)
            {
                auto age_in_new_bucket = [&](size_t j) { return stampAge(new_bucket.stamps[j].load(std::memory_order_relaxed), clock); };
                place = 0;
                for (size_t j = 1; j < BUCKET_SIZE; ++j)
                    if (age_in_new_bucket(j) > age_in_new_bucket(place))
                        place = j;

                if (age_in_new_bucket(place) <= stampAge(stamp, clock))
                    continue;

                --new_entries;
                new_name_bytes -= sizeof(UInt32) + readNameSize(new_bucket.names[place]);
            }

            new_bucket.keys[place] = key;
            new_bucket.names[place] = buckets[b].names[i];
            new_bucket.stamps[place].store(stamp, std::memory_order_relaxed);
            ++new_entries;
            new_name_bytes += sizeof(UInt32) + readNameSize(buckets[b].names[i]);
        }
    }

    {
        ExclusiveLock lock(*this);
        if (!lock.isLocked())
            return false;

        std::swap(buckets, new_buckets);
        std::swap(bucket_count, new_bucket_count);
        num_entries.store(new_entries, std::memory_order_relaxed);
        bytes_allocated.store(new_name_bytes + bucket_count * sizeof(Bucket), std::memory_order_relaxed);
        current_capacity.store(bucket_count * BUCKET_SIZE, std::memory_order_relaxed);
        scan_count.store(new_entries, std::memory_order_relaxed);
    }

    /// Readers no longer see the old table. Free the names that were not moved to the new one; the
    /// old array itself is freed together with `new_buckets`.
    for (size_t b = 0; b < new_bucket_count; ++b)
    {
        Bucket & old_bucket = new_buckets[b];
        for (size_t i = 0; i < BUCKET_SIZE; ++i)
        {
            UInt32 key = old_bucket.keys[i];
            if (key == EMPTY_KEY)
                continue;
            const Bucket & bucket = buckets[getBucketIndex(key, bucket_count)];
            if (std::find(std::begin(bucket.keys), std::end(bucket.keys), key) == std::end(bucket.keys))
                delete[] old_bucket.names[i];
        }
    }

    return true;
}

}
