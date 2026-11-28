#pragma once

#include <Common/CacheLine.h>
#include <base/types.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>

namespace DB
{

/// Process-wide cache of decoded symbol names, keyed by the index of a name in one symbol table.
///
/// Concurrency. Lookups never block and never allocate, so they can be used from the thread that
/// symbolizes the stack of a thread that received a fatal signal. Each CPU has a counter of readers
/// inside the table; a reader increments the counter of its CPU, and reports a miss if a writer has
/// announced itself. There is one writer at a time (an insert or maintenance); it announces itself
/// and waits, with a bounded spin, until every counter drops to zero, and gives up otherwise (e.g.
/// when a reader is preempted, or will never run again). Thus nothing waits without bound.
///
/// Time. The cache measures time in seconds of activity: its clock advances by one for each new
/// second (`now_seconds` of the callers, greater than any seen before) in which it is called, so a
/// pause between calls ages nothing. All durations below are in seconds of activity.
///
/// Sizing. The table is set-associative (`BUCKET_SIZE` entries per bucket); it is allocated on the
/// first insert with the minimum capacity, and its capacity stays between the minimum and twice the
/// number of names rounded up to a power of two. Only reuse sizes the table: an entry is used when
/// a lookup hits it in a later second than the one it was inserted in. A hit records the second in
/// the stamp of the entry at most every `REFRESH_INTERVAL_SECONDS`, and the entry is recently used
/// while its stamp is at most `RECENT_WINDOW_SECONDS` old: an entry hit continuously stays recently
/// used, and one hit for the last time stops being recently used 44 to 60 seconds later. A one-off
/// scan, or a name looked up a few times in quick succession and never again, inserts entries that
/// are never used.
/// - Admission: `add` accepts one offered miss out of `ADMISSION_ONE_IN`, so a name that keeps
///   missing is inserted after that many offers on average and a one-off scan inserts that fraction.
/// - Insert into a full bucket: recently used entries are never evicted. Of the others, the never
///   used entry inserted first is evicted, or else the used entry used least recently. If all entries
///   of the bucket are recently used, the table doubles when at least a quarter of its capacity is
///   recently used; otherwise the name is not cached, and stays uncached until the table grows or an
///   entry of its bucket stops being recently used. Such a name costs no table scan and no writer
///   pass in a steady state: the recently used entries are counted by a scan, at most once per
///   second, only when the last count plus the entries that became recently used since then reaches
///   the threshold.
/// - Maintenance, due every `MAINTENANCE_INTERVAL_SECONDS` and triggered by the caller after lookups,
///   counts the recently used entries and rebuilds the table at `max(minimum, bit_ceil(8 * count))`
///   entries when the count is at most a sixteenth of the capacity, keeping those entries and
///   freeing the rest (never used entries included). Memory is therefore released only from the
///   lookup path: an idle cache keeps its size until the next lookup, and a table at the minimum
///   capacity keeps its entries.
class SymbolNameCache
{
public:
    static constexpr UInt32 EMPTY_KEY = UINT32_MAX;
    static constexpr size_t BUCKET_SIZE = 8;
    static constexpr UInt32 ADMISSION_ONE_IN = 8;
    static constexpr UInt32 REFRESH_INTERVAL_SECONDS = 16;
    static constexpr UInt32 RECENT_WINDOW_SECONDS = 60;
    static constexpr UInt32 MAINTENANCE_INTERVAL_SECONDS = 60;
    static constexpr UInt32 MAINTENANCE_RETRY_SECONDS = 1;
    static constexpr size_t WRITER_MAX_SPINS_PER_SLOT = 4096;

    /// `min_capacity` is rounded up to a power of two not less than `BUCKET_SIZE`.
    /// `num_names` is the number of names in the symbol table; it bounds growth.
    SymbolNameCache(size_t min_capacity, size_t num_names);
    ~SymbolNameCache();

    SymbolNameCache(const SymbolNameCache &) = delete;
    SymbolNameCache & operator=(const SymbolNameCache &) = delete;

    /// On a hit copies the name followed by a NUL into `out` and returns the length of the name
    /// (without the NUL). Returns `std::nullopt` on a miss, including when the name with its NUL
    /// does not fit into `out_size` bytes or when a writer is active. Never allocates and never waits.
    std::optional<size_t> tryGet(UInt32 key, UInt64 now_seconds, char * out, size_t out_size) noexcept;

    /// To be called after a miss. Inserts the name if the offer is admitted and the insert would
    /// change the table. Returns without inserting if another writer is active, the readers do not
    /// leave within the spin bound, or an allocation fails.
    void add(UInt32 key, std::string_view name, UInt64 now_seconds) noexcept;

    /// To be called after lookups. When maintenance is not due, it is a few relaxed loads and
    /// comparisons. Otherwise one caller performs it, possibly allocating the new table and freeing
    /// names and the old table. If the table cannot be taken exclusively or the allocation fails,
    /// it is retried after `MAINTENANCE_RETRY_SECONDS`.
    void maintain(UInt64 now_seconds) noexcept;

    size_t size() const { return num_entries.load(std::memory_order_relaxed); }
    /// Bytes allocated for the bucket array and the names.
    size_t bytesAllocated() const { return bytes_allocated.load(std::memory_order_relaxed); }
    /// Zero until the table is allocated.
    size_t capacity() const { return current_capacity.load(std::memory_order_relaxed); }
    /// Number of times a writer took the table exclusively.
    size_t writerPasses() const { return writer_passes.load(std::memory_order_relaxed); }
    /// Number of scans of the whole table that counted the recently used entries.
    size_t tableScans() const { return table_scans.load(std::memory_order_relaxed); }

protected:
    size_t getNumReaderSlots() const { return num_reader_slots; }
    /// Count a reader in the slot or out of it, as a lookup does, without looking at the writer.
    void holdReaderSlot(size_t index) noexcept;
    void releaseReaderSlot(size_t index) noexcept;
    /// The bucket of the key in the current table, which must be allocated. Not synchronized with writers.
    size_t getBucketIndexOfKey(UInt32 key) const noexcept;

private:
    struct alignas(CH_CACHE_LINE_SIZE) ReaderSlot
    {
        std::atomic<UInt32> readers{0};
    };

    /// Keys and stamps of a bucket share the first cache line, so that a lookup scans one line.
    /// A stamp is `(clock << 1) | used`, with the activity clock taken modulo 2^31: the second of the
    /// insertion until the entry is used, then of the last recorded use. Readers write it concurrently
    /// with other readers, hence atomic. Keys and names change only in a table that readers cannot see
    /// or while the writer has the table exclusively.
    struct alignas(CH_CACHE_LINE_SIZE) Bucket
    {
        UInt32 keys[BUCKET_SIZE];
        std::atomic<UInt32> stamps[BUCKET_SIZE];
        /// Points to a `UInt32` length followed by the bytes of the name.
        char * names[BUCKET_SIZE];

        Bucket();
    };

    class ReaderSection;
    class ExclusiveLock;
    class WriterLock;

    /// Advances the activity clock if `now_seconds` is a new second, and returns the clock.
    UInt32 advanceClock(UInt64 now_seconds) noexcept;
    size_t getReaderSlotIndex() const noexcept;
    bool canGrow(size_t recently_used) const noexcept;
    /// An estimate from above of the number of recently used entries: the last count plus the entries that
    /// became recently used since then. It may miss an entry that became recently used while a scan was running.
    size_t getRecentlyUsedUpperBound() const noexcept;
    /// Requires `writer_active`. The number of recently used entries for the growth check: the upper
    /// bound while it is below the threshold, otherwise a count, rescanning at most once per second.
    size_t getRecentlyUsedForGrowth(UInt32 clock) noexcept;
    /// Requires `writer_active`. Scans the table and remembers the count.
    size_t countRecentlyUsed(UInt32 clock) noexcept;
    void insertExclusive(UInt32 key, char * name_block, size_t name_block_size, UInt32 clock) noexcept;
    /// Returns false if maintenance should be retried soon.
    bool shrinkIfMostlyIdle(UInt32 clock) noexcept;

    const size_t num_reader_slots;
    const size_t min_bucket_count;
    const size_t max_bucket_count;
    std::unique_ptr<ReaderSlot[]> reader_slots;

    alignas(CH_CACHE_LINE_SIZE) std::atomic<bool> writer_pending{false};
    /// Held by the only thread that may modify the table. That thread may read keys and names
    /// outside a reader section, since nobody else writes them.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<bool> writer_active{false};
    alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt32> next_maintenance_clock{0};

    /// Written once per second of activity.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt64> last_caller_second{0};
    std::atomic<UInt32> activity_clock{0};

    /// Written by lookups only when an entry becomes recently used: its first use, or a use after it
    /// stopped being recently used.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> became_recently_used{0};

    /// Replaced only while the writer has the table exclusively.
    alignas(CH_CACHE_LINE_SIZE) std::unique_ptr<Bucket[]> buckets;
    size_t bucket_count = 0;

    std::atomic<size_t> num_entries{0};
    std::atomic<size_t> bytes_allocated{0};
    std::atomic<size_t> current_capacity{0};
    std::atomic<size_t> writer_passes{0};
    std::atomic<size_t> table_scans{0};

    /// The number of recently used entries found by the last scan, at `scan_clock`. Written under `writer_active`.
    std::atomic<size_t> scan_count{0};
    std::atomic<UInt32> scan_clock{UINT32_MAX};
};

}
