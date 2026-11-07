#pragma once

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>
#include <base/defines.h>
#include <Common/Allocator.h>


/** Experiment: the buckets of a `TwoLevelHashTable` can live in one shared memory area and grow together,
  * so that the area is large enough to be backed by transparent huge pages.
  * See `TwoLevelHashTable::growAllBuckets`.
  *
  * The area is reference-counted by the buckets that have their buffers in it: a bucket can be moved to another
  * table (`ConcurrentHashJoin` does it), or it can grow on its own when somebody inserts into it directly
  * (the merge of two-level aggregation states does it). In the latter case the bucket moves to a private buffer.
  */
struct TwoLevelHashTableArena
{
    void * data = nullptr;
    size_t size = 0;
    std::atomic<size_t> refs = 0;
};

/// Temporary knob for the A/B comparison with the same binary: `CLICKHOUSE_TWO_LEVEL_GROUPED=1` enables it.
inline bool twoLevelHashTableGroupedGrowthEnabled()
{
    static const bool value = []
    {
        const char * env = getenv("CLICKHOUSE_TWO_LEVEL_GROUPED"); // NOLINT(concurrency-mt-unsafe)
        return env && env[0] == '1';
    }();
    return value;
}

/// The allocator of a bucket of a `TwoLevelHashTable`. Without an arena it is the same as `Base`.
template <typename Base>
class TwoLevelHashTableBucketAllocator : private Base
{
    static_assert(allocatorInitialBytes<Base> == 0, "allocators with stack memory are not supported");

    TwoLevelHashTableArena * arena = nullptr;

    void release()
    {
        chassert(arena);
        if (arena->refs.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            Base::free(arena->data, arena->size);
            delete arena;
        }
        arena = nullptr;
    }

public:
    TwoLevelHashTableBucketAllocator() = default;
    TwoLevelHashTableBucketAllocator(const TwoLevelHashTableBucketAllocator &) = delete;
    TwoLevelHashTableBucketAllocator & operator=(const TwoLevelHashTableBucketAllocator &) = delete;

    TwoLevelHashTableBucketAllocator(TwoLevelHashTableBucketAllocator && rhs) noexcept
        : arena(std::exchange(rhs.arena, nullptr))
    {
    }

    /// `HashTable` frees its buffer before it takes the buffer of `rhs`, so there is no arena to release here.
    TwoLevelHashTableBucketAllocator & operator=(TwoLevelHashTableBucketAllocator && rhs) noexcept
    {
        chassert(!arena);
        arena = std::exchange(rhs.arena, nullptr);
        return *this;
    }

    ~TwoLevelHashTableBucketAllocator()
    {
        chassert(!arena);
    }

    void * alloc(size_t size, size_t alignment = 0)
    {
        chassert(!arena);
        return Base::alloc(size, alignment);
    }

    void free(void * buf, size_t size, size_t alignment = 0)
    {
        if (arena)
            release();
        else
            Base::free(buf, size, alignment);
    }

    /// A bucket that grows on its own leaves the arena.
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        if (!arena)
            return Base::realloc(buf, old_size, new_size, alignment);

        void * new_buf = Base::alloc(new_size, alignment);
        memcpy(new_buf, buf, std::min(old_size, new_size));
        release();
        return new_buf;
    }

    bool inArena() const { return arena != nullptr; }

    /// The memory is zero-filled. Every bucket that is placed into the arena takes one reference with `adoptArena`.
    static TwoLevelHashTableArena * createArena(size_t size)
    {
        auto arena = std::make_unique<TwoLevelHashTableArena>();
        arena->data = Base().alloc(size);
        arena->size = size;
        return arena.release();
    }

    /// For the case when no bucket has adopted the arena.
    static void destroyArena(TwoLevelHashTableArena * arena_)
    {
        chassert(arena_->refs == 0);
        Base().free(arena_->data, arena_->size);
        delete arena_;
    }

    void adoptArena(TwoLevelHashTableArena * arena_)
    {
        chassert(!arena);
        arena_->refs.fetch_add(1, std::memory_order_relaxed);
        arena = arena_;
    }
};

template <typename T>
constexpr bool is_two_level_hash_table_bucket_allocator = false;

template <typename Base>
constexpr bool is_two_level_hash_table_bucket_allocator<TwoLevelHashTableBucketAllocator<Base>> = true;
