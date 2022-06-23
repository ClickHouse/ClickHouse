#pragma once

#include <Common/Arena.h>
#include <Columns/IColumn.h>
#include <Interpreters/asof.h>

#include <optional>
#include <variant>
#include <list>
#include <mutex>
#include <algorithm>

namespace DB
{

class Block;

/// Reference to the row in block.
struct RowRef
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    const Block * block = nullptr;
    SizeT row_num = 0;

    RowRef() {}
    RowRef(const Block * block_, size_t row_num_) : block(block_), row_num(row_num_) {}
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefList : RowRef
{
    /// Portion of RowRefs, 16 * (MAX_SIZE + 1) bytes sized.
    struct Batch
    {
        static constexpr size_t MAX_SIZE = 7; /// Adequate values are 3, 7, 15, 31.

        SizeT size = 0; /// It's smaller than size_t but keeps align in Arena.
        Batch * next;
        RowRef row_refs[MAX_SIZE];

        Batch(Batch * parent)
            : next(parent)
        {}

        bool full() const { return size == MAX_SIZE; }

        Batch * insert(RowRef && row_ref, Arena & pool)
        {
            if (full())
            {
                auto batch = pool.alloc<Batch>();
                *batch = Batch(this);
                batch->insert(std::move(row_ref), pool);
                return batch;
            }

            row_refs[size++] = std::move(row_ref);
            return this;
        }
    };

    class ForwardIterator
    {
    public:
        ForwardIterator(const RowRefList * begin)
            : root(begin)
            , first(true)
            , batch(root->next)
            , position(0)
        {}

        const RowRef * operator -> () const
        {
            if (first)
                return root;
            return &batch->row_refs[position];
        }

        void operator ++ ()
        {
            if (first)
            {
                first = false;
                return;
            }

            if (batch)
            {
                ++position;
                if (position >= batch->size)
                {
                    batch = batch->next;
                    position = 0;
                }
            }
        }

        bool ok() const { return first || (batch && position < batch->size); }

    private:
        const RowRefList * root;
        bool first;
        Batch * batch;
        size_t position;
    };

    RowRefList() {}
    RowRefList(const Block * block_, size_t row_num_) : RowRef(block_, row_num_) {}

    ForwardIterator begin() const { return ForwardIterator(this); }

    /// insert element after current one
    void insert(RowRef && row_ref, Arena & pool)
    {
        if (!next)
        {
            next = pool.alloc<Batch>();
            *next = Batch(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

private:
    Batch * next = nullptr;
};

/**
 * This class is intended to push sortable data into.
 * When looking up values the container ensures that it is sorted for log(N) lookup
 * After calling any of the lookup methods, it is no longer allowed to insert more data as this would invalidate the
 * references that can be returned by the lookup methods
 */

template <typename TEntry, typename TKey>
class SortedLookupVector
{
public:
    using Base = std::vector<TEntry>;

    // First stage, insertions into the vector
    template <typename U, typename ... TAllocatorParams>
    void insert(U && x, TAllocatorParams &&... allocator_params)
    {
        assert(!sorted.load(std::memory_order_acquire));
        array.push_back(std::forward<U>(x), std::forward<TAllocatorParams>(allocator_params)...);
    }

    const RowRef * upperBound(const TEntry & k, bool ascending)
    {
        sort(ascending);
        auto it = std::upper_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

    const RowRef * lowerBound(const TEntry & k, bool ascending)
    {
        sort(ascending);
        auto it = std::lower_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

private:
    std::atomic<bool> sorted = false;
    Base array;
    mutable std::mutex lock;

    static bool less(const TEntry & a, const TEntry & b)
    {
        return a.asof_value < b.asof_value;
    }

    static bool greater(const TEntry & a, const TEntry & b)
    {
        return a.asof_value > b.asof_value;
    }

    // Double checked locking with SC atomics works in C++
    // https://preshing.com/20130930/double-checked-locking-is-fixed-in-cpp11/
    // The first thread that calls one of the lookup methods sorts the data
    // After calling the first lookup method it is no longer allowed to insert any data
    // the array becomes immutable
    void sort(bool ascending)
    {
        if (!sorted.load(std::memory_order_acquire))
        {
            std::lock_guard<std::mutex> l(lock);
            if (!sorted.load(std::memory_order_relaxed))
            {
                if (!array.empty())
                    std::sort(array.begin(), array.end(), (ascending ? less : greater));

                sorted.store(true, std::memory_order_release);
            }
        }
    }
};

class AsofRowRefs
{
public:
    template <typename T>
    struct Entry
    {
        using LookupType = SortedLookupVector<Entry<T>, T>;
        using LookupPtr = std::unique_ptr<LookupType>;
        T asof_value;
        RowRef row_ref;

        Entry(T v) : asof_value(v) {}
        Entry(T v, RowRef rr) : asof_value(v), row_ref(rr) {}
    };

    using Lookups = std::variant<
        Entry<UInt8>::LookupPtr,
        Entry<UInt16>::LookupPtr,
        Entry<UInt32>::LookupPtr,
        Entry<UInt64>::LookupPtr,
        Entry<Int8>::LookupPtr,
        Entry<Int16>::LookupPtr,
        Entry<Int32>::LookupPtr,
        Entry<Int64>::LookupPtr,
        Entry<Float32>::LookupPtr,
        Entry<Float64>::LookupPtr,
        Entry<Decimal32>::LookupPtr,
        Entry<Decimal64>::LookupPtr,
        Entry<Decimal128>::LookupPtr,
        Entry<DateTime64>::LookupPtr>;

    AsofRowRefs() {}
    AsofRowRefs(TypeIndex t);

    static std::optional<TypeIndex> getTypeSize(const IColumn & asof_column, size_t & type_size);

    // This will be synchronized by the rwlock mutex in Join.h
    void insert(TypeIndex type, const IColumn & asof_column, const Block * block, size_t row_num);

    // This will internally synchronize
    const RowRef * findAsof(TypeIndex type, ASOF::Inequality inequality, const IColumn & asof_column, size_t row_num) const;

private:
    // Lookups can be stored in a HashTable because it is memmovable
    // A std::variant contains a currently active type id (memmovable), together with a union of the types
    // The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    // Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};

}
