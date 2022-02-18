#pragma once

#include <algorithm>
#include <cassert>
#include <list>
#include <mutex>
#include <optional>
#include <variant>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Interpreters/asof.h>
#include <base/sort.h>
#include <Common/Arena.h>


#include <base/logger_useful.h>

namespace DB
{

class Block;

/// Reference to the row in block.
struct __attribute__((__packed__)) RowRef
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    const Block * block = nullptr;
    SizeT row_num = 0;

    RowRef() = default;
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

        const RowRef * operator * () const
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

        bool ok() const { return first || batch; }

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
struct SortedLookupVectorBase
{
    SortedLookupVectorBase() = default;
    virtual ~SortedLookupVectorBase() { }

    static std::optional<TypeIndex> getTypeSize(const IColumn & asof_column, size_t & type_size);

    // This will be synchronized by the rwlock mutex in Join.h
    virtual void insert(const IColumn &, const Block *, size_t) = 0;

    // This needs to be synchronized internally
    virtual const RowRef * findAsof(const IColumn &, size_t) = 0;
};

template <typename TKey, ASOF::Inequality inequality>
class SortedLookupVector : public SortedLookupVectorBase
{
    struct Entry
    {
        TKey asof_value;
        RowRef row_ref;

        Entry() = delete;
        Entry(TKey v, const Block * block, size_t row_num) : asof_value(v), row_ref(block, row_num) { }

        bool operator<(const Entry & other) const { return asof_value < other.asof_value; }
    };

public:
    using Base = std::vector<Entry>;
    using Keys = std::vector<TKey>;

    void insert(const IColumn & asof_column, const Block * block, size_t row_num) override
    {
        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey k = column.getElement(row_num);

        assert(!sorted.load(std::memory_order_acquire));
        array.emplace_back(k, block, row_num);
    }

    /// Unrolled version of upper_bound and lower_bound
    /// Based on https://academy.realm.io/posts/how-we-beat-cpp-stl-binary-search/
    /// In the future it'd interesting to replace it with a B+Tree Layout as described
    /// at https://en.algorithmica.org/hpc/data-structures/s-tree/
    enum bound_type
    {
        upperBound,
        lowerBound
    };
    template <bound_type type>
    size_t fastBound(TKey value)
    {
        size_t size = array.size();
        size_t low = 0;

#define BOUND_ITERATION \
    { \
        size_t half = size / 2; \
        size_t other_half = size - half; \
        size_t probe = low + half; \
        size_t other_low = low + other_half; \
        TKey v = array[probe].asof_value; \
        size = half; \
        if constexpr (type == bound_type::upperBound) \
            low = value >= v ? other_low : low; \
        else \
            low = value > v ? other_low : low; \
    }

        /// Unroll the loop
        while (size >= 8)
        {
            BOUND_ITERATION
            BOUND_ITERATION
            BOUND_ITERATION
        }

        while (size > 0)
        {
            BOUND_ITERATION
        }

#undef BOUND_ITERATION
        return low;
    }

    /// Find an element based on the inequality rules
    /// Note that the inequality rules and the sort order are reversed, so when looking for
    const RowRef * findAsof(const IColumn & asof_column, size_t row_num) override
    {
        if (array.empty())
            return nullptr;
        sort();

        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey k = column.getElement(row_num);

        const size_t array_size = array.size();
        size_t pos = array_size;

        if constexpr (inequality == ASOF::Inequality::LessOrEquals)
        {
            pos = fastBound<lowerBound>(k);
        }
        else if constexpr (inequality == ASOF::Inequality::Less)
        {
            pos = fastBound<upperBound>(k);
        }
        else if constexpr (inequality == ASOF::Inequality::GreaterOrEquals)
        {
            size_t first_ge = fastBound<lowerBound>(k);
            if (first_ge == array_size)
                first_ge--;
            while (first_ge && array[first_ge].asof_value > k)
                first_ge--;
            if (array[first_ge].asof_value <= k)
                pos = first_ge;
        }
        else if constexpr (inequality == ASOF::Inequality::Greater)
        {
            size_t first_ge = fastBound<lowerBound>(k);
            if (first_ge == array_size)
                first_ge--;
            while (first_ge && array[first_ge].asof_value >= k)
                first_ge--;
            if (array[first_ge].asof_value < k)
                pos = first_ge;
        }

        if (pos != array_size)
            return &(array[pos].row_ref);

        return nullptr;
    }

private:
    std::atomic<bool> sorted = false;
    mutable std::mutex lock;
    Base array;

    // Double checked locking with SC atomics works in C++
    // https://preshing.com/20130930/double-checked-locking-is-fixed-in-cpp11/
    // The first thread that calls one of the lookup methods sorts the data
    // After calling the first lookup method it is no longer allowed to insert any data
    // the array becomes immutable
    void sort()
    {
        if (!sorted.load(std::memory_order_acquire))
        {
            std::lock_guard<std::mutex> l(lock);
            if (!sorted.load(std::memory_order_relaxed))
            {
                ::sort(array.begin(), array.end());
                sorted.store(true, std::memory_order_release);
            }
        }
    }
};


// It only contains a std::unique_ptr which is memmovable.
// Source: https://github.com/ClickHouse/ClickHouse/issues/4906
using AsofRowRefs = std::unique_ptr<SortedLookupVectorBase>;
AsofRowRefs createAsofRowRef(TypeIndex type, ASOF::Inequality inequality);
}
