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
    virtual const RowRef * findAsof(ASOF::Inequality, const IColumn &, size_t) = 0;
};

template <typename TKey>
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

    inline size_t fast_upper_bound(TKey value)
    {
        size_t size = array.size();
        size_t low = 0;

        while (size >= 8)
        {
            size_t half = size / 2;
            size_t other_half = size - half;
            size_t probe = low + half;
            size_t other_low = low + other_half;
            TKey v = array[probe].asof_value;
            size = half;
            low = value >= v ? other_low : low;

            half = size / 2;
            other_half = size - half;
            probe = low + half;
            other_low = low + other_half;
            v = array[probe].asof_value;
            size = half;
            low = value >= v ? other_low : low;

            half = size / 2;
            other_half = size - half;
            probe = low + half;
            other_low = low + other_half;
            v = array[probe].asof_value;
            size = half;
            low = value >= v ? other_low : low;
        }

        while (size > 0)
        {
            size_t half = size / 2;
            size_t other_half = size - half;
            size_t probe = low + half;
            size_t other_low = low + other_half;
            TKey v = array[probe].asof_value;
            size = half;
            low = value >= v ? other_low : low;
        }

        return low;
    }

    /// Find an element based on the inequality rules
    /// Note that the inequality rules and the sort order are reversed, so when looking for
    /// ASOF::Inequality::LessOrEquals we are actually looking for the first element that's greater or equal than k
    const RowRef * findAsof(ASOF::Inequality inequality, const IColumn & asof_column, size_t row_num) override
    {
        if (array.empty())
            return nullptr;
        sort();

        using ColumnType = ColumnVectorOrDecimal<TKey>;
        const auto & column = assert_cast<const ColumnType &>(asof_column);
        TKey k = column.getElement(row_num);

        const size_t array_size = array.size();
        size_t pos = array_size;

        size_t first_ge = fast_upper_bound(k);
        switch (inequality)
        {
            case ASOF::Inequality::LessOrEquals:
            {
                if (first_ge == array.size())
                    first_ge--;
                while (first_ge && array[first_ge - 1].asof_value >= k)
                    first_ge--;
                if (array[first_ge].asof_value >= k)
                    pos = first_ge;
                break;
            }
            case ASOF::Inequality::Less:
            {
                pos = first_ge;
                break;
            }
            case ASOF::Inequality::GreaterOrEquals:
            {
                if (first_ge == array.size())
                    first_ge--;
                while (first_ge && array[first_ge].asof_value > k)
                    first_ge--;
                if (array[first_ge].asof_value <= k)
                    pos = first_ge;
                break;
            }
            case ASOF::Inequality::Greater:
            {
                if (first_ge == array.size())
                    first_ge--;
                while (first_ge && array[first_ge].asof_value >= k)
                    first_ge--;
                if (array[first_ge].asof_value < k)
                    pos = first_ge;
                break;
            }
            default:
                throw Exception("Invalid ASOF Join order", ErrorCodes::LOGICAL_ERROR);
        }

        if (pos != array.size())
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
AsofRowRefs createAsofRowRef(TypeIndex type);
}
