#pragma once

#include <algorithm>
#include <cassert>
#include <list>
#include <mutex>
#include <optional>
#include <variant>

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Joins.h>
#include <base/sort.h>
#include <Common/Arena.h>


namespace DB
{

class Block;

/// Reference to the row in block.
struct RowRef
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    const Block * block = nullptr;
    SizeT row_num = 0;

    RowRef() = default;
    RowRef(const Block * block_, size_t row_num_)
        : block(block_)
        , row_num(static_cast<SizeT>(row_num_))
    {}
} __attribute__((packed));

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefList : RowRef
{
    /// Portion of RowRefs, 16 * (MAX_SIZE + 1) bytes sized.
    struct Batch
    {
        static constexpr size_t MAX_SIZE = 9;

        SizeT size = 0; /// It's smaller than size_t but keeps align in Arena.
        Batch * next;
        RowRef row_refs[MAX_SIZE];

        explicit Batch(Batch * parent)
            : next(parent)
        {}

        explicit Batch(Batch * parent, RowRef && row_ref) : size(1), next(parent) { row_refs[0] = std::move(row_ref); }

        bool full() const { return size == MAX_SIZE; }

        Batch * insert(RowRef && row_ref, Arena & pool)
        {
            if (full())
            {
                auto * batch = pool.alloc<Batch>();
                *batch = Batch(this, std::move(row_ref));
                return batch;
            }

            row_refs[size] = std::move(row_ref);
            ++size;
            return this;
        }
    };

    static_assert(sizeof(Batch) <= 128);

    class ForwardIterator
    {
    public:
        explicit ForwardIterator(const RowRefList * begin) : position(0), first(true), root(begin), batch(root->next) { }

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
        size_t position;
        bool first;
        const RowRefList * root;
        Batch * batch;
    };

    RowRefList() {} /// NOLINT
    RowRefList(const Block * block_, size_t row_num_) : RowRef(block_, row_num_), rows(1) {}
    RowRefList(const Block * block_, size_t row_start_, size_t rows_) : RowRef(block_, row_start_), rows(static_cast<SizeT>(rows_)) {}

    ForwardIterator begin() const { return ForwardIterator(this); }

    /// insert element after current one
    void insert(RowRef && row_ref, Arena & pool)
    {
        rows++;
        if (likely(next))
            next = next->insert(std::move(row_ref), pool);
        else
        {
            next = pool.alloc<Batch>();
            *next = Batch(nullptr, std::move(row_ref));
        }
    }

private:
    Batch * next = nullptr;

public:
    SizeT rows = 0;
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
    virtual ~SortedLookupVectorBase() = default;

    static std::optional<TypeIndex> getTypeSize(const IColumn & asof_column, size_t & type_size);

    // This will be synchronized by the rwlock mutex in Join.h
    virtual void insert(const IColumn &, const Block *, size_t) = 0;

    // This needs to be synchronized internally
    virtual RowRef * findAsof(const IColumn &, size_t) = 0;
};


// It only contains a std::unique_ptr which is memmovable.
// Source: https://github.com/ClickHouse/ClickHouse/issues/4906
using AsofRowRefs = std::unique_ptr<SortedLookupVectorBase>;
AsofRowRefs createAsofRowRef(TypeIndex type, ASOFJoinInequality inequality);
}
