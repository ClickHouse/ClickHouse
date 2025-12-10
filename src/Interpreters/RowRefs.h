#pragma once

#include <optional>

#include <Columns/IColumn_fwd.h>
#include <Core/Joins.h>
#include <Core/TypeId.h>
#include <Common/Arena.h>


namespace DB
{

class Block;

/// Reference to the row in block.
struct RowRef
{
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    const Columns * columns = nullptr;
    SizeT row_num = 0;

    RowRef() = default;
    RowRef(const Columns * columns_, size_t row_num_)
        : columns(columns_)
        , row_num(static_cast<SizeT>(row_num_))
    {}
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

        explicit Batch(Batch * parent)
            : next(parent)
        {}

        bool full() const { return size == MAX_SIZE; }

        Batch * insert(RowRef && row_ref, Arena & pool)
        {
            if (full())
            {
                auto * batch = pool.alloc<Batch>();
                *batch = Batch(this);
                batch->insert(std::move(row_ref), pool);
                return batch;
            }

            row_refs[size] = std::move(row_ref);
            ++size;
            return this;
        }
    };

    class ForwardIterator
    {
    public:
        explicit ForwardIterator(const RowRefList * begin)
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

    RowRefList() {} /// NOLINT
    RowRefList(const Columns * columns_, size_t row_num_) : RowRef(columns_, row_num_), rows(1) {}
    RowRefList(const Columns * columns_, size_t row_start_, size_t rows_) : RowRef(columns_, row_start_), rows(static_cast<SizeT>(rows_)) {}

    ForwardIterator begin() const { return ForwardIterator(this); }

    /// Check that RowRefList represent a range of consecutive rows
    /// In this case there must be no next element
    void assertIsRange() const
    {
        chassert(rows >= 1, "RowRefList should have at least one row");
        chassert(next == nullptr, "When RowRefList represent range, it should not have next element");
    }

    /// insert element after current one
    void insert(RowRef && row_ref, Arena & pool)
    {
        if (!next)
        {
            next = pool.alloc<Batch>();
            *next = Batch(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
        ++rows;
    }

public:
    SizeT rows = 0;
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
    virtual ~SortedLookupVectorBase() = default;

    static std::optional<TypeIndex> getTypeSize(const IColumn & asof_column, size_t & type_size);

    // This will be synchronized by the rwlock mutex in Join.h
    virtual void insert(const IColumn &, const Columns *, size_t) = 0;

    // This needs to be synchronized internally
    virtual RowRef * findAsof(const IColumn &, size_t) = 0;
};


// It only contains a std::unique_ptr which is memmovable.
// Source: https://github.com/ClickHouse/ClickHouse/issues/4906
using AsofRowRefs = std::unique_ptr<SortedLookupVectorBase>;
AsofRowRefs createAsofRowRef(TypeIndex type, ASOFJoinInequality inequality);
}
