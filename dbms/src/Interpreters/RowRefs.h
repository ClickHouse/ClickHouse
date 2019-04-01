#pragma once

#include <Columns/IColumn.h>
#include <Common/SortedLookupPODArray.h>

#include <optional>

namespace DB
{

class Block;

/// Reference to the row in block.
struct RowRef
{
    const Block * block = nullptr;
    size_t row_num = 0;

    RowRef() {}
    RowRef(const Block * block_, size_t row_num_) : block(block_), row_num(row_num_) {}
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefList : RowRef
{
    RowRefList * next = nullptr;

    RowRefList() {}
    RowRefList(const Block * block_, size_t row_num_) : RowRef(block_, row_num_) {}
};

class AsofRowRefs
{
public:
    /// Different types of asof join keys
    #define APPLY_FOR_ASOF_JOIN_VARIANTS(M) \
            M(key32, UInt32)                    \
            M(key64, UInt64)                    \
            M(keyf32, Float32)                  \
            M(keyf64, Float64)

    enum class Type
    {
        EMPTY,
    #define M(NAME, TYPE) NAME,
        APPLY_FOR_ASOF_JOIN_VARIANTS(M)
    #undef M
    };

    static std::optional<std::pair<Type, size_t>> getTypeSize(const IColumn * asof_column);

    template<typename T>
    struct Entry
    {
        T asof_value;
        RowRef row_ref;

        Entry(T v) : asof_value(v) {}
        Entry(T v, RowRef rr) : asof_value(v), row_ref(rr) {}

        bool operator< (const Entry& o) const
        {
            return asof_value < o.asof_value;
        }
    };

    struct Lookups
    {
    #define M(NAME, TYPE) \
            std::unique_ptr<SortedLookupPODArray<Entry<TYPE>>> NAME;
        APPLY_FOR_ASOF_JOIN_VARIANTS(M)
    #undef M

        void create(Type which);
    };

    AsofRowRefs() : type(Type::EMPTY) {}
    AsofRowRefs(Type t) : type(t)
    {
        lookups.create(t);
    }

    void insert(const IColumn * asof_column, const Block * block, size_t row_num, Arena & pool);
    const RowRef * findAsof(const IColumn * asof_column, size_t row_num, Arena & pool) const;

private:
    const Type type;
    mutable Lookups lookups;
};

}
