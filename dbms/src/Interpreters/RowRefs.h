#pragma once

#include <Columns/IColumn.h>
#include <Common/SortedLookupPODArray.h>

#include <optional>
#include <variant>
#include <list>
#include <mutex>

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
    template <typename T>
    struct Entry
    {
        using LookupType = SortedLookupPODArray<Entry<T>>;

        T asof_value;
        RowRef row_ref;

        Entry(T v) : asof_value(v) {}
        Entry(T v, RowRef rr) : asof_value(v), row_ref(rr) {}

        bool operator < (const Entry & o) const
        {
            return asof_value < o.asof_value;
        }
    };

    using Lookups = std::variant<
        Entry<UInt32>::LookupType,
        Entry<UInt64>::LookupType,
        Entry<Float32>::LookupType,
        Entry<Float64>::LookupType>;

    struct LookupLists
    {
        mutable std::mutex mutex;
        std::list<Lookups> lookups;
    };

    enum class Type
    {
        key32,
        key64,
        keyf32,
        keyf64,
    };

    static std::optional<Type> getTypeSize(const IColumn * asof_column, size_t & type_size);

    void insert(Type type, LookupLists &, const IColumn * asof_column, const Block * block, size_t row_num);
    const RowRef * findAsof(Type type, const LookupLists &, const IColumn * asof_column, size_t row_num) const;

private:
    Lookups * lookups = nullptr;
};

}
