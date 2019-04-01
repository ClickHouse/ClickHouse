#pragma once

#include <Columns/IColumn.h>
#include <Common/SortedLookupPODArray.h>

#include <optional>
#include <variant>

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

    template <typename T>
    struct LookupTypes
    {
        using ElementType = T;
        using SearcherType = SortedLookupPODArray<Entry<T>>;
        using Ptr = std::unique_ptr<SearcherType>;
    };

    using Lookups = std::variant<
        LookupTypes<UInt32>::Ptr,
        LookupTypes<UInt64>::Ptr,
        LookupTypes<Float32>::Ptr,
        LookupTypes<Float64>::Ptr>;

    enum class Type
    {
        key32,
        key64,
        keyf32,
        keyf64,
    };

    static std::optional<Type> getTypeSize(const IColumn * asof_column, size_t & type_size);

    AsofRowRefs() = default;
    AsofRowRefs(Type t)
        : type(t)
    {
        createLookup(t);
    }

    void insert(const IColumn * asof_column, const Block * block, size_t row_num, Arena & pool);
    const RowRef * findAsof(const IColumn * asof_column, size_t row_num, Arena & pool) const;

private:
    const std::optional<Type> type;
    mutable Lookups lookups;

    void createLookup(Type which);
};

}
