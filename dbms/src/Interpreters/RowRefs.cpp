#include <Interpreters/RowRefs.h>

#include <Common/typeid_cast.h>
#include <Common/ColumnsHashing.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>


namespace DB
{

namespace
{

/// maps enum values to types
template <typename F>
void callWithType(AsofRowRefs::Type which, F && f)
{
    switch (which)
    {
        case AsofRowRefs::Type::key32:  return f(AsofRowRefs::LookupTypes<UInt32>());
        case AsofRowRefs::Type::key64:  return f(AsofRowRefs::LookupTypes<UInt64>());
        case AsofRowRefs::Type::keyf32: return f(AsofRowRefs::LookupTypes<Float32>());
        case AsofRowRefs::Type::keyf64: return f(AsofRowRefs::LookupTypes<Float64>());
    }

    __builtin_unreachable();
}

} // namespace


void AsofRowRefs::createLookup(AsofRowRefs::Type which)
{
    auto call = [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using SearcherType = typename Types::SearcherType;

        lookups = std::make_unique<SearcherType>();
    };

    callWithType(which, call);
}

template<typename T>
using AsofGetterType = ColumnsHashing::HashMethodOneNumber<T, T, T, false>;

void AsofRowRefs::insert(const IColumn * asof_column, const Block * block, size_t row_num, Arena & pool)
{
    auto call = [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using ElementType = typename Types::ElementType;
        using SearcherPtr = typename Types::Ptr;

        auto asof_getter = AsofGetterType<ElementType>(asof_column);
        auto entry = Entry<ElementType>(asof_getter.getKey(row_num, pool), RowRef(block, row_num));

        std::get<SearcherPtr>(lookups)->insert(entry);
    };

    callWithType(*type, call);
}

const RowRef * AsofRowRefs::findAsof(const IColumn * asof_column, size_t row_num, Arena & pool) const
{
    const RowRef * out = nullptr;

    auto call = [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using ElementType = typename Types::ElementType;
        using SearcherPtr = typename Types::Ptr;

        auto asof_getter = AsofGetterType<ElementType>(asof_column);
        ElementType key = asof_getter.getKey(row_num, pool);
        auto & typed_lookup = std::get<SearcherPtr>(lookups);

        auto it = typed_lookup->upper_bound(Entry<ElementType>(key));
        if (it != typed_lookup->cbegin())
            out = &((--it)->row_ref);
    };

    callWithType(*type, call);
    return out;
}

std::optional<AsofRowRefs::Type> AsofRowRefs::getTypeSize(const IColumn * asof_column, size_t & size)
{
    if (typeid_cast<const ColumnVector<UInt32> *>(asof_column))
    {
        size = sizeof(UInt32);
        return Type::key32;
    }
    else if (typeid_cast<const ColumnVector<UInt64> *>(asof_column))
    {
        size = sizeof(UInt64);
        return Type::key64;
    }
    else if (typeid_cast<const ColumnVector<Float32> *>(asof_column))
    {
        size = sizeof(Float32);
        return Type::keyf32;
    }
    else if (typeid_cast<const ColumnVector<Float64> *>(asof_column))
    {
        size = sizeof(Float64);
        return Type::keyf64;
    }

    size = 0;
    return {};
}

}
