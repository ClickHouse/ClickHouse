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
        case AsofRowRefs::Type::key32:  return f(UInt32());
        case AsofRowRefs::Type::key64:  return f(UInt64());
        case AsofRowRefs::Type::keyf32: return f(Float32());
        case AsofRowRefs::Type::keyf64: return f(Float64());
    }

    __builtin_unreachable();
}

} // namespace


void AsofRowRefs::insert(Type type, LookupLists & lookup_data, const IColumn * asof_column, const Block * block, size_t row_num)
{
    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        using LookupType = typename Entry<T>::LookupType;

        auto * column = typeid_cast<const ColumnVector<T> *>(asof_column);
        T key = column->getElement(row_num);
        auto entry = Entry<T>(key, RowRef(block, row_num));

        std::lock_guard<std::mutex> lock(lookup_data.mutex);

        if (!lookups)
        {
            lookup_data.lookups.push_back(Lookups());
            lookup_data.lookups.back() = LookupType();
            lookups = &lookup_data.lookups.back();
        }
        std::get<LookupType>(*lookups).insert(entry);
    };

    callWithType(type, call);
}

const RowRef * AsofRowRefs::findAsof(Type type, const LookupLists & lookup_data, const IColumn * asof_column, size_t row_num) const
{
    const RowRef * out = nullptr;

    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        using LookupType = typename Entry<T>::LookupType;

        auto * column = typeid_cast<const ColumnVector<T> *>(asof_column);
        T key = column->getElement(row_num);

        std::lock_guard<std::mutex> lock(lookup_data.mutex);

        if (!lookups)
            return;

        auto & typed_lookup = std::get<LookupType>(*lookups);
        auto it = typed_lookup.upper_bound(Entry<T>(key));
        if (it != typed_lookup.cbegin())
            out = &((--it)->row_ref);
    };

    callWithType(type, call);
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
