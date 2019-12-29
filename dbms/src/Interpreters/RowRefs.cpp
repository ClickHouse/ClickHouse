#include <Interpreters/RowRefs.h>

#include <Core/Block.h>
#include <Core/Types.h>
#include <Common/typeid_cast.h>
#include <Common/ColumnsHashing.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>


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
        case AsofRowRefs::Type::keyu32:  return f(UInt32());
        case AsofRowRefs::Type::keyu64:  return f(UInt64());
        case AsofRowRefs::Type::keyi32:  return f(Int32());
        case AsofRowRefs::Type::keyi64:  return f(Int64());
        case AsofRowRefs::Type::keyf32: return f(Float32());
        case AsofRowRefs::Type::keyf64: return f(Float64());
        case AsofRowRefs::Type::keyDecimal32: return f(Decimal32());
        case AsofRowRefs::Type::keyDecimal64: return f(Decimal64());
        case AsofRowRefs::Type::keyDecimal128: return f(Decimal128());
    }

    __builtin_unreachable();
}

}


AsofRowRefs::AsofRowRefs(Type type)
{
    auto call = [&](const auto & t)
    {
      using T = std::decay_t<decltype(t)>;
      using LookupType = typename Entry<T>::LookupType;
      lookups = std::make_unique<LookupType>();
    };

    callWithType(type, call);
}

void AsofRowRefs::insert(Type type, const IColumn * asof_column, const Block * block, size_t row_num)
{
    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        using LookupPtr = typename Entry<T>::LookupPtr;

        using ColumnType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
        auto * column = typeid_cast<const ColumnType *>(asof_column);

        T key = column->getElement(row_num);
        auto entry = Entry<T>(key, RowRef(block, row_num));
        std::get<LookupPtr>(lookups)->insert(entry);
    };

    callWithType(type, call);
}

const RowRef * AsofRowRefs::findAsof(Type type, ASOF::Inequality inequality, const IColumn * asof_column, size_t row_num) const
{
    const RowRef * out = nullptr;

    bool ascending = (inequality == ASOF::Inequality::Less) || (inequality == ASOF::Inequality::LessOrEquals);
    bool is_strict = (inequality == ASOF::Inequality::Less) || (inequality == ASOF::Inequality::Greater);

    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        using EntryType = Entry<T>;
        using LookupPtr = typename EntryType::LookupPtr;

        using ColumnType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
        auto * column = typeid_cast<const ColumnType *>(asof_column);
        T key = column->getElement(row_num);
        auto & typed_lookup = std::get<LookupPtr>(lookups);

        if (is_strict)
            out = typed_lookup->upperBound(EntryType(key), ascending);
        else
            out = typed_lookup->lowerBound(EntryType(key), ascending);
    };

    callWithType(type, call);
    return out;
}

std::optional<AsofRowRefs::Type> AsofRowRefs::getTypeSize(const IColumn * asof_column, size_t & size)
{
    if (typeid_cast<const ColumnVector<UInt32> *>(asof_column))
    {
        size = sizeof(UInt32);
        return Type::keyu32;
    }
    else if (typeid_cast<const ColumnVector<UInt64> *>(asof_column))
    {
        size = sizeof(UInt64);
        return Type::keyu64;
    }
    else if (typeid_cast<const ColumnVector<Int32> *>(asof_column))
    {
        size = sizeof(Int32);
        return Type::keyi32;
    }
    else if (typeid_cast<const ColumnVector<Int64> *>(asof_column))
    {
        size = sizeof(Int64);
        return Type::keyi64;
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
    else if (typeid_cast<const ColumnDecimal<Decimal32> *>(asof_column))
    {
        size = sizeof(Decimal32);
        return Type::keyDecimal32;
    }
    else if (typeid_cast<const ColumnDecimal<Decimal64> *>(asof_column))
    {
        size = sizeof(Decimal64);
        return Type::keyDecimal64;
    }
    else if (typeid_cast<const ColumnDecimal<Decimal128> *>(asof_column))
    {
        size = sizeof(Decimal128);
        return Type::keyDecimal128;
    }

    size = 0;
    return {};
}

}
