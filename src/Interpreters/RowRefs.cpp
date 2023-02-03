#include <Interpreters/RowRefs.h>

#include <Core/Block.h>
#include <base/types.h>
#include <Common/typeid_cast.h>
#include <Common/ColumnsHashing.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

namespace
{

/// maps enum values to types
template <typename F>
void callWithType(TypeIndex which, F && f)
{
    switch (which)
    {
        case TypeIndex::UInt8:  return f(UInt8());
        case TypeIndex::UInt16: return f(UInt16());
        case TypeIndex::UInt32: return f(UInt32());
        case TypeIndex::UInt64: return f(UInt64());
        case TypeIndex::Int8:   return f(Int8());
        case TypeIndex::Int16:  return f(Int16());
        case TypeIndex::Int32:  return f(Int32());
        case TypeIndex::Int64:  return f(Int64());
        case TypeIndex::Float32: return f(Float32());
        case TypeIndex::Float64: return f(Float64());
        case TypeIndex::Decimal32: return f(Decimal32());
        case TypeIndex::Decimal64: return f(Decimal64());
        case TypeIndex::Decimal128: return f(Decimal128());
        case TypeIndex::DateTime64: return f(DateTime64());
        default:
            break;
    }

    __builtin_unreachable();
}

}


AsofRowRefs::AsofRowRefs(TypeIndex type)
{
    auto call = [&](const auto & t)
    {
      using T = std::decay_t<decltype(t)>;
      using LookupType = typename Entry<T>::LookupType;
      lookups = std::make_unique<LookupType>();
    };

    callWithType(type, call);
}

void AsofRowRefs::insert(TypeIndex type, const IColumn & asof_column, const Block * block, size_t row_num)
{
    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        using LookupPtr = typename Entry<T>::LookupPtr;

        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);
        auto entry = Entry<T>(key, RowRef(block, row_num));
        std::get<LookupPtr>(lookups)->insert(entry);
    };

    callWithType(type, call);
}

const RowRef * AsofRowRefs::findAsof(TypeIndex type, ASOF::Inequality inequality, const IColumn & asof_column, size_t row_num) const
{
    const RowRef * out = nullptr;

    bool ascending = (inequality == ASOF::Inequality::Less) || (inequality == ASOF::Inequality::LessOrEquals);
    bool is_strict = (inequality == ASOF::Inequality::Less) || (inequality == ASOF::Inequality::Greater);

    auto call = [&](const auto & t)
    {
        using T = std::decay_t<decltype(t)>;
        using EntryType = Entry<T>;
        using LookupPtr = typename EntryType::LookupPtr;

        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);
        T key = column.getElement(row_num);
        auto & typed_lookup = std::get<LookupPtr>(lookups);

        if (is_strict)
            out = typed_lookup->upperBound(EntryType(key), ascending);
        else
            out = typed_lookup->lowerBound(EntryType(key), ascending);
    };

    callWithType(type, call);
    return out;
}

std::optional<TypeIndex> AsofRowRefs::getTypeSize(const IColumn & asof_column, size_t & size)
{
    TypeIndex idx = asof_column.getDataType();

    switch (idx)
    {
        case TypeIndex::UInt8:
            size = sizeof(UInt8);
            return idx;
        case TypeIndex::UInt16:
            size = sizeof(UInt16);
            return idx;
        case TypeIndex::UInt32:
            size = sizeof(UInt32);
            return idx;
        case TypeIndex::UInt64:
            size = sizeof(UInt64);
            return idx;
        case TypeIndex::Int8:
            size = sizeof(Int8);
            return idx;
        case TypeIndex::Int16:
            size = sizeof(Int16);
            return idx;
        case TypeIndex::Int32:
            size = sizeof(Int32);
            return idx;
        case TypeIndex::Int64:
            size = sizeof(Int64);
            return idx;
        //case TypeIndex::Int128:
        case TypeIndex::Float32:
            size = sizeof(Float32);
            return idx;
        case TypeIndex::Float64:
            size = sizeof(Float64);
            return idx;
        case TypeIndex::Decimal32:
            size = sizeof(Decimal32);
            return idx;
        case TypeIndex::Decimal64:
            size = sizeof(Decimal64);
            return idx;
        case TypeIndex::Decimal128:
            size = sizeof(Decimal128);
            return idx;
        case TypeIndex::DateTime64:
            size = sizeof(DateTime64);
            return idx;
        default:
            break;
    }

    throw Exception("ASOF join not supported for type: " + std::string(asof_column.getFamilyName()), ErrorCodes::BAD_TYPE_OF_FIELD);
}

}
