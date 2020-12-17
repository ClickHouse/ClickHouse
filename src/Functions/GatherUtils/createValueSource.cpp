#include "GatherUtils.h"
#include "Sinks.h"
#include "Sources.h"
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
/// Creates IValueSource from Column

namespace
{

template <typename... Types>
struct ValueSourceCreator;

template <typename Type, typename... Types>
struct ValueSourceCreator<Type, Types...>
{
    static std::unique_ptr<IValueSource> create(const IColumn & col, const NullMap * null_map, bool is_const, size_t total_rows)
    {
        using ColVecType = std::conditional_t<IsDecimalNumber<Type>, ColumnDecimal<Type>, ColumnVector<Type>>;

        if (auto column_vector = typeid_cast<const ColVecType *>(&col))
        {
            if (null_map)
            {
                if (is_const)
                    return std::make_unique<ConstSource<NullableValueSource<NumericValueSource<Type>>>>(*column_vector, *null_map, total_rows);
                return std::make_unique<NullableValueSource<NumericValueSource<Type>>>(*column_vector, *null_map);
            }
            if (is_const)
                return std::make_unique<ConstSource<NumericValueSource<Type>>>(*column_vector, total_rows);
            return std::make_unique<NumericValueSource<Type>>(*column_vector);
        }

        return ValueSourceCreator<Types...>::create(col, null_map, is_const, total_rows);
    }
};

template <>
struct ValueSourceCreator<>
{
    static std::unique_ptr<IValueSource> create(const IColumn & col, const NullMap * null_map, bool is_const, size_t total_rows)
    {
        if (null_map)
        {
            if (is_const)
                return std::make_unique<ConstSource<NullableValueSource<GenericValueSource>>>(col, *null_map, total_rows);
            return std::make_unique<NullableValueSource<GenericValueSource>>(col, *null_map);
        }
        if (is_const)
            return std::make_unique<ConstSource<GenericValueSource>>(col, total_rows);
        return std::make_unique<GenericValueSource>(col);
    }
};

}

std::unique_ptr<IValueSource> createValueSource(const IColumn & col, bool is_const, size_t total_rows)
{
    using Creator = typename ApplyTypeListForClass<ValueSourceCreator, TypeListNumbersAndUInt128>::Type;
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&col))
    {
        return Creator::create(column_nullable->getNestedColumn(), &column_nullable->getNullMapData(), is_const, total_rows);
    }
    return Creator::create(col, nullptr, is_const, total_rows);
}

}
