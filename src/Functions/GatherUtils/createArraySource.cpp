#include "GatherUtils.h"
#include "Sinks.h"
#include "Sources.h"
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
/// Creates IArraySource from ColumnArray

namespace
{

template <typename... Types>
struct ArraySourceCreator;

template <typename Type, typename... Types>
struct ArraySourceCreator<Type, Types...>
{
    static std::unique_ptr<IArraySource> create(const ColumnArray & col, const NullMap * null_map, bool is_const, size_t total_rows)
    {
        using ColVecType = std::conditional_t<IsDecimalNumber<Type>, ColumnDecimal<Type>, ColumnVector<Type>>;

        if (typeid_cast<const ColVecType *>(&col.getData()))
        {
            if (null_map)
            {
                if (is_const)
                    return std::make_unique<ConstSource<NullableArraySource<NumericArraySource<Type>>>>(col, *null_map, total_rows);
                return std::make_unique<NullableArraySource<NumericArraySource<Type>>>(col, *null_map);
            }
            if (is_const)
                return std::make_unique<ConstSource<NumericArraySource<Type>>>(col, total_rows);
            return std::make_unique<NumericArraySource<Type>>(col);
        }

        return ArraySourceCreator<Types...>::create(col, null_map, is_const, total_rows);
    }
};

template <>
struct ArraySourceCreator<>
{
    static std::unique_ptr<IArraySource> create(const ColumnArray & col, const NullMap * null_map, bool is_const, size_t total_rows)
    {
        if (null_map)
        {
            if (is_const)
                return std::make_unique<ConstSource<NullableArraySource<GenericArraySource>>>(col, *null_map, total_rows);
            return std::make_unique<NullableArraySource<GenericArraySource>>(col, *null_map);
        }
        if (is_const)
            return std::make_unique<ConstSource<GenericArraySource>>(col, total_rows);
        return std::make_unique<GenericArraySource>(col);
    }
};

}

std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows)
{
    using Creator = typename ApplyTypeListForClass<ArraySourceCreator, TypeListNumbersAndUUID>::Type;
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&col.getData()))
    {
        auto column = ColumnArray::create(column_nullable->getNestedColumnPtr(), col.getOffsetsPtr());
        return Creator::create(*column, &column_nullable->getNullMapData(), is_const, total_rows);
    }
    return Creator::create(col, nullptr, is_const, total_rows);
}
}
