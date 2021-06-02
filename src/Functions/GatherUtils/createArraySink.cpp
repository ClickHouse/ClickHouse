#include "GatherUtils.h"
#include "Sinks.h"
#include "Sources.h"
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
/// Creates IArraySink from ColumnArray

namespace
{

template <typename... Types>
struct ArraySinkCreator;

template <typename Type, typename... Types>
struct ArraySinkCreator<Type, Types...>
{
    static std::unique_ptr<IArraySink> create(IColumn & values, ColumnArray::Offsets & offsets, size_t column_size)
    {
        using ColVecType = std::conditional_t<IsDecimalNumber<Type>, ColumnDecimal<Type>, ColumnVector<Type>>;

        IColumn * not_null_values = &values;
        bool is_nullable = false;

        if (auto * nullable = typeid_cast<ColumnNullable *>(&values))
        {
            not_null_values = &nullable->getNestedColumn();
            is_nullable = true;
        }

        if (typeid_cast<ColVecType *>(not_null_values))
        {
            if (is_nullable)
                return std::make_unique<NullableArraySink<NumericArraySink<Type>>>(values, offsets, column_size);
            return std::make_unique<NumericArraySink<Type>>(values, offsets, column_size);
        }

        return ArraySinkCreator<Types...>::create(values, offsets, column_size);
    }
};

template <>
struct ArraySinkCreator<>
{
    static std::unique_ptr<IArraySink> create(IColumn & values, ColumnArray::Offsets & offsets, size_t column_size)
    {
        if (typeid_cast<ColumnNullable *>(&values))
            return std::make_unique<NullableArraySink<GenericArraySink>>(values, offsets, column_size);
        return std::make_unique<GenericArraySink>(values, offsets, column_size);
    }
};

}

std::unique_ptr<IArraySink> createArraySink(ColumnArray & col, size_t column_size)
{
    using Creator = ApplyTypeListForClass<ArraySinkCreator, TypeListNumbersAndUUID>::Type;
    return Creator::create(col.getData(), col.getOffsets(), column_size);
}
}
