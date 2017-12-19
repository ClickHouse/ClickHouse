#include "GatherUtils.h"

namespace DB
{
/// Creates IArraySink from ColumnArray

template <typename... Types>
struct ArraySinkCreator;

template <typename Type, typename... Types>
struct ArraySinkCreator<Type, Types...>
{
    static std::unique_ptr<IArraySink> create(ColumnArray & col, ColumnUInt8 * null_map, size_t column_size)
    {
        if (typeid_cast<ColumnVector<Type> *>(&col.getData()))
        {
            if (null_map)
                return std::make_unique<NullableArraySink<NumericArraySink<Type>>>(col, *null_map, column_size);
            return std::make_unique<NumericArraySink<Type>>(col, column_size);
        }

        return ArraySinkCreator<Types...>::create(col, null_map, column_size);
    }
};

template <>
struct ArraySinkCreator<>
{
    static std::unique_ptr<IArraySink> create(ColumnArray & col, ColumnUInt8 * null_map, size_t column_size)
    {
        if (null_map)
            return std::make_unique<NullableArraySink<GenericArraySink>>(col, *null_map, column_size);
        return std::make_unique<GenericArraySink>(col, column_size);
    }
};

std::unique_ptr<IArraySink> createArraySink(ColumnArray & col, size_t column_size)
{
    using Creator = ApplyTypeListForClass<ArraySinkCreator, TypeListNumbers>::Type;
    if (auto column_nullable = typeid_cast<ColumnNullable *>(&col.getData()))
    {
        auto column = ColumnArray::create(column_nullable->getNestedColumnPtr(), col.getOffsetsPtr());
        return Creator::create(*column, &column_nullable->getNullMapColumn(), column_size);
    }
    return Creator::create(col, nullptr, column_size);
}
}
