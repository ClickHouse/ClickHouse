#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Sources.h>
#include <Core/TypeListNumber.h>

namespace DB::GatherUtils
{
/// Creates IArraySink from ColumnArray

template <typename... Types>
struct ArraySinkCreator;

template <typename Type, typename... Types>
struct ArraySinkCreator<Type, Types...>
{
    static std::unique_ptr<IArraySink> create(ColumnArray & col, NullMap * null_map, size_t column_size)
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
    static std::unique_ptr<IArraySink> create(ColumnArray & col, NullMap * null_map, size_t column_size)
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
        auto column = ColumnArray::create(column_nullable->getNestedColumnPtr()->assumeMutable(),
                                          col.getOffsetsPtr()->assumeMutable());
        return Creator::create(*column, &column_nullable->getNullMapData(), column_size);
    }
    return Creator::create(col, nullptr, column_size);
}
}
