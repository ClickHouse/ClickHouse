#include <Columns/ColumnReplicated.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Sources.h>
#include <base/TypeLists.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
        using ColVecType = ColumnVectorOrDecimal<Type>;

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

template <typename... Types>
struct ReplicatedArraySourceCreator;

template <typename Type, typename... Types>
struct ReplicatedArraySourceCreator<Type, Types...>
{
    static std::unique_ptr<IArraySource> create(const ColumnArray & col, const NullMap * null_map, const ColumnIndex & replication_indexes)
    {
        using ColVecType = ColumnVectorOrDecimal<Type>;

        if (typeid_cast<const ColVecType *>(&col.getData()))
        {
            if (null_map)
                return std::make_unique<ReplicatedSource<NullableArraySource<NumericArraySource<Type>>>>(col, *null_map, replication_indexes);
            return std::make_unique<ReplicatedSource<NumericArraySource<Type>>>(col, replication_indexes);
        }

        return ReplicatedArraySourceCreator<Types...>::create(col, null_map, replication_indexes);
    }
};

template <>
struct ReplicatedArraySourceCreator<>
{
    static std::unique_ptr<IArraySource> create(const ColumnArray & col, const NullMap * null_map, const ColumnIndex & replication_indexes)
    {
        if (null_map)
            return std::make_unique<ReplicatedSource<NullableArraySource<GenericArraySource>>>(col, *null_map, replication_indexes);
        return std::make_unique<ReplicatedSource<GenericArraySource>>(col, replication_indexes);
    }
};

}

std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows)
{
    using Creator = TypeListChangeRoot<ArraySourceCreator, TypeListNumberWithUUID>;
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&col.getData()))
    {
        auto column = ColumnArray::create(column_nullable->getNestedColumnPtr(), col.getOffsetsPtr());
        return Creator::create(*column, &column_nullable->getNullMapData(), is_const, total_rows);
    }
    return Creator::create(col, nullptr, is_const, total_rows);
}

std::unique_ptr<IArraySource> createArraySourceFromReplicated(const ColumnReplicated & col)
{
    const auto * nested_array = typeid_cast<const ColumnArray *>(col.getNestedColumn().get());
    if (!nested_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "createArraySourceFromReplicated expects ColumnReplicated over ColumnArray, got {}",
            col.getNestedColumn()->getName());

    using Creator = TypeListChangeRoot<ReplicatedArraySourceCreator, TypeListNumberWithUUID>;
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&nested_array->getData()))
    {
        auto column = ColumnArray::create(column_nullable->getNestedColumnPtr(), nested_array->getOffsetsPtr());
        return Creator::create(*column, &column_nullable->getNullMapData(), col.getIndexes());
    }
    return Creator::create(*nested_array, nullptr, col.getIndexes());
}
}
