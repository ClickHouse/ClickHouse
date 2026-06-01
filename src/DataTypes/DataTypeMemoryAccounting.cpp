#include <DataTypes/DataTypeMemoryAccounting.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace
{

template <typename>
struct IsStdUnorderedMap : std::false_type
{
};

template <typename... Args>
struct IsStdUnorderedMap<std::unordered_map<Args...>> : std::true_type
{
};

template <typename>
struct IsStdUnorderedSet : std::false_type
{
};

template <typename... Args>
struct IsStdUnorderedSet<std::unordered_set<Args...>> : std::true_type
{
};

template <typename EnumType>
size_t getEnumTypeBytesAllocated(const EnumType & type)
{
    /// Count the concrete `DataTypeEnum` object, the `EnumValues` vector backing storage,
    /// and enum label string buffers. Private lookup indexes inside `EnumValues` are not exposed.
    size_t res = sizeof(EnumType);
    const auto & values = type.getValues();
    res += values.capacity() * sizeof(typename EnumType::Values::value_type);
    for (const auto & [name, _] : values)
        res += name.capacity();
    return res;
}

template <typename Map>
size_t getUnorderedStringMapBytesAllocated(const Map & map)
{
    static_assert(IsStdUnorderedMap<std::decay_t<Map>>::value);
    /// `std::unordered_map` buckets are node-pointer slots, so use `sizeof(void *)`
    /// per bucket. Each node also has implementation bookkeeping such as a next
    /// pointer; estimate that as one more `sizeof(void *)` per entry.
    size_t res = map.bucket_count() * sizeof(void *);
    res += map.size() * (sizeof(typename Map::value_type) + sizeof(void *));
    for (const auto & [key, _] : map)
        res += key.capacity();
    return res;
}

template <typename Set>
size_t getUnorderedStringSetBytesAllocated(const Set & set)
{
    static_assert(IsStdUnorderedSet<std::decay_t<Set>>::value);
    /// `std::unordered_set` buckets are node-pointer slots, so use `sizeof(void *)`
    /// per bucket. Each node also has implementation bookkeeping such as a next
    /// pointer; estimate that as one more `sizeof(void *)` per entry.
    size_t res = set.bucket_count() * sizeof(void *);
    res += set.size() * (sizeof(typename Set::value_type) + sizeof(void *));
    for (const auto & key : set)
        res += key.capacity();
    return res;
}

size_t getDataTypeMapSyntheticNestedBytesAllocated(const DataTypeMap & map)
{
    /// `DataTypeMap` stores a synthetic `Array(Tuple(keys, values))` type. Count only the
    /// retained wrapper objects and their immediate storage here; key/value types are
    /// counted through normal child traversal.
    size_t res = 0;
    const auto & nested = map.getNestedType();

    const auto * array = typeid_cast<const DataTypeArray *>(nested.get());
    chassert(array);
    if (!array)
        return res;

    res += getDataTypeShallowBytesAllocated(*array);

    const auto * tuple = typeid_cast<const DataTypeTuple *>(array->getNestedType().get());
    chassert(tuple);
    if (!tuple)
        return res;

    res += getDataTypeShallowBytesAllocated(*tuple);
    return res;
}

void addSpecialChildTypeBytes(const IDataType & type, size_t & total)
{
    /// Add full child `DataType` graphs for types whose retained child pointers are not
    /// currently visited by `IDataType::forEachChild`.
    const WhichDataType which(type);

    if (which.isAggregateFunction())
    {
        const auto & aggregate = assert_cast<const DataTypeAggregateFunction &>(type);
        /// PRECONDITION: `DataTypeAggregateFunction::forEachChild` is currently the base no-op.
        /// If it starts visiting `argument_types`, this branch must be removed or adjusted.
        for (const auto & argument_type : aggregate.getArgumentTypes())
            total += getDataTypeBytesAllocated(*argument_type);
        return;
    }

    if (which.isFunction())
    {
        const auto & function = assert_cast<const DataTypeFunction &>(type);
        /// Keep this with the aggregate special case unless `DataTypeFunction::forEachChild`
        /// starts visiting arguments and return type.
        for (const auto & argument_type : function.getArgumentTypes())
            total += getDataTypeBytesAllocated(*argument_type);
        if (function.getReturnType())
            total += getDataTypeBytesAllocated(*function.getReturnType());
        return;
    }

    if (which.isQBit())
    {
        const auto & qbit = assert_cast<const DataTypeQBit &>(type);
        /// Keep this special case unless `DataTypeQBit::forEachChild` starts visiting `element_type`.
        total += getDataTypeBytesAllocated(*qbit.getElementType());
        return;
    }
}

}

size_t getDataTypeShallowBytesAllocated(const IDataType & type)
{
    /// Count the concrete `IDataType` object plus immediate owned container/string backing
    /// storage. Child `DataType` graphs are added separately by `getDataTypeBytesAllocated`.
    const WhichDataType which(type);

    if (which.isEnum8())
        return getEnumTypeBytesAllocated(assert_cast<const DataTypeEnum8 &>(type));
    if (which.isEnum16())
        return getEnumTypeBytesAllocated(assert_cast<const DataTypeEnum16 &>(type));

    if (which.isTuple())
    {
        const auto & tuple = assert_cast<const DataTypeTuple &>(type);
        /// Count tuple element pointer vector storage and tuple element-name strings.
        /// Element type graphs are counted by child traversal.
        size_t res = sizeof(DataTypeTuple);
        res += tuple.getElements().capacity() * sizeof(DataTypePtr);
        res += tuple.getElementNames().capacity() * sizeof(String);
        for (const auto & name : tuple.getElementNames())
            res += name.capacity();
        return res;
    }

    if (which.isVariant())
    {
        const auto & variant = assert_cast<const DataTypeVariant &>(type);
        /// Count the variants pointer vector storage; variant type graphs are counted by child traversal.
        return sizeof(DataTypeVariant) + variant.getVariants().capacity() * sizeof(DataTypePtr);
    }

    if (which.isObject())
    {
        const auto & object = assert_cast<const DataTypeObject &>(type);
        /// Count retained typed-path and skip-path lookup storage plus regexp string buffers.
        size_t res = sizeof(DataTypeObject);
        res += getUnorderedStringMapBytesAllocated(object.getTypedPaths());
        res += getUnorderedStringSetBytesAllocated(object.getPathsToSkip());
        res += object.getPathRegexpsToSkip().capacity() * sizeof(String);
        for (const auto & regexp : object.getPathRegexpsToSkip())
            res += regexp.capacity();
        return res;
    }

    if (which.isMap())
        return sizeof(DataTypeMap) + getDataTypeMapSyntheticNestedBytesAllocated(assert_cast<const DataTypeMap &>(type));

    if (which.isAggregateFunction())
    {
        const auto & aggregate = assert_cast<const DataTypeAggregateFunction &>(type);
        /// Count argument pointer vector storage and shallow parameter `Field` array storage.
        /// Argument type graphs are counted by `addSpecialChildTypeBytes`.
        return sizeof(DataTypeAggregateFunction)
            + aggregate.getArgumentTypes().capacity() * sizeof(DataTypePtr)
            + aggregate.getParametersRef().capacity() * sizeof(Field);
    }

    if (which.isFunction())
    {
        const auto & function = assert_cast<const DataTypeFunction &>(type);
        /// Count function argument pointer vector storage. Argument and return type graphs are
        /// counted by `addSpecialChildTypeBytes`.
        return sizeof(DataTypeFunction) + function.getArgumentTypes().capacity() * sizeof(DataTypePtr);
    }

    if (which.isQBit())
        return sizeof(DataTypeQBit);
    if (which.isArray())
        return sizeof(DataTypeArray);
    if (which.isNullable())
        return sizeof(DataTypeNullable);
    if (which.isLowCardinality())
        return sizeof(DataTypeLowCardinality);
    if (which.isDynamic())
        return sizeof(DataTypeDynamic);
    if (which.isFixedString())
        return sizeof(DataTypeFixedString);
    if (which.isString())
        return sizeof(DataTypeString);
    if (which.isUUID())
        return sizeof(DataTypeUUID);
    if (which.isIPv4())
        return sizeof(DataTypeIPv4);
    if (which.isIPv6())
        return sizeof(DataTypeIPv6);
    if (which.isDate())
        return sizeof(DataTypeDate);
    if (which.isDate32())
        return sizeof(DataTypeDate32);
    if (which.isDateTime())
        return sizeof(DataTypeDateTime);
    if (which.isDateTime64())
        return sizeof(DataTypeDateTime64);
    if (which.isTime())
        return sizeof(DataTypeTime);
    if (which.isTime64())
        return sizeof(DataTypeTime64);
    if (which.isNothing())
        return sizeof(DataTypeNothing);
    if (which.isInterval())
        return sizeof(DataTypeInterval);

    if (which.isUInt8())
        return sizeof(DataTypeUInt8);
    if (which.isUInt16())
        return sizeof(DataTypeUInt16);
    if (which.isUInt32())
        return sizeof(DataTypeUInt32);
    if (which.isUInt64())
        return sizeof(DataTypeUInt64);
    if (which.isUInt128())
        return sizeof(DataTypeUInt128);
    if (which.isUInt256())
        return sizeof(DataTypeUInt256);
    if (which.isInt8())
        return sizeof(DataTypeInt8);
    if (which.isInt16())
        return sizeof(DataTypeInt16);
    if (which.isInt32())
        return sizeof(DataTypeInt32);
    if (which.isInt64())
        return sizeof(DataTypeInt64);
    if (which.isInt128())
        return sizeof(DataTypeInt128);
    if (which.isInt256())
        return sizeof(DataTypeInt256);
    if (which.isBFloat16())
        return sizeof(DataTypeBFloat16);
    if (which.isFloat32())
        return sizeof(DataTypeFloat32);
    if (which.isFloat64())
        return sizeof(DataTypeFloat64);

    if (which.isDecimal32())
        return sizeof(DataTypeDecimal32);
    if (which.isDecimal64())
        return sizeof(DataTypeDecimal64);
    if (which.isDecimal128())
        return sizeof(DataTypeDecimal128);
    if (which.isDecimal256())
        return sizeof(DataTypeDecimal256);

    chassert(false && "Unhandled IDataType subclass in memory accounting");
    return sizeof(IDataType);
}

size_t getDataTypeBytesAllocated(const IDataType & type)
{
    /// `DataType` graphs are treated as acyclic. `forEachChild` implementations already
    /// recurse, so the callback adds shallow bytes only; hidden child pointers are handled
    /// by `addSpecialChildTypeBytes`.
    size_t total = getDataTypeShallowBytesAllocated(type);
    addSpecialChildTypeBytes(type, total);

    type.forEachChild([&](const IDataType & child)
    {
        total += getDataTypeShallowBytesAllocated(child);
        addSpecialChildTypeBytes(child, total);
    });

    return total;
}

}
