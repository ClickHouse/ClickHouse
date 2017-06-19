#pragma once

#include <DataTypes/NumberTraits.h>
#include <DataTypes/EnrichedDataTypePtr.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNull.h>


namespace DB
{

namespace DataTypeTraits
{

/// If the input type is nullable, return its nested type.
/// Otherwise it is an identity mapping.
const DataTypePtr & removeNullable(const DataTypePtr & type);

template <typename T>
struct DataTypeFromFieldTypeOrError
{
    static DataTypePtr getDataType()
    {
        return std::make_shared<DataTypeNumber<T>>();
    }
};

template <typename T>
struct DataTypeFromFieldTypeOrError<Nullable<T>>
{
    static DataTypePtr getDataType()
    {
        auto nested_type = DataTypeFromFieldTypeOrError<T>::getDataType();
        if (nested_type != nullptr)
            return std::make_shared<DataTypeNullable>(nested_type);
        else
            return nullptr;
    }
};

/// Special case for the null type.
template <>
struct DataTypeFromFieldTypeOrError<Nullable<void>>
{
    static DataTypePtr getDataType()
    {
        return std::make_shared<DataTypeNull>();
    }
};

template <>
struct DataTypeFromFieldTypeOrError<NumberTraits::Error>
{
    static DataTypePtr getDataType()
    {
        return nullptr;
    }
};

/// Convert an enriched data type into an enriched numberic type.
template <typename T>
struct ToEnrichedNumericType
{
private:
    using Type0 = typename std::tuple_element<0, T>::type;
    using Type1 = typename std::tuple_element<1, T>::type;
    using Nullability = typename std::tuple_element<2, T>::type;

public:
    using Type = std::tuple<
        typename Type0::FieldType,
        typename Type1::FieldType,
        Nullability
    >;
};

/// Convert an enriched numeric type into an enriched data type.
template <typename T>
struct ToEnrichedDataType
{
private:
    using Type0 = typename std::tuple_element<0, T>::type;
    using Type1 = typename std::tuple_element<1, T>::type;
    using Nullability = typename std::tuple_element<2, T>::type;

public:
    using Type = std::tuple<
        DataTypeNumber<Type0>,
        DataTypeNumber<Type1>,
        Nullability
    >;
};

/// Convert an enriched numeric type into an enriched data type.
/// Error case.
template <>
struct ToEnrichedDataType<NumberTraits::Error>
{
    using Type = NumberTraits::Error;
};

template <typename TEnrichedType, bool isNumeric>
struct ToEnrichedDataTypeObject;

/// Convert an enriched numeric type into an enriched data type object.
template <typename TEnrichedType>
struct ToEnrichedDataTypeObject<TEnrichedType, true>
{
    static EnrichedDataTypePtr execute()
    {
        using Type0 = typename std::tuple_element<0, TEnrichedType>::type;
        using Type1 = typename std::tuple_element<1, TEnrichedType>::type;
        using Nullability = typename std::tuple_element<2, TEnrichedType>::type;

        auto obj_type0 = DataTypeFromFieldTypeOrError<
            typename NumberTraits::AddNullability<Type0, Nullability>::Type
        >::getDataType();

        auto obj_type1 = DataTypeFromFieldTypeOrError<
            typename NumberTraits::AddNullability<Type1, Nullability>::Type
        >::getDataType();

        return std::make_pair(obj_type0, obj_type1);
    }
};

namespace
{

template <typename T, typename Nullability>
struct CreateDataTypeObject;

template <typename T>
struct CreateDataTypeObject<T, NumberTraits::HasNull>
{
    static DataTypePtr execute()
    {
        return std::make_shared<DataTypeNullable>(std::make_shared<T>());
    }
};

/// Special case for the null type.
template <>
struct CreateDataTypeObject<DataTypeVoid, NumberTraits::HasNull>
{
    static DataTypePtr execute()
    {
        return std::make_shared<DataTypeNull>();
    }
};

template <typename T>
struct CreateDataTypeObject<T, NumberTraits::HasNoNull>
{
    static DataTypePtr execute()
    {
        return std::make_shared<T>();
    }
};

}

/// Convert an enriched data type into an enriched data type object.
template <typename TEnrichedType>
struct ToEnrichedDataTypeObject<TEnrichedType, false>
{
    static EnrichedDataTypePtr execute()
    {
        using DataType0 = typename std::tuple_element<0, TEnrichedType>::type;
        using DataType1 = typename std::tuple_element<1, TEnrichedType>::type;
        using Nullability = typename std::tuple_element<2, TEnrichedType>::type;

        auto obj_type0 = CreateDataTypeObject<DataType0, Nullability>::execute();
        auto obj_type1 = CreateDataTypeObject<DataType1, Nullability>::execute();

        return std::make_pair(obj_type0, obj_type1);
    }
};

/// Convert an enriched numeric type into an enriched data type object.
/// Error case.
template <>
struct ToEnrichedDataTypeObject<NumberTraits::Error, true>
{
    static EnrichedDataTypePtr execute()
    {
        return std::make_pair(nullptr, nullptr);
    }
};

/// Convert an enriched data type into an enriched data type object.
/// Error case.
template <>
struct ToEnrichedDataTypeObject<NumberTraits::Error, false>
{
    static EnrichedDataTypePtr execute()
    {
        return std::make_pair(nullptr, nullptr);
    }
};

/// Compute the product of an enriched data type with an ordinary data type.
template <typename T1, typename T2>
struct DataTypeProduct
{
    using Type = typename ToEnrichedDataType<
        typename NumberTraits::TypeProduct<
            typename ToEnrichedNumericType<T1>::Type,
            typename NumberTraits::EmbedType<typename T2::FieldType>::Type
        >::Type
    >::Type;
};

template <typename T1, typename T2>
struct DataTypeProduct<T1, Nullable<T2> >
{
    using Type = typename ToEnrichedDataType<
        typename NumberTraits::TypeProduct<
            typename ToEnrichedNumericType<T1>::Type,
            typename NumberTraits::EmbedType<Nullable<typename T2::FieldType> >::Type
        >::Type
    >::Type;
};

}

}
