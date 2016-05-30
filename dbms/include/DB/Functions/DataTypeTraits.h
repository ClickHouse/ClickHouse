#pragma once

#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/EnrichedDataTypePtr.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

namespace DataTypeTraits
{

template <typename T>
struct DataTypeFromFieldTypeOrError
{
	static DataTypePtr getDataType()
	{
		return std::make_shared<typename DataTypeFromFieldType<T>::Type>();
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

public:
	using Type = std::tuple<
		typename Type0::FieldType,
		typename Type1::FieldType
	>;
};

/// Convert an enriched numeric type into an enriched data type.
template <typename T>
struct ToEnrichedDataType
{
private:
	using Type0 = typename std::tuple_element<0, T>::type;
	using Type1 = typename std::tuple_element<1, T>::type;

public:
	using Type = std::tuple<
		typename DataTypeFromFieldType<Type0>::Type,
		typename DataTypeFromFieldType<Type1>::Type
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
		using DataType0 = typename DataTypeFromFieldType<Type0>::Type;

		using Type1 = typename std::tuple_element<1, TEnrichedType>::type;
		using DataType1 = typename DataTypeFromFieldType<Type1>::Type;

		return std::make_pair(std::make_shared<DataType0>(), std::make_shared<DataType1>());
	}
};

/// Convert an enriched data type into an enriched data type object.
template <typename TEnrichedType>
struct ToEnrichedDataTypeObject<TEnrichedType, false>
{
	static EnrichedDataTypePtr execute()
	{
		using DataType0 = typename std::tuple_element<0, TEnrichedType>::type;
		using DataType1 = typename std::tuple_element<1, TEnrichedType>::type;

		return std::make_pair(std::make_shared<DataType0>(), std::make_shared<DataType1>());
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

}

}
