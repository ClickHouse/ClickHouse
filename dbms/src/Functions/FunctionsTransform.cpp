#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsTransform.h>


namespace DB
{

/// TODO: Убрать copy-paste из FunctionsConditional.h
template <typename T>
struct DataTypeFromFieldTypeOrError
{
	static DataTypePtr getDataType()
	{
		return new typename DataTypeFromFieldType<T>::Type;
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

template <typename T1, typename T2>
DataTypePtr getSmallestCommonNumericTypeImpl()
{
	using ResultType = typename NumberTraits::ResultOfIf<T1, T2>::Type;
	auto type_res = DataTypeFromFieldTypeOrError<ResultType>::getDataType();
	if (!type_res)
		throw Exception("Types " + TypeName<T1>::get() + " and " + TypeName<T2>::get()
			+ " are not upscalable to a common type without loss of precision", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return type_res;
}

template <typename T1>
DataTypePtr getSmallestCommonNumericTypeLeft(const IDataType & t2)
{
	if (typeid_cast<const DataTypeUInt8 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt8>();
	if (typeid_cast<const DataTypeUInt16 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt16>();
	if (typeid_cast<const DataTypeUInt32 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt32>();
	if (typeid_cast<const DataTypeUInt64 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, UInt64>();
	if (typeid_cast<const DataTypeInt8 *>(&t2))		return getSmallestCommonNumericTypeImpl<T1, Int8>();
	if (typeid_cast<const DataTypeInt16 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Int16>();
	if (typeid_cast<const DataTypeInt32 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Int32>();
	if (typeid_cast<const DataTypeInt64 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Int64>();
	if (typeid_cast<const DataTypeFloat32 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Float32>();
	if (typeid_cast<const DataTypeFloat64 *>(&t2))	return getSmallestCommonNumericTypeImpl<T1, Float64>();

	throw Exception("Logical error: not a numeric type passed to function getSmallestCommonNumericType", ErrorCodes::LOGICAL_ERROR);
}

DataTypePtr getSmallestCommonNumericType(const IDataType & t1, const IDataType & t2)
{
	if (typeid_cast<const DataTypeUInt8 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt8>(t2);
	if (typeid_cast<const DataTypeUInt16 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt16>(t2);
	if (typeid_cast<const DataTypeUInt32 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt32>(t2);
	if (typeid_cast<const DataTypeUInt64 *>(&t1))	return getSmallestCommonNumericTypeLeft<UInt64>(t2);
	if (typeid_cast<const DataTypeInt8 *>(&t1))		return getSmallestCommonNumericTypeLeft<Int8>(t2);
	if (typeid_cast<const DataTypeInt16 *>(&t1))	return getSmallestCommonNumericTypeLeft<Int16>(t2);
	if (typeid_cast<const DataTypeInt32 *>(&t1))	return getSmallestCommonNumericTypeLeft<Int32>(t2);
	if (typeid_cast<const DataTypeInt64 *>(&t1))	return getSmallestCommonNumericTypeLeft<Int64>(t2);
	if (typeid_cast<const DataTypeFloat32 *>(&t1))	return getSmallestCommonNumericTypeLeft<Float32>(t2);
	if (typeid_cast<const DataTypeFloat64 *>(&t1))	return getSmallestCommonNumericTypeLeft<Float64>(t2);

	throw Exception("Logical error: not a numeric type passed to function getSmallestCommonNumericType", ErrorCodes::LOGICAL_ERROR);
}

}


namespace DB
{

void registerFunctionsTransform(FunctionFactory & factory)
{
	factory.registerFunction<FunctionTransform>();
}

}
