#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeEnum.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Functions/DataTypeTraits.h>

#include <DB/Core/FieldVisitors.h>

#include <DB/Interpreters/convertFieldToType.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int TYPE_MISMATCH;
}


/** Проверка попадания Field from, имеющим тип From в диапазон значений типа To.
  * From и To - числовые типы. Могут быть типами с плавающей запятой.
  * From - это одно из UInt64, Int64, Float64,
  *  тогда как To может быть также 8, 16, 32 битным.
  *
  * Если попадает в диапазон, то from конвертируется в Field ближайшего к To типа.
  * Если не попадает - возвращается Field(Null).
  */

namespace
{

template <typename From, typename To>
static Field convertNumericTypeImpl(const Field & from)
{
	From value = from.get<From>();

	if (static_cast<long double>(value) != static_cast<long double>(To(value)))
		return {};

	return Field(typename NearestFieldType<To>::Type(value));
}

template <typename To>
static Field convertNumericType(const Field & from, const IDataType & type)
{
	if (from.getType() == Field::Types::UInt64)
		return convertNumericTypeImpl<UInt64, To>(from);
	if (from.getType() == Field::Types::Int64)
		return convertNumericTypeImpl<Int64, To>(from);
	if (from.getType() == Field::Types::Float64)
		return convertNumericTypeImpl<Float64, To>(from);

	throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
		+ Field::Types::toString(from.getType()), ErrorCodes::TYPE_MISMATCH);
}


Field convertFieldToTypeImpl(const Field & src, const IDataType & type)
{
	if (type.isNumeric())
	{
		if (typeid_cast<const DataTypeUInt8 *>(&type))		return convertNumericType<UInt8>(src, type);
		if (typeid_cast<const DataTypeUInt16 *>(&type))		return convertNumericType<UInt16>(src, type);
		if (typeid_cast<const DataTypeUInt32 *>(&type))		return convertNumericType<UInt32>(src, type);
		if (typeid_cast<const DataTypeUInt64 *>(&type))		return convertNumericType<UInt64>(src, type);
		if (typeid_cast<const DataTypeInt8 *>(&type))		return convertNumericType<Int8>(src, type);
		if (typeid_cast<const DataTypeInt16 *>(&type))		return convertNumericType<Int16>(src, type);
		if (typeid_cast<const DataTypeInt32 *>(&type))		return convertNumericType<Int32>(src, type);
		if (typeid_cast<const DataTypeInt64 *>(&type))		return convertNumericType<Int64>(src, type);
		if (typeid_cast<const DataTypeFloat32 *>(&type))	return convertNumericType<Float32>(src, type);
		if (typeid_cast<const DataTypeFloat64 *>(&type))	return convertNumericType<Float64>(src, type);

		const bool is_date = typeid_cast<const DataTypeDate *>(&type);
		bool is_datetime = false;
		bool is_enum = false;

		if (!is_date)
			if (!(is_datetime = typeid_cast<const DataTypeDateTime *>(&type)))
				if (!(is_enum = dynamic_cast<const IDataTypeEnum *>(&type)))
					throw Exception{"Logical error: unknown numeric type " + type.getName(), ErrorCodes::LOGICAL_ERROR};

		/// Numeric values for Enums should not be used directly in IN section
		if (src.getType() == Field::Types::UInt64 && !is_enum)
			return src;

		if (src.getType() == Field::Types::String)
		{
			if (is_date)
			{
				/// Convert 'YYYY-MM-DD' Strings to Date
				return UInt64(stringToDate(src.get<const String &>()));
			}
			else if (is_datetime)
			{
				/// Convert 'YYYY-MM-DD hh:mm:ss' Strings to DateTime
				return stringToDateTime(src.get<const String &>());
			}
			else if (is_enum)
			{
				/// Convert String to Enum's value
				return dynamic_cast<const IDataTypeEnum &>(type).castToValue(src);
			}
		}

		throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
			+ Field::Types::toString(src.getType()), ErrorCodes::TYPE_MISMATCH);
	}
	else if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(&type))
	{
		if (src.getType() != Field::Types::Array)
			throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
				+ Field::Types::toString(src.getType()), ErrorCodes::TYPE_MISMATCH);

		const IDataType & nested_type = *DataTypeTraits::removeNullable(type_array->getNestedType());

		const Array & src_arr = src.get<Array>();
		size_t src_arr_size = src_arr.size();

		Array res(src_arr_size);
		for (size_t i = 0; i < src_arr_size; ++i)
			res[i] = convertFieldToType(src_arr[i], nested_type);

		return res;
	}
	else
	{
		if (src.getType() == Field::Types::UInt64
			|| src.getType() == Field::Types::Int64
			|| src.getType() == Field::Types::Float64
			|| src.getType() == Field::Types::Array
			|| (src.getType() == Field::Types::String
				&& !typeid_cast<const DataTypeString *>(&type)
				&& !typeid_cast<const DataTypeFixedString *>(&type)))
			throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
				+ Field::Types::toString(src.getType()), ErrorCodes::TYPE_MISMATCH);
	}

	return src;
}

}

Field convertFieldToType(const Field & src, const IDataType & type)
{
	if (type.isNullable())
	{
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const DataTypePtr & nested_type = nullable_type.getNestedType();
		return convertFieldToTypeImpl(src, *nested_type);
	}
	else
		return convertFieldToTypeImpl(src, type);
}


}
