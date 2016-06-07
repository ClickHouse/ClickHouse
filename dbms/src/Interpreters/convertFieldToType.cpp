#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeEnum.h>

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

	throw Exception("Type mismatch in IN or VALUES section: " + type.getName() + " expected, "
		+ Field::Types::toString(from.getType()) + " got", ErrorCodes::TYPE_MISMATCH);
}


Field convertFieldToType(const Field & src, const IDataType & type)
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
		bool is_enum8 = false;
		bool is_enum16 = false;

		if (!is_date)
			if (!(is_datetime = typeid_cast<const DataTypeDateTime *>(&type)))
				if (!(is_enum8 = typeid_cast<const DataTypeEnum8 *>(&type)))
					if (!(is_enum16 = typeid_cast<const DataTypeEnum16 *>(&type)))
						throw Exception{
							"Logical error: unknown numeric type " + type.getName(),
							ErrorCodes::LOGICAL_ERROR
						};

		const auto is_enum = is_enum8 || is_enum16;

		/// Numeric values for Enums should not be used directly in IN section
		if (src.getType() == Field::Types::UInt64 && !is_enum)
			return src;

		if (src.getType() == Field::Types::String)
		{
			/// Возможность сравнивать даты и даты-с-временем со строкой.
			const String & str = src.get<const String &>();
			ReadBufferFromString in(str);

			if (is_date)
			{
				DayNum_t date{};
				readDateText(date, in);
				if (!in.eof())
					throw Exception("String is too long for Date: " + str);

				return Field(UInt64(date));
			}
			else if (is_datetime)
			{
				time_t date_time{};
				readDateTimeText(date_time, in);
				if (!in.eof())
					throw Exception("String is too long for DateTime: " + str);

				return Field(UInt64(date_time));
			}
			else if (is_enum8)
				return Field(UInt64(static_cast<const DataTypeEnum8 &>(type).getValue(str)));
			else if (is_enum16)
				return Field(UInt64(static_cast<const DataTypeEnum16 &>(type).getValue(str)));
		}

		throw Exception("Type mismatch in IN or VALUES section: " + type.getName() + " expected, "
			+ Field::Types::toString(src.getType()) + " got", ErrorCodes::TYPE_MISMATCH);
	}
	else
	{
		if (src.getType() == Field::Types::UInt64
			|| src.getType() == Field::Types::Int64
			|| src.getType() == Field::Types::Float64
			|| src.getType() == Field::Types::Null
			|| (src.getType() == Field::Types::String
				&& !typeid_cast<const DataTypeString *>(&type)
				&& !typeid_cast<const DataTypeFixedString *>(&type))
			|| (src.getType() == Field::Types::Array
				&& !typeid_cast<const DataTypeArray *>(&type)))
			throw Exception("Type mismatch in IN or VALUES section: " + type.getName() + " expected, "
				+ Field::Types::toString(src.getType()) + " got", ErrorCodes::TYPE_MISMATCH);
	}

	return src;
}

}
