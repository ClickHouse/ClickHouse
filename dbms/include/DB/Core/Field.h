#ifndef DBMS_CORE_FIELD_H
#define DBMS_CORE_FIELD_H

#include <vector>

#include <boost/variant.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/static_visitor.hpp>

#include <DB/Core/Types.h>


namespace DB
{

/** Типы данных для представления единичного значения произвольного типа в оперативке.
  * Внимание! Предпочтительно вместо единичных значений хранить кусочки столбцов. См. Column.h
  */

typedef boost::make_recursive_variant<
	Null,
	UInt64,
	Int64,
	Float64,
	String,
	std::vector<boost::recursive_variant_>	/// Array, Tuple
	>::type Field;

typedef std::vector<Field> Array;			/// Значение типа "массив"


/** Числовое значение конкретного типа Field */
namespace FieldType
{
	enum Enum
	{
		Null = 0,
		UInt64,
		Int64,
		Float64,
		String,
		Array
	};
}


/** Возвращает true, если вариант - Null */
class FieldVisitorIsNull : public boost::static_visitor<bool>
{
public:
	template <typename T> bool operator() (const T & x) const { return false; }
	bool operator() (const Null & x) const { return true; }
};

/** Возвращает числовое значение типа */
class FieldVisitorGetType : public boost::static_visitor<FieldType::Enum>
{
public:
	FieldType::Enum operator() (const Null 		& x) const { return FieldType::Null; }
	FieldType::Enum operator() (const UInt64 	& x) const { return FieldType::UInt64; }
	FieldType::Enum operator() (const Int64 	& x) const { return FieldType::Int64; }
	FieldType::Enum operator() (const Float64 	& x) const { return FieldType::Float64; }
	FieldType::Enum operator() (const String 	& x) const { return FieldType::String; }
	FieldType::Enum operator() (const Array 	& x) const { return FieldType::Array; }
};


template <typename T> struct NearestFieldType;

template <> struct NearestFieldType<UInt8> { typedef UInt64 Type; };
template <> struct NearestFieldType<UInt16> { typedef UInt64 Type; };
template <> struct NearestFieldType<UInt32> { typedef UInt64 Type; };
template <> struct NearestFieldType<UInt64> { typedef UInt64 Type; };

template <> struct NearestFieldType<Int8> { typedef Int64 Type; };
template <> struct NearestFieldType<Int16> { typedef Int64 Type; };
template <> struct NearestFieldType<Int32> { typedef Int64 Type; };
template <> struct NearestFieldType<Int64> { typedef Int64 Type; };

template <> struct NearestFieldType<Float32> { typedef Float64 Type; };
template <> struct NearestFieldType<Float64> { typedef Float64 Type; };


}

#endif
