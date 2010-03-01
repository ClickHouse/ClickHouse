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


/// почему-то у boost::variant определены операторы < и ==, но не остальные операторы сравнения
inline bool operator!= (const Field & lhs, const Field & rhs) { return !(lhs == rhs); }
inline bool operator<= (const Field & lhs, const Field & rhs) { return lhs < rhs || lhs == rhs; }
inline bool operator>= (const Field & lhs, const Field & rhs) { return !(lhs < rhs); }
inline bool operator> (const Field & lhs, const Field & rhs) { return !(lhs < rhs) && !(lhs == rhs); }


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


}

#endif
