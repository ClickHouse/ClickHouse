#ifndef DBMS_CORE_FIELD_H
#define DBMS_CORE_FIELD_H

#include <vector>
#include <sstream>

#include <boost/variant.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/static_visitor.hpp>

#include <Poco/NumberFormatter.h>

#include <mysqlxx/mysqlxx.h>

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

/** Возвращает строковый дамп типа */
class FieldVisitorDump : public boost::static_visitor<std::string>
{
public:
	std::string operator() (const Null 		& x) const { return "NULL"; }
	std::string operator() (const UInt64 	& x) const { return "UInt64_" + Poco::NumberFormatter::format(x); }
	std::string operator() (const Int64 	& x) const { return "Int64_" + Poco::NumberFormatter::format(x); }
	std::string operator() (const Float64 	& x) const { return "Float64_" + Poco::NumberFormatter::format(x); }

	std::string operator() (const String 	& x) const
	{
		std::stringstream s;
		s << mysqlxx::quote << x;
		return s.str();
	}

	std::string operator() (const Array 	& x) const
	{
		std::stringstream s;

		s << "Array_[";
		for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
		{
			if (it != x.begin())
				s << ", ";
			s << boost::apply_visitor(FieldVisitorDump(), *it);
		}
		s << "]";
		
		return s.str();
	}
};

/** Выводит текстовое представление типа, как литерала в SQL запросе */
class FieldVisitorToString : public boost::static_visitor<String>
{
public:
	String operator() (const Null 		& x) const { return "NULL"; }
	String operator() (const UInt64 	& x) const { return Poco::NumberFormatter::format(x); }
	String operator() (const Int64 		& x) const { return Poco::NumberFormatter::format(x); }
	String operator() (const Float64 	& x) const { return Poco::NumberFormatter::format(x); }

	String operator() (const String 	& x) const
	{
		std::stringstream s;
		s << mysqlxx::quote << x;
		return s.str();
	}

	String operator() (const Array 		& x) const
	{
		std::stringstream s;
		FieldVisitorToString visitor;

		s << "[";
		for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
		{
			if (it != x.begin())
				s << ", ";
			s << boost::apply_visitor(FieldVisitorToString(), *it);
		}
		s << "]";

		return s.str();
	}
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
