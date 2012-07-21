#pragma once

#include <vector>
#include <sstream>

#include <boost/variant.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/static_visitor.hpp>

#include <Poco/NumberFormatter.h>

#include <mysqlxx/mysqlxx.h>	/// mysqlxx::Date, mysqlxx::DateTime

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{

using Poco::SharedPtr;

class IAggregateFunction;
	
/** Типы данных для представления единичного значения произвольного типа в оперативке.
  * Внимание! Предпочтительно вместо единичных значений хранить кусочки столбцов. См. Column.h
  */
typedef boost::make_recursive_variant<
	Null,
	UInt64,
	Int64,
	Float64,
	String,
	SharedPtr<IAggregateFunction>,			/// Состояние агрегатной функции
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
		AggregateFunction,
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
	FieldType::Enum operator() (const SharedPtr<IAggregateFunction> & x) const { return FieldType::AggregateFunction; }
	FieldType::Enum operator() (const Array 	& x) const { return FieldType::Array; }
};

/** Возвращает строковый дамп типа */
class FieldVisitorDump : public boost::static_visitor<std::string>
{
public:
	String operator() (const Null 		& x) const { return "NULL"; }
	String operator() (const UInt64 	& x) const { return "UInt64_" + Poco::NumberFormatter::format(x); }
	String operator() (const Int64 		& x) const { return "Int64_" + Poco::NumberFormatter::format(x); }
	String operator() (const Float64 	& x) const { return "Float64_" + Poco::NumberFormatter::format(x); }
	String operator() (const SharedPtr<IAggregateFunction> & x) const { return "AggregateFunction"; }

	String operator() (const String 	& x) const
	{
		std::stringstream s;
		s << mysqlxx::quote << x;
		return s.str();
	}

	String operator() (const Array 	& x) const
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
	String operator() (const SharedPtr<IAggregateFunction> & x) const { return "AggregateFunction"; }

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

/** Числовой тип преобразует в указанный. */
template <typename T>
class FieldVisitorConvertToNumber : public boost::static_visitor<T>
{
public:
	T operator() (const Null & x) const
	{
		throw Exception("Cannot convert NULL to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
	}
	
	T operator() (const String & x) const
	{
		throw Exception("Cannot convert String to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
	}
	
	T operator() (const Array & x) const
	{
		throw Exception("Cannot convert Array to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
	}

	T operator() (const SharedPtr<IAggregateFunction> & x) const
	{
		throw Exception("Cannot convert AggregateFunctionPtr to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
	}

	T operator() (const UInt64 	& x) const { return x; }
	T operator() (const Int64 	& x) const { return x; }
	T operator() (const Float64 & x) const { return x; }
};


class FieldVisitorLess : public boost::static_visitor<bool>
{
public:
	template <typename T, typename U>
	bool operator() (const T &, const U &) const { return false; }

    template <typename T>
    bool operator() (const T & lhs, const T & rhs) const { return lhs < rhs; }
};

class FieldVisitorGreater : public boost::static_visitor<bool>
{
public:
	template <typename T, typename U>
	bool operator() (const T &, const U &) const { return false; }

    template <typename T>
    bool operator() (const T & lhs, const T & rhs) const { return lhs > rhs; }
};


template <typename T> struct NearestFieldType;

template <> struct NearestFieldType<UInt8> 		{ typedef UInt64 	Type; };
template <> struct NearestFieldType<UInt16> 	{ typedef UInt64 	Type; };
template <> struct NearestFieldType<UInt32> 	{ typedef UInt64 	Type; };
template <> struct NearestFieldType<UInt64> 	{ typedef UInt64 	Type; };
template <> struct NearestFieldType<Int8> 		{ typedef Int64 	Type; };
template <> struct NearestFieldType<Int16> 		{ typedef Int64 	Type; };
template <> struct NearestFieldType<Int32> 		{ typedef Int64 	Type; };
template <> struct NearestFieldType<Int64> 		{ typedef Int64 	Type; };
template <> struct NearestFieldType<Float32> 	{ typedef Float64 	Type; };
template <> struct NearestFieldType<Float64> 	{ typedef Float64 	Type; };
template <> struct NearestFieldType<String> 	{ typedef String 	Type; };
template <> struct NearestFieldType<bool> 		{ typedef UInt64 	Type; };

}
