#pragma once

#include <vector>
#include <sstream>
#include <tr1/type_traits>

#include <boost/static_assert.hpp>

#include <Poco/NumberFormatter.h>

#include <mysqlxx/Date.h>
#include <mysqlxx/DateTime.h>
#include <mysqlxx/Manip.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{

using Poco::SharedPtr;

class IAggregateFunction;


/** Discriminated union из нескольких типов.
  * Сделан для замены boost::variant:
  *  является не обобщённым,
  *  зато несколько более эффективным, и более простым.
  *
  * Используется для представления единичного значения одного из нескольких типов в оперативке.
  * Внимание! Предпочтительно вместо единичных значений хранить кусочки столбцов. См. Column.h
  */
class Field
{
public:
	struct Types
	{
		/// Идентификатор типа.
		enum Which
		{
			Null 				= 0,
			UInt64				= 1,
			Int64				= 2,
			Float64				= 3,
			/// не POD типы
			String				= 16,
			AggregateFunction 	= 17,	/// Состояние агрегатной функции
			Array				= 18,
		};

		static const char * toString(Which which)
		{
			switch (which)
			{
				case Null: 				return "Null";
				case UInt64: 			return "UInt64";
				case Int64: 			return "Int64";
				case Float64: 			return "Float64";
				case String: 			return "String";
				case AggregateFunction: return "AggregateFunctionPtr";
				case Array: 			return "Array";

				default:
					throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
			}
		}
	};

	/// Позволяет получить идентификатор для типа или наоборот.
	template <typename T> struct TypeToEnum;
	template <Types::Which which> struct EnumToType;


	Field()
		: which(Types::Null)
	{
//		std::cerr << "Field()" << std::endl;
	}

	/** Не смотря на наличие шаблонного конструктора, этот конструктор всё-равно нужен,
	  *  так как при его отсутствии, не смотря на наличие шаблонного конструктора,
	  *  компилятор всё-равно сгенерирует конструктор по-умолчанию.
	  */
	Field(const Field & rhs)
	{
//		std::cerr << this << " Field::Field(const Field &)" << std::endl;
		create(rhs);
	}

	Field & operator= (const Field & rhs)
	{
//		std::cerr << this << " Field::operator=(const Field &)" << std::endl;
		destroy();
		create(rhs);
		return *this;
	}

	template <typename T>
	Field(const T & rhs)
		: which(Types::Null)
	{
//		std::cerr << this << " Field::Field(" << Types::toString(TypeToEnum<T>::value) << ")" << std::endl;
		create(rhs);
	}

	template <typename T>
	Field & operator= (const T & rhs)
	{
//		std::cerr << this << " Field::operator=(" << Types::toString(TypeToEnum<T>::value) << ")" << std::endl;
		destroy();
		create(rhs);
		return *this;
	}

	~Field()
	{
//		std::cerr << this << " Field::~Field()" << std::endl;
		destroy();
	}


	Types::Which getType() const { return which; }
	const char * getTypeName() const { return Types::toString(which); }


	template <typename T> T & get()
	{
		typedef typename std::tr1::remove_reference<T>::type TWithoutRef;
		TWithoutRef * __attribute__((__may_alias__)) ptr = reinterpret_cast<TWithoutRef*>(storage);
		return *ptr;
	};
	
	template <typename T> const T & get() const
	{
		typedef typename std::tr1::remove_reference<T>::type TWithoutRef;
		const TWithoutRef * __attribute__((__may_alias__)) ptr = reinterpret_cast<const TWithoutRef*>(storage);
		return *ptr;
	};

	template <typename T> T & safeGet()
	{
		const Types::Which requested = TypeToEnum<typename std::tr1::remove_cv<typename std::tr1::remove_reference<T>::type>::type>::value;
		if (which != requested)
			throw Exception("Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)), ErrorCodes::BAD_GET);
		return get<T>();
	}

	template <typename T> const T & safeGet() const
	{
		const Types::Which requested = TypeToEnum<typename std::tr1::remove_cv<typename std::tr1::remove_reference<T>::type>::type>::value;
		if (which != requested)
			throw Exception("Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)), ErrorCodes::BAD_GET);
		return get<T>();
	}


	bool operator< (const Field & rhs) const
	{
		if (which < rhs.which)
			return true;
		if (which > rhs.which)
			return false;
		
		switch (which)
		{
			case Types::Null: 				return get<Null>() 					< rhs.get<Null>();
			case Types::UInt64: 			return get<UInt64>() 				< rhs.get<UInt64>();
			case Types::Int64: 				return get<Int64>() 				< rhs.get<Int64>();
			case Types::Float64: 			return get<Float64>() 				< rhs.get<Float64>();
			case Types::String: 			return get<String>() 				< rhs.get<String>();
			case Types::AggregateFunction: 	return get<AggregateFunctionPtr>() 	< rhs.get<AggregateFunctionPtr>();
			case Types::Array: 				return get<Array>() 				< rhs.get<Array>();

			default:
				throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
		}
	}

	bool operator> (const Field & rhs) const
	{
		return rhs < *this;
	}

	bool operator<= (const Field & rhs) const
	{
		if (which < rhs.which)
			return true;
		if (which > rhs.which)
			return false;

		switch (which)
		{
			case Types::Null: 				return get<Null>() 					<= rhs.get<Null>();
			case Types::UInt64: 			return get<UInt64>() 				<= rhs.get<UInt64>();
			case Types::Int64: 				return get<Int64>() 				<= rhs.get<Int64>();
			case Types::Float64: 			return get<Float64>() 				<= rhs.get<Float64>();
			case Types::String: 			return get<String>() 				<= rhs.get<String>();
			case Types::AggregateFunction: 	return get<AggregateFunctionPtr>() 	<= rhs.get<AggregateFunctionPtr>();
			case Types::Array: 				return get<Array>() 				<= rhs.get<Array>();

			default:
				throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
		}
	}

	bool operator>= (const Field & rhs) const
	{
		return rhs <= *this;
	}

	bool operator== (const Field & rhs) const
	{
		if (which != rhs.which)
			return false;

		switch (which)
		{
			case Types::Null: 				return get<Null>() 					== rhs.get<Null>();
			case Types::UInt64: 			return get<UInt64>() 				== rhs.get<UInt64>();
			case Types::Int64: 				return get<Int64>() 				== rhs.get<Int64>();
			case Types::Float64: 			return get<Float64>() 				== rhs.get<Float64>();
			case Types::String: 			return get<String>() 				== rhs.get<String>();
			case Types::AggregateFunction: 	return get<AggregateFunctionPtr>() 	== rhs.get<AggregateFunctionPtr>();
			case Types::Array: 				return get<Array>() 				== rhs.get<Array>();

			default:
				throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
		}
	}

	bool operator!= (const Field & rhs) const
	{
		return !(*this == rhs);
	}


	typedef SharedPtr<IAggregateFunction> AggregateFunctionPtr;
	typedef std::vector<Field> Array;
	
private:
	static const size_t storage_size = 24;
	
	BOOST_STATIC_ASSERT(storage_size >= sizeof(Null));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(UInt64));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(Int64));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(Float64));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(String));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(AggregateFunctionPtr));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(Array));

	char storage[storage_size];
	Types::Which which;


	template <typename T>
	void create(const T & x)
	{
		which = TypeToEnum<T>::value;
//		std::cerr << this << " Creating " << getTypeName() << std::endl;
		T * __attribute__((__may_alias__)) ptr = reinterpret_cast<T*>(storage);
		new (ptr) T(x);
	}

	void create(const Null & x)
	{
		which = Types::Null;
//		std::cerr << this << " Creating " << getTypeName() << std::endl;
	}

	void create(const Field & x)
	{
//		std::cerr << this << " Creating Field" << std::endl;

		switch (x.which)
		{
			case Types::Null: 				create(Null());							break;
			case Types::UInt64: 			create(x.get<UInt64>());				break;
			case Types::Int64: 				create(x.get<Int64>());					break;
			case Types::Float64: 			create(x.get<Float64>());				break;
			case Types::String: 			create(x.get<String>());				break;
			case Types::AggregateFunction: 	create(x.get<AggregateFunctionPtr>());	break;
			case Types::Array: 				create(x.get<Array>());					break;
		}
	}


	void destroy()
	{
//		std::cerr << this << " Destroying " << getTypeName() << std::endl;
		
		switch (which)
		{
			default:
				break;
				
			case Types::String:
				destroy<String>();
				break;
			case Types::AggregateFunction:
				destroy<AggregateFunctionPtr>();
				break;
			case Types::Array:
				destroy<Array>();
				break;
		}
	}

	template <typename T>
	void destroy()
	{
		T * __attribute__((__may_alias__)) ptr = reinterpret_cast<T*>(storage);
		ptr->~T();
	}
};


template <> struct Field::TypeToEnum<Null> 							{ static const Types::Which value = Types::Null; };
template <> struct Field::TypeToEnum<UInt64> 						{ static const Types::Which value = Types::UInt64; };
template <> struct Field::TypeToEnum<Int64> 						{ static const Types::Which value = Types::Int64; };
template <> struct Field::TypeToEnum<Float64> 						{ static const Types::Which value = Types::Float64; };
template <> struct Field::TypeToEnum<String> 						{ static const Types::Which value = Types::String; };
template <> struct Field::TypeToEnum<Field::AggregateFunctionPtr> 	{ static const Types::Which value = Types::AggregateFunction; };
template <> struct Field::TypeToEnum<Field::Array> 					{ static const Types::Which value = Types::Array; };

template <> struct Field::EnumToType<Field::Types::Null> 				{ typedef Null 					Type; };
template <> struct Field::EnumToType<Field::Types::UInt64> 				{ typedef UInt64 				Type; };
template <> struct Field::EnumToType<Field::Types::Int64> 				{ typedef Int64 				Type; };
template <> struct Field::EnumToType<Field::Types::Float64> 			{ typedef Float64 				Type; };
template <> struct Field::EnumToType<Field::Types::String> 				{ typedef String 				Type; };
template <> struct Field::EnumToType<Field::Types::AggregateFunction> 	{ typedef AggregateFunctionPtr 	Type; };
template <> struct Field::EnumToType<Field::Types::Array> 				{ typedef Array 				Type; };


template <typename T>
T get(const Field & field)
{
	return field.template get<T>();
}

template <typename T>
T get(Field & field)
{
	return field.template get<T>();
}

template <typename T>
T safeGet(const Field & field)
{
	return field.template safeGet<T>();
}

template <typename T>
T safeGet(Field & field)
{
	return field.template safeGet<T>();
}


/** StaticVisitor (его наследники) - класс с перегруженными для разных типов операторами ().
  * Вызвать visitor для field можно с помощью функции apply_visitor.
  * Также поддерживается visitor, в котором оператор () принимает два аргумента.
  */
template <typename R = void>
struct StaticVisitor
{
	typedef R ResultType;
};


template <typename Visitor, typename F>
typename Visitor::ResultType apply_visitor_impl(Visitor & visitor, F & field)
{
	switch (field.getType())
	{
		case Field::Types::Null: 				return visitor(field.get<Null>());
		case Field::Types::UInt64: 				return visitor(field.get<UInt64>());
		case Field::Types::Int64: 				return visitor(field.get<Int64>());
		case Field::Types::Float64: 			return visitor(field.get<Float64>());
		case Field::Types::String: 				return visitor(field.get<String>());
		case Field::Types::AggregateFunction: 	return visitor(field.get<Field::AggregateFunctionPtr>());
		case Field::Types::Array: 				return visitor(field.get<Field::Array>());

		default:
			throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
	}
}

/** Эти штуки нужны, чтобы принимать временный объект по константной ссылке.
  * В шаблон выше, типы форвардятся уже с const-ом.
  */

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, Field & field)
{
	return apply_visitor_impl(visitor, field);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, const Field & field)
{
	return apply_visitor_impl(visitor, field);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, Field & field)
{
	return apply_visitor_impl(visitor, field);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, const Field & field)
{
	return apply_visitor_impl(visitor, field);
}


template <typename Visitor, typename F1, typename F2>
typename Visitor::ResultType apply_binary_visitor_impl2(Visitor & visitor, F1 & field1, F2 & field2)
{
	switch (field2.getType())
	{
		case Field::Types::Null: 				return visitor(field1, field2.get<Null>());
		case Field::Types::UInt64: 				return visitor(field1, field2.get<UInt64>());
		case Field::Types::Int64: 				return visitor(field1, field2.get<Int64>());
		case Field::Types::Float64: 			return visitor(field1, field2.get<Float64>());
		case Field::Types::String: 				return visitor(field1, field2.get<String>());
		case Field::Types::AggregateFunction: 	return visitor(field1, field2.get<Field::AggregateFunctionPtr>());
		case Field::Types::Array: 				return visitor(field1, field2.get<Field::Array>());

		default:
			throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
	}
}

template <typename Visitor, typename F1, typename F2>
typename Visitor::ResultType apply_binary_visitor_impl1(Visitor & visitor, F1 & field1, F2 & field2)
{
	switch (field1.getType())
	{
		case Field::Types::Null: 				return apply_binary_visitor_impl2(visitor, field1.get<Null>(), 		field2);
		case Field::Types::UInt64: 				return apply_binary_visitor_impl2(visitor, field1.get<UInt64>(), 	field2);
		case Field::Types::Int64: 				return apply_binary_visitor_impl2(visitor, field1.get<Int64>(), 	field2);
		case Field::Types::Float64: 			return apply_binary_visitor_impl2(visitor, field1.get<Float64>(), 	field2);
		case Field::Types::String: 				return apply_binary_visitor_impl2(visitor, field1.get<String>(), 	field2);
		case Field::Types::AggregateFunction: 	return apply_binary_visitor_impl2(visitor, field1.get<Field::AggregateFunctionPtr>(), field2);
		case Field::Types::Array: 				return apply_binary_visitor_impl2(visitor, field1.get<Field::Array>(), field2);

		default:
			throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
	}
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, const Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, const Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, const Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, const Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}


typedef std::vector<Field> Array;			/// Значение типа "массив"

template <> struct TypeName<Array> { static std::string get() { return "Array"; } };


/** Возвращает строковый дамп типа */
class FieldVisitorDump : public StaticVisitor<std::string>
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
			s << apply_visitor(FieldVisitorDump(), *it);
		}
		s << "]";

		return s.str();
	}
};

/** Выводит текстовое представление типа, как литерала в SQL запросе */
class FieldVisitorToString : public StaticVisitor<String>
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
			s << apply_visitor(visitor, *it);
		}
		s << "]";

		return s.str();
	}
};

/** Числовой тип преобразует в указанный. */
template <typename T>
class FieldVisitorConvertToNumber : public StaticVisitor<T>
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


class FieldVisitorLess : public StaticVisitor<bool>
{
public:
	template <typename T, typename U>
	bool operator() (const T &, const U &) const { return false; }

    template <typename T>
    bool operator() (const T & lhs, const T & rhs) const { return lhs < rhs; }
};

class FieldVisitorGreater : public StaticVisitor<bool>
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
template <> struct NearestFieldType<Array> 		{ typedef Array 	Type; };
template <> struct NearestFieldType<bool> 		{ typedef UInt64 	Type; };

}


/// Заглушки, чтобы DBObject-ы с полем типа Array компилировались.
namespace mysqlxx
{
	inline std::ostream & operator<< (mysqlxx::EscapeManipResult res, const DB::Array & value)
	{
		throw Poco::Exception("Cannot escape Array with mysqlxx::escape.");
	}

	inline std::ostream & operator<< (mysqlxx::QuoteManipResult res, const DB::Array & value)
	{
		throw Poco::Exception("Cannot quote Array with mysqlxx::quote.");
	}

	inline std::istream & operator>> (mysqlxx::UnEscapeManipResult res, DB::Array & value)
	{
		throw Poco::Exception("Cannot unescape Array with mysqlxx::unescape.");
	}

	inline std::istream & operator>> (mysqlxx::UnQuoteManipResult res, DB::Array & value)
	{
		throw Poco::Exception("Cannot unquote Array with mysqlxx::unquote.");
	}
}


namespace DB
{
	class ReadBuffer;
	class WriteBuffer;

	inline void readBinary(Array & x, ReadBuffer & buf) 		{ throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }
	inline void readText(Array & x, ReadBuffer & buf) 			{ throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }
	inline void readQuoted(Array & x, ReadBuffer & buf) 		{ throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }

	inline void writeBinary(const Array & x, WriteBuffer & buf) { throw Exception("Cannot write Array.", ErrorCodes::NOT_IMPLEMENTED); }
	inline void writeText(const Array & x, WriteBuffer & buf) 	{ throw Exception("Cannot write Array.", ErrorCodes::NOT_IMPLEMENTED); }
	inline void writeQuoted(const Array & x, WriteBuffer & buf) { throw Exception("Cannot write Array.", ErrorCodes::NOT_IMPLEMENTED); }
}
