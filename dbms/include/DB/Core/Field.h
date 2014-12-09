#pragma once

#include <vector>
#include <type_traits>
#include <functional>

#include <boost/static_assert.hpp>

#include <mysqlxx/Date.h>
#include <mysqlxx/DateTime.h>
#include <mysqlxx/Manip.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferFromString.h>

#include <DB/IO/DoubleConverter.h>


namespace DB
{

class Field;
typedef std::vector<Field> Array; /// Значение типа "массив"


using Poco::SharedPtr;

/** 32 хватает с запасом (достаточно 28), но выбрано круглое число,
  * чтобы арифметика при использовании массивов из Field была проще (не содержала умножения).
  */
#define DBMS_TOTAL_FIELD_SIZE 32


/** Discriminated union из нескольких типов.
  * Сделан для замены boost::variant:
  *  является не обобщённым,
  *  зато несколько более эффективным, и более простым.
  *
  * Используется для представления единичного значения одного из нескольких типов в оперативке.
  * Внимание! Предпочтительно вместо единичных значений хранить кусочки столбцов. См. Column.h
  */
class __attribute__((aligned(DBMS_TOTAL_FIELD_SIZE))) Field
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

			/// не POD типы. Для них предполагается relocatable.

			String				= 16,
			Array				= 17,
		};

		static const int MIN_NON_POD = 16;

		static const char * toString(Which which)
		{
			switch (which)
			{
				case Null: 				return "Null";
				case UInt64: 			return "UInt64";
				case Int64: 			return "Int64";
				case Float64: 			return "Float64";
				case String: 			return "String";
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
	}

	/** Не смотря на наличие шаблонного конструктора, этот конструктор всё-равно нужен,
	  *  так как при его отсутствии, компилятор всё-равно сгенерирует конструктор по-умолчанию.
	  */
	Field(const Field & rhs)
	{
		create(rhs);
	}

	Field & operator= (const Field & rhs)
	{
		destroy();
		create(rhs);
		return *this;
	}

	template <typename T>
	Field(const T & rhs)
	{
		create(rhs);
	}

	/// Создать строку inplace.
	Field(const char * data, size_t size)
	{
		create(data, size);
	}

	Field(const unsigned char * data, size_t size)
	{
		create(data, size);
	}

	void assignString(const char * data, size_t size)
	{
		destroy();
		create(data, size);
	}

	void assignString(const unsigned char * data, size_t size)
	{
		destroy();
		create(data, size);
	}

	template <typename T>
	Field & operator= (const T & rhs)
	{
		destroy();
		create(rhs);
		return *this;
	}

	~Field()
	{
		destroy();
	}


	Types::Which getType() const { return which; }
	const char * getTypeName() const { return Types::toString(which); }

	bool isNull() const { return which == Types::Null; }


	template <typename T> T & get()
	{
		typedef typename std::remove_reference<T>::type TWithoutRef;
		TWithoutRef * __attribute__((__may_alias__)) ptr = reinterpret_cast<TWithoutRef*>(storage);
		return *ptr;
	};

	template <typename T> const T & get() const
	{
		typedef typename std::remove_reference<T>::type TWithoutRef;
		const TWithoutRef * __attribute__((__may_alias__)) ptr = reinterpret_cast<const TWithoutRef*>(storage);
		return *ptr;
	};

	template <typename T> T & safeGet()
	{
		const Types::Which requested = TypeToEnum<typename std::remove_cv<typename std::remove_reference<T>::type>::type>::value;
		if (which != requested)
			throw Exception("Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)), ErrorCodes::BAD_GET);
		return get<T>();
	}

	template <typename T> const T & safeGet() const
	{
		const Types::Which requested = TypeToEnum<typename std::remove_cv<typename std::remove_reference<T>::type>::type>::value;
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
			case Types::Null: 				return false;
			case Types::UInt64: 			return get<UInt64>() 				< rhs.get<UInt64>();
			case Types::Int64: 				return get<Int64>() 				< rhs.get<Int64>();
			case Types::Float64: 			return get<Float64>() 				< rhs.get<Float64>();
			case Types::String: 			return get<String>() 				< rhs.get<String>();
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
			case Types::Null: 				return true;
			case Types::UInt64: 			return get<UInt64>() 				<= rhs.get<UInt64>();
			case Types::Int64: 				return get<Int64>() 				<= rhs.get<Int64>();
			case Types::Float64: 			return get<Float64>() 				<= rhs.get<Float64>();
			case Types::String: 			return get<String>() 				<= rhs.get<String>();
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
			case Types::Null: 				return true;
			case Types::UInt64:
			case Types::Int64:
			case Types::Float64:			return get<UInt64>() 				== rhs.get<UInt64>();
			case Types::String: 			return get<String>() 				== rhs.get<String>();
			case Types::Array: 				return get<Array>() 				== rhs.get<Array>();

			default:
				throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
		}
	}

	bool operator!= (const Field & rhs) const
	{
		return !(*this == rhs);
	}

private:
	/// Хватает с запасом
	static const size_t storage_size = DBMS_TOTAL_FIELD_SIZE - sizeof(Types::Which);

	BOOST_STATIC_ASSERT(storage_size >= sizeof(Null));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(UInt64));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(Int64));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(Float64));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(String));
	BOOST_STATIC_ASSERT(storage_size >= sizeof(Array));

	char storage[storage_size] __attribute__((aligned(8)));
	Types::Which which;


	template <typename T>
	void create(const T & x)
	{
		which = TypeToEnum<T>::value;
		T * __attribute__((__may_alias__)) ptr = reinterpret_cast<T*>(storage);
		new (ptr) T(x);
	}

	void create(const Null & x)
	{
		which = Types::Null;
	}

	void create(const Field & x)
	{
		switch (x.which)
		{
			case Types::Null: 				create(Null());							break;
			case Types::UInt64: 			create(x.get<UInt64>());				break;
			case Types::Int64: 				create(x.get<Int64>());					break;
			case Types::Float64: 			create(x.get<Float64>());				break;
			case Types::String: 			create(x.get<String>());				break;
			case Types::Array: 				create(x.get<Array>());					break;
		}
	}

	void create(const char * data, size_t size)
	{
		which = Types::String;
		String * __attribute__((__may_alias__)) ptr = reinterpret_cast<String*>(storage);
		new (ptr) String(data, size);
	}

	void create(const unsigned char * data, size_t size)
	{
		create(reinterpret_cast<const char *>(data), size);
	}


	__attribute__((__always_inline__)) void destroy()
	{
		if (which < Types::MIN_NON_POD)
			return;

		switch (which)
		{
			case Types::String:
				destroy<String>();
				break;
			case Types::Array:
				destroy<Array>();
				break;
			default:
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


template <> struct Field::TypeToEnum<Null> 								{ static const Types::Which value = Types::Null; };
template <> struct Field::TypeToEnum<UInt64> 							{ static const Types::Which value = Types::UInt64; };
template <> struct Field::TypeToEnum<Int64> 							{ static const Types::Which value = Types::Int64; };
template <> struct Field::TypeToEnum<Float64> 							{ static const Types::Which value = Types::Float64; };
template <> struct Field::TypeToEnum<String> 							{ static const Types::Which value = Types::String; };
template <> struct Field::TypeToEnum<Array> 							{ static const Types::Which value = Types::Array; };

template <> struct Field::EnumToType<Field::Types::Null> 				{ typedef Null 						Type; };
template <> struct Field::EnumToType<Field::Types::UInt64> 				{ typedef UInt64 					Type; };
template <> struct Field::EnumToType<Field::Types::Int64> 				{ typedef Int64 					Type; };
template <> struct Field::EnumToType<Field::Types::Float64> 			{ typedef Float64 					Type; };
template <> struct Field::EnumToType<Field::Types::String> 				{ typedef String 					Type; };
template <> struct Field::EnumToType<Field::Types::Array> 				{ typedef Array 					Type; };


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
		case Field::Types::Null: 				return visitor(field.template get<Null>());
		case Field::Types::UInt64: 				return visitor(field.template get<UInt64>());
		case Field::Types::Int64: 				return visitor(field.template get<Int64>());
		case Field::Types::Float64: 			return visitor(field.template get<Float64>());
		case Field::Types::String: 				return visitor(field.template get<String>());
		case Field::Types::Array: 				return visitor(field.template get<Array>());

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
		case Field::Types::Null: 				return visitor(field1, field2.template get<Null>());
		case Field::Types::UInt64: 				return visitor(field1, field2.template get<UInt64>());
		case Field::Types::Int64: 				return visitor(field1, field2.template get<Int64>());
		case Field::Types::Float64: 			return visitor(field1, field2.template get<Float64>());
		case Field::Types::String: 				return visitor(field1, field2.template get<String>());
		case Field::Types::Array: 				return visitor(field1, field2.template get<Array>());

		default:
			throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
	}
}

template <typename Visitor, typename F1, typename F2>
typename Visitor::ResultType apply_binary_visitor_impl1(Visitor & visitor, F1 & field1, F2 & field2)
{
	switch (field1.getType())
	{
		case Field::Types::Null: 				return apply_binary_visitor_impl2(visitor, field1.template get<Null>(), 	field2);
		case Field::Types::UInt64: 				return apply_binary_visitor_impl2(visitor, field1.template get<UInt64>(), 	field2);
		case Field::Types::Int64: 				return apply_binary_visitor_impl2(visitor, field1.template get<Int64>(), 	field2);
		case Field::Types::Float64: 			return apply_binary_visitor_impl2(visitor, field1.template get<Float64>(), 	field2);
		case Field::Types::String: 				return apply_binary_visitor_impl2(visitor, field1.template get<String>(), 	field2);
		case Field::Types::Array: 				return apply_binary_visitor_impl2(visitor, field1.template get<Array>(), field2);

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


template <> struct TypeName<Array> { static std::string get() { return "Array"; } };


/** Возвращает строковый дамп типа */
class FieldVisitorDump : public StaticVisitor<String>
{
private:
	template <typename T>
	static inline String formatQuotedWithPrefix(T x, const char * prefix)
	{
		String res;
		WriteBufferFromString wb(res);
		wb.write(prefix, strlen(prefix));
		writeQuoted(x, wb);
		return res;
	}
public:
	String operator() (const Null 		& x) const { return "NULL"; }
	String operator() (const UInt64 	& x) const { return formatQuotedWithPrefix(x, "UInt64_"); }
	String operator() (const Int64 		& x) const { return formatQuotedWithPrefix(x, "Int64_"); }
	String operator() (const Float64 	& x) const { return formatQuotedWithPrefix(x, "Float64_"); }

	String operator() (const String 	& x) const
	{
		String res;
		WriteBufferFromString wb(res);
		writeQuoted(x, wb);
		return res;
	}

	String operator() (const Array 	& x) const
	{
		String res;
		WriteBufferFromString wb(res);
		FieldVisitorDump visitor;

		wb.write("Array_[", 7);
		for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
		{
			if (it != x.begin())
				wb.write(", ", 2);
			writeString(apply_visitor(visitor, *it), wb);
		}
		writeChar(']', wb);

		return res;
	}
};

/** Выводит текстовое представление типа, как литерала в SQL запросе */
class FieldVisitorToString : public StaticVisitor<String>
{
private:
	template <typename T>
	static inline String formatQuoted(T x)
	{
		String res;
		WriteBufferFromString wb(res);
		writeQuoted(x, wb);
		return res;
	}

	/** В отличие от writeFloatText (и writeQuoted), если число после форматирования выглядит целым, всё равно добавляет десятичную точку.
	  * - для того, чтобы это число могло обратно распарситься как Float64 парсером запроса (иначе распарсится как целое).
	  *
	  * При этом, не оставляет завершающие нули справа.
	  *
	  * NOTE: При таком roundtrip-е, точность может теряться.
	  */
	static String formatFloat(const Float64 x)
	{
		char tmp[25];
		double_conversion::StringBuilder builder{tmp, sizeof(tmp)};

		const auto result = getDoubleToStringConverter().ToShortest(x, &builder);

		if (!result)
			throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

		return { tmp, tmp + builder.position() };
	}

public:
	String operator() (const Null 		& x) const { return "NULL"; }
	String operator() (const UInt64 	& x) const { return formatQuoted(x); }
	String operator() (const Int64 		& x) const { return formatQuoted(x); }
	String operator() (const Float64 	& x) const { return formatFloat(x); }
	String operator() (const String 	& x) const { return formatQuoted(x); }

	String operator() (const Array 		& x) const
	{
		String res;
		WriteBufferFromString wb(res);
		FieldVisitorToString visitor;

		writeChar('[', wb);
		for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
		{
			if (it != x.begin())
				wb.write(", ", 2);
			writeString(apply_visitor(visitor, *it), wb);
		}
		writeChar(']', wb);

		return res;
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

	T operator() (const UInt64 	& x) const { return x; }
	T operator() (const Int64 	& x) const { return x; }
	T operator() (const Float64 & x) const { return x; }
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


template <typename T>
typename NearestFieldType<T>::Type nearestFieldType(const T & x)
{
	return typename NearestFieldType<T>::Type(x);
}

}


/// Заглушки, чтобы DBObject-ы с полем типа Array компилировались.
namespace mysqlxx
{
	inline std::ostream & operator<< (mysqlxx::EscapeManipResult res, const DB::Array & value)
	{
		return res.ostr << apply_visitor(DB::FieldVisitorToString(), value);
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

	/// Предполагается что у всех элементов массива одинаковый тип.
	inline void readBinary(Array & x, ReadBuffer & buf)
	{
		size_t size;
		UInt8 type;
		DB::readBinary(type, buf);
		DB::readBinary(size, buf);

		for (size_t index = 0; index < size; ++index)
		{
			switch (type)
			{
				case Field::Types::Null:
				{
					x.push_back(DB::Field());
					break;
				}
				case Field::Types::UInt64:
				{
					UInt64 value;
					DB::readVarUInt(value, buf);
					x.push_back(value);
					break;
				}
				case Field::Types::Int64:
				{
					Int64 value;
					DB::readVarInt(value, buf);
					x.push_back(value);
					break;
				}
				case Field::Types::Float64:
				{
					Float64 value;
					DB::readFloatBinary(value, buf);
					x.push_back(value);
					break;
				}
				case Field::Types::String:
				{
					std::string value;
					DB::readStringBinary(value, buf);
					x.push_back(value);
					break;
				}
				case Field::Types::Array:
				{
					Array value;
					DB::readBinary(value, buf);
					x.push_back(value);
					break;
				}
			};
		}
	}

	inline void readText(Array & x, ReadBuffer & buf) 			{ throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }
	inline void readQuoted(Array & x, ReadBuffer & buf) 		{ throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }

	/// Предполагается что у всех элементов массива одинаковый тип.
	inline void writeBinary(const Array & x, WriteBuffer & buf)
	{
		UInt8 type = Field::Types::Null;
		size_t size = x.size();
		if (size)
			type = x.front().getType();
		DB::writeBinary(type, buf);
		DB::writeBinary(size, buf);

		for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
		{
			switch (type)
			{
				case Field::Types::Null: break;
				case Field::Types::UInt64:
				{
					DB::writeVarUInt(get<UInt64>(*it), buf);
					break;
				}
				case Field::Types::Int64:
				{
					DB::writeVarInt(get<Int64>(*it), buf);
					break;
				}
				case Field::Types::Float64:
				{
					DB::writeFloatBinary(get<Float64>(*it), buf);
					break;
				}
				case Field::Types::String:
				{
					DB::writeStringBinary(get<std::string>(*it), buf);
					break;
				}
				case Field::Types::Array:
				{
					DB::writeBinary(get<Array>(*it), buf);
					break;
				}
			};
		}
	}

	inline void writeText(const Array & x, WriteBuffer & buf)
	{
		DB::String res = apply_visitor(DB::FieldVisitorToString(), DB::Field(x));
		buf.write(res.data(), res.size());
	}

	inline void writeQuoted(const Array & x, WriteBuffer & buf) { throw Exception("Cannot write Array quoted.", ErrorCodes::NOT_IMPLEMENTED); }
}


#undef DBMS_TOTAL_FIELD_SIZE
