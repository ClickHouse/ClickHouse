#pragma once

#include <vector>
#include <algorithm>
#include <type_traits>
#include <functional>

#include <DB/Common/Exception.h>
#include <DB/Core/Types.h>
#include <common/strong_typedef.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int BAD_TYPE_OF_FIELD;
	extern const int BAD_GET;
	extern const int NOT_IMPLEMENTED;
}

class Field;
using Array = std::vector<Field>; /// Значение типа "массив"
using TupleBackend = std::vector<Field>;
STRONG_TYPEDEF(TupleBackend, Tuple); /// Значение типа "кортеж"


/** 32 хватает с запасом (достаточно 28), но выбрано круглое число,
  * чтобы арифметика при использовании массивов из Field была проще (не содержала умножения).
  */
#define DBMS_MIN_FIELD_SIZE 32


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

			/// не POD типы. Для них предполагается relocatable.

			String				= 16,
			Array				= 17,
			Tuple				= 18,
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
				case Tuple: 			return "Tuple";

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

	Field(Field && rhs)
	{
		move(std::move(rhs));
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

	Field & operator= (const Field & rhs)
	{
		if (this != &rhs)
		{
			destroy();
			create(rhs);
		}
		return *this;
	}

	Field & operator= (Field && rhs)
	{
		if (this != &rhs)
		{
			move(std::move(rhs));
		}
		return *this;
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
		using TWithoutRef = typename std::remove_reference<T>::type;
		TWithoutRef * __attribute__((__may_alias__)) ptr = reinterpret_cast<TWithoutRef*>(storage);
		return *ptr;
	};

	template <typename T> const T & get() const
	{
		using TWithoutRef = typename std::remove_reference<T>::type;
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
			case Types::Tuple: 				return get<Tuple>() 				< rhs.get<Tuple>();

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
			case Types::Tuple: 				return get<Tuple>() 				<= rhs.get<Tuple>();

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
			case Types::Tuple: 				return get<Tuple>() 				== rhs.get<Tuple>();

			default:
				throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
		}
	}

	bool operator!= (const Field & rhs) const
	{
		return !(*this == rhs);
	}

private:
	static const size_t storage_size = std::max({
		DBMS_MIN_FIELD_SIZE - sizeof(Types::Which),
		sizeof(Null), sizeof(UInt64), sizeof(Int64), sizeof(Float64), sizeof(String), sizeof(Array), sizeof(Tuple)});

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
			case Types::Tuple: 				create(x.get<Tuple>());					break;
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
			case Types::Tuple:
				destroy<Tuple>();
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

	template <typename T>
	void moveValue(Field && x)
	{
		T * __attribute__((__may_alias__)) ptr_this = reinterpret_cast<T*>(storage);
		T * __attribute__((__may_alias__)) ptr_x    = reinterpret_cast<T*>(x.storage);

		new (ptr_this) T(std::move(*ptr_x));
	}

	void move(Field && x)
	{
		which = x.which;

		switch (x.which)
		{
			case Types::Null: 				create(Null());							break;
			case Types::UInt64: 			create(x.get<UInt64>());				break;
			case Types::Int64: 				create(x.get<Int64>());					break;
			case Types::Float64: 			create(x.get<Float64>());				break;
			case Types::String: 			moveValue<String>(std::move(x));		break;
			case Types::Array: 				moveValue<Array>(std::move(x));			break;
			case Types::Tuple: 				moveValue<Tuple>(std::move(x));			break;
		}
	}
};

#undef DBMS_MIN_FIELD_SIZE


template <> struct Field::TypeToEnum<Null> 								{ static const Types::Which value = Types::Null; };
template <> struct Field::TypeToEnum<UInt64> 							{ static const Types::Which value = Types::UInt64; };
template <> struct Field::TypeToEnum<Int64> 							{ static const Types::Which value = Types::Int64; };
template <> struct Field::TypeToEnum<Float64> 							{ static const Types::Which value = Types::Float64; };
template <> struct Field::TypeToEnum<String> 							{ static const Types::Which value = Types::String; };
template <> struct Field::TypeToEnum<Array> 							{ static const Types::Which value = Types::Array; };
template <> struct Field::TypeToEnum<Tuple> 							{ static const Types::Which value = Types::Tuple; };

template <> struct Field::EnumToType<Field::Types::Null> 				{ using Type = Null 					; };
template <> struct Field::EnumToType<Field::Types::UInt64> 				{ using Type = UInt64 				; };
template <> struct Field::EnumToType<Field::Types::Int64> 				{ using Type = Int64 				; };
template <> struct Field::EnumToType<Field::Types::Float64> 			{ using Type = Float64 				; };
template <> struct Field::EnumToType<Field::Types::String> 				{ using Type = String 				; };
template <> struct Field::EnumToType<Field::Types::Array> 				{ using Type = Array 				; };
template <> struct Field::EnumToType<Field::Types::Tuple> 				{ using Type = Tuple 				; };


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


template <> struct TypeName<Array> { static std::string get() { return "Array"; } };
template <> struct TypeName<Tuple> { static std::string get() { return "Tuple"; } };


template <typename T> struct NearestFieldType;

template <> struct NearestFieldType<UInt8> 		{ using Type = UInt64 ; };
template <> struct NearestFieldType<UInt16> 	{ using Type = UInt64 ; };
template <> struct NearestFieldType<UInt32> 	{ using Type = UInt64 ; };
template <> struct NearestFieldType<UInt64> 	{ using Type = UInt64 ; };
template <> struct NearestFieldType<Int8> 		{ using Type = Int64 ; };
template <> struct NearestFieldType<Int16> 		{ using Type = Int64 ; };
template <> struct NearestFieldType<Int32> 		{ using Type = Int64 ; };
template <> struct NearestFieldType<Int64> 		{ using Type = Int64 ; };
template <> struct NearestFieldType<Float32> 	{ using Type = Float64 ; };
template <> struct NearestFieldType<Float64> 	{ using Type = Float64 ; };
template <> struct NearestFieldType<String> 	{ using Type = String ; };
template <> struct NearestFieldType<Array> 		{ using Type = Array ; };
template <> struct NearestFieldType<Tuple> 		{ using Type = Tuple	; };
template <> struct NearestFieldType<bool> 		{ using Type = UInt64 ; };
template <> struct NearestFieldType<Null>	{ using Type = Null; };


template <typename T>
typename NearestFieldType<T>::Type nearestFieldType(const T & x)
{
	return typename NearestFieldType<T>::Type(x);
}

}


/// Заглушки, чтобы DBObject-ы с полем типа Array компилировались.
#include <mysqlxx/Manip.h>

namespace mysqlxx
{
	std::ostream & operator<< (mysqlxx::EscapeManipResult res, const DB::Array & value);
	std::ostream & operator<< (mysqlxx::QuoteManipResult res, const DB::Array & value);
	std::istream & operator>> (mysqlxx::UnEscapeManipResult res, DB::Array & value);
	std::istream & operator>> (mysqlxx::UnQuoteManipResult res, DB::Array & value);

	std::ostream & operator<< (mysqlxx::EscapeManipResult res, const DB::Tuple & value);
	std::ostream & operator<< (mysqlxx::QuoteManipResult res, const DB::Tuple & value);
	std::istream & operator>> (mysqlxx::UnEscapeManipResult res, DB::Tuple & value);
	std::istream & operator>> (mysqlxx::UnQuoteManipResult res, DB::Tuple & value);
}


namespace DB
{
	class ReadBuffer;
	class WriteBuffer;

	/// Предполагается что у всех элементов массива одинаковый тип.
	void readBinary(Array & x, ReadBuffer & buf);

	inline void readText(Array & x, ReadBuffer & buf) 			{ throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }
	inline void readQuoted(Array & x, ReadBuffer & buf) 		{ throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }

	/// Предполагается что у всех элементов массива одинаковый тип.
	void writeBinary(const Array & x, WriteBuffer & buf);

	void writeText(const Array & x, WriteBuffer & buf);

	inline void writeQuoted(const Array & x, WriteBuffer & buf) { throw Exception("Cannot write Array quoted.", ErrorCodes::NOT_IMPLEMENTED); }
}

namespace DB
{
	void readBinary(Tuple & x, ReadBuffer & buf);

	inline void readText(Tuple & x, ReadBuffer & buf) 			{ throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED); }
	inline void readQuoted(Tuple & x, ReadBuffer & buf) 		{ throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED); }

	void writeBinary(const Tuple & x, WriteBuffer & buf);

	void writeText(const Tuple & x, WriteBuffer & buf);

	inline void writeQuoted(const Tuple & x, WriteBuffer & buf) { throw Exception("Cannot write Tuple quoted.", ErrorCodes::NOT_IMPLEMENTED); }
}
