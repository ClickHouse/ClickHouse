#ifndef DBMS_FIELD_H
#define DBMS_FIELD_H

#include <string>
#include <vector>

#include <Poco/Types.h>

#include <boost/variant.hpp>
#include <boost/variant/recursive_variant.hpp>

#include <DB/Exception.h>
#include <DB/ErrorCodes.h>


namespace DB
{


typedef Poco::Int64 Int;
typedef Poco::UInt64 UInt;
typedef std::string String;

struct Null {};


/** Используется для хранения значений в памяти
	* - перед сериализацией и после десериализации из БД или результата.
	* - при обработке запроса, для временных таблиц, для результата.
	* Типов данных для хранения больше, чем перечисленных здесь внутренних представлений в памяти.
	* Метаданные этих типов, а также правила их сериализации/десериализации находятся в файле ColumnType.h
	*/
typedef boost::make_recursive_variant<
	Int,
	UInt,
	String,
	Null,
	std::vector<boost::recursive_variant_>	/// FieldVector
	>::type Field;

typedef std::vector<Field> FieldVector;		/// Значение типа "массив"


/** Визитор по умолчанию, который ничего не делает
  * - нужно для того, чтобы наследоваться от него и писать меньше кода
  * для визиторов, которые делают что-то содержательное только для нескольких вариантов.
  */
class FieldVisitorDefaultNothing : public boost::static_visitor<>
{
public:
	template <typename T> void operator() (const T & x) const {}
};


/** Визитор по умолчанию, который для всех вариантов кидает исключение
  * - нужно для того, чтобы наследоваться от него и писать меньше кода
  * для визиторов, которые делают что-то содержательное только для нескольких вариантов,
  * но передача в него других вариантов должна вызывать runtime ошибку.
  */
class FieldVisitorDefaultThrow : public boost::static_visitor<>
{
public:
	void operator() (const DB::Int & x) const
	{
		throw Exception("Unimplemented visitor for variant Int", ErrorCodes::UNIMPLEMENTED_VISITOR_FOR_VARIANT);
	}

	void operator() (const DB::UInt & x) const
	{
		throw Exception("Unimplemented visitor for variant UInt", ErrorCodes::UNIMPLEMENTED_VISITOR_FOR_VARIANT);
	}

	void operator() (const DB::String & x) const
	{
		throw Exception("Unimplemented visitor for variant String", ErrorCodes::UNIMPLEMENTED_VISITOR_FOR_VARIANT);
	}

	void operator() (const DB::Null & x) const
	{
		throw Exception("Unimplemented visitor for variant Null", ErrorCodes::UNIMPLEMENTED_VISITOR_FOR_VARIANT);
	}

	void operator() (const DB::FieldVector & x) const
	{
		throw Exception("Unimplemented visitor for variant FieldVector", ErrorCodes::UNIMPLEMENTED_VISITOR_FOR_VARIANT);
	}
};


/** Возвращает true, если вариант - Null */
class FieldVisitorIsNull : public boost::static_visitor<bool>
{
public:
	template <typename T> bool operator() (const T & x) const { return false; }
	bool operator() (const DB::Null & x) const { return true; }
};


}

#endif
