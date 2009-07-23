#ifndef DBMS_FIELD_H
#define DBMS_FIELD_H

#include <string>
#include <vector>

#include <Poco/Types.h>
#include <Poco/Void.h>
#include <Poco/SharedPtr.h>

#include <boost/strong_typedef.hpp>
#include <boost/variant.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/static_visitor.hpp>

#include <DB/Exception.h>
#include <DB/ErrorCodes.h>
#include <DB/AggregateFunction.h>


namespace DB
{


typedef Poco::Int64 Int;
typedef Poco::UInt64 UInt;
typedef std::string String;
BOOST_STRONG_TYPEDEF(char, Null);


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
	Poco::SharedPtr<IAggregateFunction<boost::recursive_variant_> >,
	std::vector<boost::recursive_variant_>	/// FieldVector
	>::type Field;

typedef std::vector<Field> FieldVector;		/// Значение типа "массив"
typedef Poco::SharedPtr<IAggregateFunction<Field> > AggregateFunctionPtr;


/// почему-то у boost::variant определены операторы < и ==, но не остальные операторы сравнения
inline bool operator!= (const Field & lhs, const Field & rhs) { return !(lhs == rhs); }
inline bool operator<= (const Field & lhs, const Field & rhs) { return lhs < rhs || lhs == rhs; }
inline bool operator>= (const Field & lhs, const Field & rhs) { return !(lhs < rhs); }
inline bool operator> (const Field & lhs, const Field & rhs) { return !(lhs < rhs) && !(lhs == rhs); }


/** Возвращает true, если вариант - Null */
class FieldVisitorIsNull : public boost::static_visitor<bool>
{
public:
	template <typename T> bool operator() (const T & x) const { return false; }
	bool operator() (const DB::Null & x) const { return true; }
};


/** Принимает два значения Field, обновляет первое вторым.
  * Если только первое - не агрегатная функция, результат обновления - замена первого вторым.
  * Иначе - добавление к агрегатной функции значения или объединение агрегатных функций.
  */
class FieldVisitorUpdate : public boost::static_visitor<>
{
public:
	template <typename T, typename U>
	void operator() (T & x, const U & y) const
	{
		x = y;
	}

	template <typename T>
	void operator() (AggregateFunctionPtr & x, const T & y) const
	{
		x->add(y);
	}

	void operator() (AggregateFunctionPtr & x, const AggregateFunctionPtr & y) const
	{
		x->merge(*y);
	}
};


}

#endif
