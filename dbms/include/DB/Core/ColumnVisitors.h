#ifndef DBMS_CORE_COLUMN_VISITORS_H
#define DBMS_CORE_COLUMN_VISITORS_H

#include <boost/variant/static_visitor.hpp>

#include <DB/Core/Field.h>
#include <DB/Core/Column.h>
#include <DB/Core/ColumnTypeToFieldType.h>


namespace DB
{


/** Возвращает количество значений в столбце
  * TODO: поправить для tuple.
  */
class ColumnVisitorSize : public boost::static_visitor<size_t>
{
public:
	template <typename T> size_t operator() (const T & x) const { return x.size(); }
};


/** Возвращает n-ый элемент столбца.
  * TODO: поправить для tuple.
  */
class ColumnVisitorNthElement : public boost::static_visitor<Field>
{
public:
	ColumnVisitorNthElement(size_t n_) : n(n_) {}

	template <typename T> Field operator() (const T & x) const
	{
		return typename ColumnTypeToFieldType<T>::Type(
			x.size() == 1
				? x[0]	/// столбец - константа
				: x[n]);
	}

	Field operator() (const TupleColumn & x) const
	{
		return UInt64(0);	/// заглушка
	}

private:
	size_t n;
};


}

#endif
