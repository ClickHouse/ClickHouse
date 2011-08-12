#ifndef DBMS_CORE_COLUMN_CONST_H
#define DBMS_CORE_COLUMN_CONST_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

/** шаблон для столбцов-констант (столбцов одинаковых значений).
  */
template <typename T>
class ColumnConst : public IColumn
{
public:
	ColumnConst(size_t s_, const T & data_) : s(s_), data(data_) {}

	ColumnPtr cloneEmpty() const { return new ColumnConst(0, data); }
	size_t size() const { return s; }
	Field operator[](size_t n) const { return typename NearestFieldType<T>::Type(data); }
	void cut(size_t start, size_t length) { s = length; }
	void clear() { s = 0; }
	void insert(const Field & x)
	{
		throw Exception("Cannot insert element into constant column", ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
	}
	void insertDefault() { ++s; }

	/** Более эффективные методы манипуляции */
	T & getData() { return data; }
	const T & getData() const { return data; }

	/** Преобразование из константы в полноценный столбец */
//	virtual ColumnPtr convertToFullColumn() const = 0;

private:
	size_t s;
	T data;
};


}

#endif
