#ifndef DBMS_CORE_COLUMN_CONST_H
#define DBMS_CORE_COLUMN_CONST_H

#include <Poco/SharedPtr.h>

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
	ColumnConst(size_t s_, T & data_) : s(s_), data(data_) {}

	size_t size() const { return s; }
	Field operator[](size_t n) const { return data; }
	void cut(size_t start, size_t length) { s = length; }
	void clear() { s = 0; }

	/** Более эффективные методы манипуляции */
	T & getData() { return data; }
	const T & getData() const { return data; }

	/** Преобразование из константы в полноценный столбец */
	virtual SharedPtr<IColumn> convertToFullColumn() const = 0;

private:
	size_t s;
	T data;
};


}

#endif
