#ifndef DBMS_CORE_ICOLUMN_H
#define DBMS_CORE_ICOLUMN_H

#include <DB/Core/Field.h>

namespace DB
{

/** Интерфейс для хранения столбцов значений в оперативке.
  */
class IColumn
{
public:
	/** Количество значений в столбце */
	virtual size_t size() const = 0;

	/** Получить значение n-го элемента */
	virtual Field operator[](size_t n) const = 0;

	/** Удалить всё кроме диапазона элементов */
	virtual void cut(size_t start, size_t length) = 0;

	virtual ~IColumn() {}
};


}

#endif
