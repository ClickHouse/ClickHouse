#ifndef DBMS_COLUMNS_ICOLUMN_H
#define DBMS_COLUMNS_ICOLUMN_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>

namespace DB
{

using Poco::SharedPtr;

/** Интерфейс для хранения столбцов значений в оперативке.
  */
class IColumn
{
public:
	/** Создать пустой столбец такого же типа */
	virtual SharedPtr<IColumn> cloneEmpty() const = 0;

	/** Количество значений в столбце. */
	virtual size_t size() const = 0;

	/** Получить значение n-го элемента.
	  * Используется для преобразования из блоков в строки (например, при выводе значений в текстовый дамп)
	  */
	virtual Field operator[](size_t n) const = 0;

	/** Удалить всё кроме диапазона элементов.
	  * Используется, например, для операции LIMIT.
	  */
	virtual void cut(size_t start, size_t length) = 0;

	/** Вставить значение в конец столбца (количество значений увеличится на 1).
	  * Используется для преобразования из строк в блоки (например, при чтении значений из текстового дампа)
	  */
	virtual void insert(const Field & x) = 0;

	/** Вставить значение "по умолчанию".
	  * Используется, когда нужно увеличить размер столбца, но значение не имеет смысла.
	  * Например, для ColumnNullable, если взведён флаг null, то соответствующее значение во вложенном столбце игнорируется.
	  */
	virtual void insertDefault() = 0;

	/** Очистить */
	virtual void clear() = 0;

	virtual ~IColumn() {}
};


typedef SharedPtr<IColumn> ColumnPtr;
typedef std::vector<ColumnPtr> Columns;

}

#endif
