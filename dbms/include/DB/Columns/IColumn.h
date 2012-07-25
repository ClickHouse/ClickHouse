#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{

using Poco::SharedPtr;

class IColumn;
typedef SharedPtr<IColumn> ColumnPtr;
typedef std::vector<ColumnPtr> Columns;
typedef std::vector<IColumn *> ColumnPlainPtrs;
typedef std::vector<const IColumn *> ConstColumnPlainPtrs;


/** Интерфейс для хранения столбцов значений в оперативке.
  */
class IColumn
{
public:
	/** Имя столбца. Для информационных сообщений.
	  */
	virtual std::string getName() const = 0;
	
	/** Столбец представляет собой вектор чисел или числовую константу. 
	  */
	virtual bool isNumeric() const { return false; }

	/** Столбец представляет собой константу
	  */
	virtual bool isConst() const { return false; }

	/** Для числовых столбцов - вернуть sizeof числового типа
	  */
	virtual size_t sizeOfField() const { throw Exception("Cannot get sizeOfField() for column " + getName(), ErrorCodes::CANNOT_GET_SIZE_OF_FIELD); }
	
	/** Создать пустой столбец такого же типа */
	virtual SharedPtr<IColumn> cloneEmpty() const = 0;

	/** Количество значений в столбце. */
	virtual size_t size() const = 0;

	bool empty() const { return size() == 0; }

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

	/** Соединить столбец с одним или несколькими другими.
	  * Используется при склейке маленьких блоков.
	  */
	//virtual void merge(const Columns & columns) = 0;

	/** Оставить только значения, соответствующие фильтру.
	  * Используется для операции WHERE / HAVING.
	  */
	typedef std::vector<UInt8> Filter;
	virtual void filter(const Filter & filt) = 0;

	/** Переставить значения местами, используя указанную перестановку.
	  * Используется при сортировке.
	  */
	typedef std::vector<size_t> Permutation;
	virtual void permute(const Permutation & perm) = 0;

	/** Сравнить (*this)[n] и rhs[m].
	  * Вернуть отрицательное число, 0, или положительное число, если меньше, равно, или больше, соответственно.
	  * Используется при сортировке.
	  */
	virtual int compareAt(size_t n, size_t m, const IColumn & rhs) const = 0;

	/** Получить перестановку чисел, такую, что их порядок соответствует порядку значений в столбце.
	  * Используется при сортировке.
	  */
	virtual Permutation getPermutation() const = 0;

	/** Очистить */
	virtual void clear() = 0;

	/** Приблизительный размер столбца в оперативке в байтах - для профайлинга. 0 - если неизвестно. */
	virtual size_t byteSize() const = 0;

	virtual ~IColumn() {}
};


}
