#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/StringRef.h>

#include <DB/Common/PODArray.h>


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

	/** То же самое, но позволяет избежать лишнего копирования, если Field, например, кладётся в контейнер.
	  */
	virtual void get(size_t n, Field & res) const = 0;

	/** Получить кусок памяти, в котором хранится значение, если возможно.
	  * (если не реализуемо - кидает исключение)
	  * Используется для оптимизации некоторых вычислений (например, агрегации).
	  * Строки переменной длины возвращаются с нулём на конце.
	  */
	virtual StringRef getDataAt(size_t n) const = 0;

	/** Удалить всё кроме диапазона элементов.
	  * Используется, например, для операции LIMIT.
	  */
	virtual void cut(size_t start, size_t length) = 0;

	/** Вставить значение в конец столбца (количество значений увеличится на 1).
	  * Используется для преобразования из строк в блоки (например, при чтении значений из текстового дампа)
	  */
	virtual void insert(const Field & x) = 0;

	/** Вставить значение в конец столбца из другого столбца такого же типа, по заданному индексу.
	  * Используется для merge-sort. Может быть реализована оптимальнее, чем реализация по-умолчанию.
	  */
	virtual void insertFrom(const IColumn & src, size_t n) { insert(src[n]); }

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
	typedef PODArray<UInt8> Filter;
	virtual void filter(const Filter & filt) = 0;

	/** Переставить значения местами, используя указанную перестановку.
	  * Используется при сортировке.
	  */
	typedef PODArray<size_t> Permutation;
	virtual void permute(const Permutation & perm) = 0;

	/** Сравнить (*this)[n] и rhs[m].
	  * Вернуть отрицательное число, 0, или положительное число, если меньше, равно, или больше, соответственно.
	  * Используется при сортировке.
	  */
	virtual int compareAt(size_t n, size_t m, const IColumn & rhs) const = 0;

	/** Получить перестановку чисел, такую, что их порядок соответствует порядку значений в столбце.
	  * Используется при сортировке.
	  */
	virtual void getPermutation(Permutation & res) const = 0;

	/** Размножить все значения столько раз, сколько прописано в offsets.
	  * (i-е значение размножается в offsets[i] - offsets[i - 1] значений.)
	  */
	typedef UInt64 Offset_t;
	typedef PODArray<Offset_t> Offsets_t;
	virtual void replicate(const Offsets_t & offsets) = 0;

	/** Очистить */
	virtual void clear() = 0;

	/** Приблизительный размер столбца в оперативке в байтах - для профайлинга. 0 - если неизвестно. */
	virtual size_t byteSize() const = 0;

	virtual ~IColumn() {}
};


}
