#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Common/PODArray.h>
#include <DB/Common/typeid_cast.h>

#include <DB/Core/Field.h>
#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/StringRef.h>


namespace DB
{

using Poco::SharedPtr;

class IColumn;
typedef SharedPtr<IColumn> ColumnPtr;
typedef std::vector<ColumnPtr> Columns;
typedef std::vector<IColumn *> ColumnPlainPtrs;
typedef std::vector<const IColumn *> ConstColumnPlainPtrs;

class Arena;


/** Интерфейс для хранения столбцов значений в оперативке.
  */
class IColumn : private boost::noncopyable
{
public:
	/** Имя столбца. Для информационных сообщений.
	  */
	virtual std::string getName() const = 0;

	/** Столбец представляет собой вектор чисел или числовую константу.
	  */
	virtual bool isNumeric() const { return false; }

	/** Столбец представляет собой константу.
	  */
	virtual bool isConst() const { return false; }

	/** Если столбец не константа - возвращает nullptr (либо может вернуть самого себя).
	  * Если столбец константа, то превращает его в полноценный столбец (если тип столбца предполагает такую возможность) и возвращает его.
	  * Отдельный случай:
	  * Если столбец состоит из нескольких других столбцов (пример: кортеж),
	  *  и он может содержать как константные, так и полноценные столбцы,
	  *  то превратить в нём все константные столбцы в полноценные, и вернуть результат.
	  */
	virtual SharedPtr<IColumn> convertToFullColumnIfConst() const { return {}; }

	/** Значения имеют фиксированную длину.
	  */
	virtual bool isFixed() const { return false; }

	/** Для столбцов фиксированной длины - вернуть длину значения.
	  */
	virtual size_t sizeOfField() const { throw Exception("Cannot get sizeOfField() for column " + getName(), ErrorCodes::CANNOT_GET_SIZE_OF_FIELD); }

	/** Создать столбец с такими же данными. */
	virtual SharedPtr<IColumn> clone() const { return cut(0, size()); }

	/** Создать пустой столбец такого же типа */
	virtual SharedPtr<IColumn> cloneEmpty() const { return cloneResized(0); }

	/** Создать столбец такого же типа и указанного размера.
	  * Если размер меньше текущего, данные обрезаются.
	  * Если больше - добавляются значения по умолчанию.
	  */
	virtual SharedPtr<IColumn> cloneResized(size_t size) const { throw Exception("Cannot cloneResized() column " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

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
	  */
	virtual StringRef getDataAt(size_t n) const = 0;

	/** Отличется от функции getDataAt только для строк переменной длины.
	  * Для них возвращаются данные с нулём на конце (то есть, size на единицу больше длины строки).
	  */
	virtual StringRef getDataAtWithTerminatingZero(size_t n) const
	{
		return getDataAt(n);
	}

	/** Для целых чисел - преобразовать в UInt64 static_cast-ом.
	  * Для чисел с плавающей запятой - преобразовать в младшие байты UInt64 как memcpy; остальные байты, если есть - нулевые.
	  * Используется для оптимизации некоторых вычислений (например, агрегации).
	  */
	virtual UInt64 get64(size_t n) const
	{
		throw Exception("Method get64 is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Удалить всё кроме диапазона элементов.
	  * Используется, например, для операции LIMIT.
	  */
	virtual SharedPtr<IColumn> cut(size_t start, size_t length) const
	{
		SharedPtr<IColumn> res = cloneEmpty();
		res.get()->insertRangeFrom(*this, start, length);
		return res;
	}

	/** Вставить значение в конец столбца (количество значений увеличится на 1).
	  * Используется для преобразования из строк в блоки (например, при чтении значений из текстового дампа)
	  */
	virtual void insert(const Field & x) = 0;

	/** Вставить значение в конец столбца из другого столбца такого же типа, по заданному индексу.
	  * Используется для merge-sort. Может быть реализована оптимальнее, чем реализация по-умолчанию.
	  */
	virtual void insertFrom(const IColumn & src, size_t n) { insert(src[n]); }

	/** Вставить в конец столбца диапазон элементов из другого столбца.
	  * Может использоваться для склейки столбцов.
	  */
	virtual void insertRangeFrom(const IColumn & src, size_t start, size_t length) = 0;

	/** Вставить данные, расположенные в указанном куске памяти, если возможно.
	  * (если не реализуемо - кидает исключение)
	  * Используется для оптимизации некоторых вычислений (например, агрегации).
	  * В случае данных постоянной длины, параметр length может игнорироваться.
	  */
	virtual void insertData(const char * pos, size_t length) = 0;

	/** Отличется от функции insertData только для строк переменной длины.
	  * Для них принимаются данные уже с нулём на конце (то есть, length на единицу больше длины строки).
	  * В переданном куске памяти обязательно должен быть ноль на конце.
	  */
	virtual void insertDataWithTerminatingZero(const char * pos, size_t length)
	{
		insertData(pos, length);
	}

	/** Вставить значение "по умолчанию".
	  * Используется, когда нужно увеличить размер столбца, но значение не имеет смысла.
	  * Например, для ColumnNullable, если взведён флаг null, то соответствующее значение во вложенном столбце игнорируется.
	  */
	virtual void insertDefault() = 0;

	/** Сериализовать значение, расположив его в непрерывном куске памяти в Arena.
	  * Значение можно будет потом прочитать обратно. Используется для агрегации.
	  * Метод похож на getDataAt, но может работать для тех случаев,
	  *  когда значение не однозначно соответствует какому-то уже существующему непрерывному куску памяти
	  *  - например, для массива строк, чтобы получить однозначное соответствие, надо укладывать строки вместе с их размерами.
	  * Параметр begin - см. метод Arena::allocContinue.
	  */
	virtual StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const = 0;

	/** Десериализовать значение, которое было сериализовано с помощью serializeValueIntoArena.
	  * Вернуть указатель на позицию после прочитанных данных.
	  */
	virtual const char * deserializeAndInsertFromArena(const char * pos) = 0;

	/** Соединить столбец с одним или несколькими другими.
	  * Используется при склейке маленьких блоков.
	  */
	//virtual void merge(const Columns & columns) = 0;

	/** Оставить только значения, соответствующие фильтру.
	  * Используется для операции WHERE / HAVING.
	  * Если result_size_hint > 0, то сделать reserve этого размера у результата;
	  *  если 0, то не делать reserve,
	  *  иначе сделать reserve по размеру исходного столбца.
	  */
	typedef PODArray<UInt8> Filter;
	virtual SharedPtr<IColumn> filter(const Filter & filt, ssize_t result_size_hint) const = 0;

	/** Переставить значения местами, используя указанную перестановку.
	  * Используется при сортировке.
	  * limit - если не равно 0 - положить в результат только первые limit значений.
	  */
	typedef PODArray<size_t> Permutation;
	virtual SharedPtr<IColumn> permute(const Permutation & perm, size_t limit) const = 0;

	/** Сравнить (*this)[n] и rhs[m].
	  * Вернуть отрицательное число, 0, или положительное число, если меньше, равно, или больше, соответственно.
	  * Используется при сортировке.
	  *
	  * Если одно из значений является NaN, то:
	  * - если nan_direction_hint == -1 - NaN считаются меньше всех чисел;
	  * - если nan_direction_hint == 1 - NaN считаются больше всех чисел;
	  * По-сути: nan_direction_hint == -1 говорит, что сравнение идёт для сортировки по убыванию,
	  *  чтобы NaN-ы были в конце.
	  *
	  * Для чисел не с плавающей запятой, nan_direction_hint игнорируется.
	  */
	virtual int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const = 0;

	/** Получить перестановку чисел, такую, что для упорядочивания значений в столбце,
	  *  надо применить эту сортировку - то есть, поставить на i-е место значение по индексу perm[i].
	  * Используется при сортировке.
	  * reverse - обратный порядок (по возрастанию). limit - если не равно 0 - для частичной сортировки только первых значений.
	  * Независимо от порядка, NaN-ы располагаются в конце.
	  */
	virtual void getPermutation(bool reverse, size_t limit, Permutation & res) const = 0;

	/** Размножить все значения столько раз, сколько прописано в offsets.
	  * (i-е значение размножается в offsets[i] - offsets[i - 1] значений.)
	  * Необходимо для реализации операции ARRAY JOIN.
	  */
	typedef UInt64 Offset_t;
	typedef PODArray<Offset_t> Offsets_t;
	virtual SharedPtr<IColumn> replicate(const Offsets_t & offsets) const = 0;

	/** Посчитать минимум и максимум по столбцу.
	  * Функция должна быть реализована полноценно только для числовых столбцов, а также дат/дат-с-временем.
	  * Для строк и массивов функция должна возвращать значения по-умолчанию
	  *  (за исключением константных столбцов, для которых можно возвращать значение константы).
	  * Если столбец пустой - функция должна возвращать значения по-умолчанию.
	  */
	virtual void getExtremes(Field & min, Field & max) const = 0;

	/** Если возможно - зарезервировать место для указанного количества элементов. Если невозможно или не поддерживается - ничего не делать.
	  * Функция влияет только на производительность.
	  */
	virtual void reserve(size_t n) {};

	/** Приблизительный размер столбца в оперативке в байтах - для профайлинга. 0 - если неизвестно. */
	virtual size_t byteSize() const = 0;

	virtual ~IColumn() {}
};


}
