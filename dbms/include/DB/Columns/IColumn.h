#pragma once

#include <memory>

#include <DB/Common/PODArray.h>
#include <DB/Common/typeid_cast.h>

#include <DB/Core/Field.h>
#include <DB/Common/Exception.h>
#include <DB/Core/StringRef.h>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_GET_SIZE_OF_FIELD;
	extern const int NOT_IMPLEMENTED;
	extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

class IColumn;

using ColumnPtr = std::shared_ptr<IColumn>;
using Columns = std::vector<ColumnPtr>;
using ColumnPlainPtrs = std::vector<IColumn *>;
using ConstColumnPlainPtrs = std::vector<const IColumn *>;

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

	/// Is this column numeric and not nullable?
	virtual bool isNumericNotNullable() const { return isNumeric(); }

	/** Столбец представляет собой константу.
	  */
	virtual bool isConst() const { return false; }

	/// Is this column a container for nullable values?
	virtual bool isNullable() const { return false; }

	/// Is this a null column?
	virtual bool isNull() const { return false; }

	/** Если столбец не константа - возвращает nullptr (либо может вернуть самого себя).
	  * Если столбец константа, то превращает его в полноценный столбец (если тип столбца предполагает такую возможность) и возвращает его.
	  * Отдельный случай:
	  * Если столбец состоит из нескольких других столбцов (пример: кортеж),
	  *  и он может содержать как константные, так и полноценные столбцы,
	  *  то превратить в нём все константные столбцы в полноценные, и вернуть результат.
	  */
	virtual ColumnPtr convertToFullColumnIfConst() const { return {}; }

	/** Значения имеют фиксированную длину.
	  */
	virtual bool isFixed() const { return false; }

	/** Для столбцов фиксированной длины - вернуть длину значения.
	  */
	virtual size_t sizeOfField() const { throw Exception("Cannot get sizeOfField() for column " + getName(), ErrorCodes::CANNOT_GET_SIZE_OF_FIELD); }

	/** Создать столбец с такими же данными. */
	virtual ColumnPtr clone() const { return cut(0, size()); }

	/** Создать пустой столбец такого же типа */
	virtual ColumnPtr cloneEmpty() const { return cloneResized(0); }

	/** Создать столбец такого же типа и указанного размера.
	  * Если размер меньше текущего, данные обрезаются.
	  * Если больше - добавляются значения по умолчанию.
	  */
	virtual ColumnPtr cloneResized(size_t size) const { throw Exception("Cannot cloneResized() column " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

	/** Количество значений в столбце. */
	virtual size_t size() const = 0;

	bool empty() const { return size() == 0; }

	/** Получить значение n-го элемента.
	  * Используется в редких случаях, так как создание временного объекта типа Field может быть дорогим.
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
	virtual ColumnPtr cut(size_t start, size_t length) const
	{
		ColumnPtr res = cloneEmpty();
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

	/** Удалить одно или несколько значений с конца.
	  * Используется, чтобы сделать некоторые операции exception-safe,
	  *  когда после вставки значения сделать что-то ещё не удалось, и нужно откатить вставку.
	  * Если столбец имеет меньше n значений - поведение не определено.
	  * Если n == 0 - поведение не определено.
	  */
	virtual void popBack(size_t n) = 0;

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

	/** Update state of hash function with value at index n.
	  * On subsequent calls of this method for sequence of column values of arbitary types,
	  *  passed bytes to hash must identify sequence of values unambiguously.
	  */
	virtual void updateHashWithValue(size_t n, SipHash & hash) const = 0;

	/** Оставить только значения, соответствующие фильтру.
	  * Используется для операции WHERE / HAVING.
	  * Если result_size_hint > 0, то сделать reserve этого размера у результата;
	  *  если 0, то не делать reserve,
	  *  иначе сделать reserve по размеру исходного столбца.
	  */
	using Filter = PaddedPODArray<UInt8>;
	virtual ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const = 0;

	/** Переставить значения местами, используя указанную перестановку.
	  * Используется при сортировке.
	  * limit - если не равно 0 - положить в результат только первые limit значений.
	  */
	using Permutation = PaddedPODArray<size_t>;
	virtual ColumnPtr permute(const Permutation & perm, size_t limit) const = 0;

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
	using Offset_t = UInt64;
	using Offsets_t = PaddedPODArray<Offset_t>;
	virtual ColumnPtr replicate(const Offsets_t & offsets) const = 0;

	/** Split column to smaller columns. Each value goes to column index, selected by corresponding element of 'selector'.
	  * Selector must contain values from 0 to num_columns - 1.
	  * For default implementation, see scatterImpl.
	  */
	using ColumnIndex = UInt64;
	using Selector = PaddedPODArray<ColumnIndex>;
	virtual Columns scatter(ColumnIndex num_columns, const Selector & selector) const = 0;

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

	/** Size of column data in memory (may be approximate) - for profiling. Zero, if could not be determined. */
	virtual size_t byteSize() const = 0;

	/** Size of memory, allocated for column.
	  * This is greater or equals to byteSize due to memory reservation in containers.
	  * Zero, if could be determined.
	  */
	virtual size_t allocatedSize() const = 0;

	virtual ~IColumn() {}

protected:

	/// Template is to devirtualize calls to insertFrom method.
	/// In derived classes (that use final keyword), implement scatter method as call to scatterImpl.
	template <typename Derived>
	Columns scatterImpl(ColumnIndex num_columns, const Selector & selector) const
	{
		size_t num_rows = size();

		if (num_rows != selector.size())
			throw Exception("Size of selector doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		Columns columns(num_columns);
		for (auto & column : columns)
			column = cloneEmpty();

		{
			size_t reserve_size = num_rows / num_columns * 1.1;	/// 1.1 is just a guess. Better to use n-sigma rule.

			if (reserve_size > 1)
				for (auto & column : columns)
					column->reserve(reserve_size);
		}

		for (size_t i = 0; i < num_rows; ++i)
			static_cast<Derived &>(*columns[selector[i]]).insertFrom(*this, i);

		return columns;
	}
};


}
