#pragma once

#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnVector.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ILLEGAL_COLUMN;
	extern const int NOT_IMPLEMENTED;
	extern const int BAD_ARGUMENTS;
}

/** Cтолбeц значений типа массив.
  * В памяти он представлен, как один столбец вложенного типа, размер которого равен сумме размеров всех массивов,
  *  и как массив смещений в нём, который позволяет достать каждый элемент.
  */
class ColumnArray final : public IColumn
{
public:
	/** По индексу i находится смещение до начала i + 1 -го элемента. */
	using ColumnOffsets_t = ColumnVector<Offset_t>;

	/** Создать пустой столбец массивов, с типом значений, как в столбце nested_column */
	explicit ColumnArray(ColumnPtr nested_column, ColumnPtr offsets_column = nullptr);

	std::string getName() const override;
	ColumnPtr cloneResized(size_t size) const override;
	size_t size() const override;
	Field operator[](size_t n) const override;
	void get(size_t n, Field & res) const override;
	StringRef getDataAt(size_t n) const override;
	void insertData(const char * pos, size_t length) override;
	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
	const char * deserializeAndInsertFromArena(const char * pos) override;
	void updateHashWithValue(size_t n, SipHash & hash) const override;
	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
	void insert(const Field & x) override;
	void insertFrom(const IColumn & src_, size_t n) override;
	void insertDefault() override;
	void popBack(size_t n) override;
	ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
	ColumnPtr permute(const Permutation & perm, size_t limit) const override;
	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
	void getPermutation(bool reverse, size_t limit, Permutation & res) const override;
	void reserve(size_t n) override;
	size_t byteSize() const override;
	size_t allocatedSize() const override;
	ColumnPtr replicate(const Offsets_t & replicate_offsets) const override;
	ColumnPtr convertToFullColumnIfConst() const override;
	void getExtremes(Field & min, Field & max) const override;

	bool hasEqualOffsets(const ColumnArray & other) const;

	/** Более эффективные методы манипуляции */
	IColumn & getData() { return *data.get(); }
	const IColumn & getData() const { return *data.get(); }

	ColumnPtr & getDataPtr() { return data; }
	const ColumnPtr & getDataPtr() const { return data; }

	Offsets_t & ALWAYS_INLINE getOffsets()
	{
		return static_cast<ColumnOffsets_t &>(*offsets.get()).getData();
	}

	const Offsets_t & ALWAYS_INLINE getOffsets() const
	{
		return static_cast<const ColumnOffsets_t &>(*offsets.get()).getData();
	}

	ColumnPtr & getOffsetsColumn() { return offsets; }
	const ColumnPtr & getOffsetsColumn() const { return offsets; }

	Columns scatter(ColumnIndex num_columns, const Selector & selector) const override
	{
		return scatterImpl<ColumnArray>(num_columns, selector);
	}

private:
	ColumnPtr data;
	ColumnPtr offsets;	/// Смещения могут быть разделяемыми для нескольких столбцов - для реализации вложенных структур данных.

	size_t ALWAYS_INLINE offsetAt(size_t i) const	{ return i == 0 ? 0 : getOffsets()[i - 1]; }
	size_t ALWAYS_INLINE sizeAt(size_t i) const		{ return i == 0 ? getOffsets()[0] : (getOffsets()[i] - getOffsets()[i - 1]); }


	/// Размножить значения, если вложенный столбец - ColumnVector<T>.
	template <typename T>
	ColumnPtr replicateNumber(const Offsets_t & replicate_offsets) const;

	/// Размножить значения, если вложенный столбец - ColumnString. Код слишком сложный.
	ColumnPtr replicateString(const Offsets_t & replicate_offsets) const;

	/** Неконстантные массивы константных значений - довольно редкое явление.
	  * Большинство функций не умеет с ними работать, и не создаёт такие столбцы в качестве результата.
	  * Исключение - функция replicate (см. FunctionsMiscellaneous.h), которая имеет служебное значение для реализации лямбда-функций.
	  * Только ради неё сделана реализация метода replicate для ColumnArray(ColumnConst).
	  */
	ColumnPtr replicateConst(const Offsets_t & replicate_offsets) const;


	/// Специализации для функции filter.
	template <typename T>
	ColumnPtr filterNumber(const Filter & filt, ssize_t result_size_hint) const;

	ColumnPtr filterString(const Filter & filt, ssize_t result_size_hint) const;
	ColumnPtr filterGeneric(const Filter & filt, ssize_t result_size_hint) const;
};


}
