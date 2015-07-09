#pragma once

#include <string.h> // memcpy

#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnString.h>


namespace DB
{

using Poco::SharedPtr;

/** Cтолбeц значений типа массив.
  * В памяти он представлен, как один столбец вложенного типа, размер которого равен сумме размеров всех массивов,
  *  и как массив смещений в нём, который позволяет достать каждый элемент.
  */
class ColumnArray final : public IColumn
{
public:
	/** По индексу i находится смещение до начала i + 1 -го элемента. */
	typedef ColumnVector<Offset_t> ColumnOffsets_t;

	/** Создать пустой столбец массивов, с типом значений, как в столбце nested_column */
	explicit ColumnArray(ColumnPtr nested_column, ColumnPtr offsets_column = nullptr)
		: data(nested_column), offsets(offsets_column)
	{
		if (!offsets_column)
		{
			offsets = new ColumnOffsets_t;
		}
		else
		{
			if (!typeid_cast<ColumnOffsets_t *>(&*offsets_column))
				throw Exception("offsets_column must be a ColumnVector<UInt64>", ErrorCodes::ILLEGAL_COLUMN);
		}
	}

	std::string getName() const override { return "ColumnArray(" + data->getName() + ")"; }

	ColumnPtr cloneEmpty() const override
	{
		return new ColumnArray(data->cloneEmpty());
	}

	size_t size() const override
	{
		return getOffsets().size();
	}

	Field operator[](size_t n) const override
	{
		size_t offset = offsetAt(n);
		size_t size = sizeAt(n);
		Array res(size);

		for (size_t i = 0; i < size; ++i)
			res[i] = (*data)[offset + i];

		return res;
	}

	void get(size_t n, Field & res) const override
	{
		size_t offset = offsetAt(n);
		size_t size = sizeAt(n);
		res = Array(size);
		Array & res_arr = DB::get<Array &>(res);

		for (size_t i = 0; i < size; ++i)
			data->get(offset + i, res_arr[i]);
	}

	StringRef getDataAt(size_t n) const override
	{
		/** Возвращает диапазон памяти, покрывающий все элементы массива.
		  * Работает для массивов значений фиксированной длины.
		  * Для массивов строк и массивов массивов полученный кусок памяти может не взаимно-однозначно соответствовать элементам,
		  *  так как содержит лишь уложенные подряд данные, но не смещения.
		  */

		size_t array_size = sizeAt(n);
		if (array_size == 0)
			return StringRef();

		size_t offset_of_first_elem = offsetAt(n);
		StringRef first = data->getDataAtWithTerminatingZero(offset_of_first_elem);

		size_t offset_of_last_elem = getOffsets()[n] - 1;
		StringRef last = data->getDataAtWithTerminatingZero(offset_of_last_elem);

		return StringRef(first.data, last.data + last.size - first.data);
	}

	void insertData(const char * pos, size_t length) override
	{
		/** Аналогично - только для массивов значений фиксированной длины.
		  */
		IColumn * data_ = data.get();
		if (!data_->isFixed())
			throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);

		size_t field_size = data_->sizeOfField();

		const char * end = pos + length;
		size_t elems = 0;
		for (; pos + field_size <= end; pos += field_size, ++elems)
			data_->insertData(pos, field_size);

		if (pos != end)
			throw Exception("Incorrect length argument for method ColumnArray::insertData", ErrorCodes::BAD_ARGUMENTS);

		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + elems);
	}

	ColumnPtr cut(size_t start, size_t length) const override;

	void insert(const Field & x) override
	{
		const Array & array = DB::get<const Array &>(x);
		size_t size = array.size();
		for (size_t i = 0; i < size; ++i)
			data->insert(array[i]);
		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size);
	}

	void insertFrom(const IColumn & src_, size_t n) override
	{
		const ColumnArray & src = static_cast<const ColumnArray &>(src_);
		size_t size = src.sizeAt(n);
		size_t offset = src.offsetAt(n);

		for (size_t i = 0; i < size; ++i)
			data->insertFrom(src.getData(), offset + i);

		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size);
	}

	void insertDefault() override
	{
		getOffsets().push_back(getOffsets().size() == 0 ? 0 : getOffsets().back());
	}

	ColumnPtr filter(const Filter & filt) const override;

	ColumnPtr permute(const Permutation & perm, size_t limit) const override;

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		const ColumnArray & rhs = static_cast<const ColumnArray &>(rhs_);

		/// Не оптимально
		size_t lhs_size = sizeAt(n);
		size_t rhs_size = rhs.sizeAt(m);
		size_t min_size = std::min(lhs_size, rhs_size);
		for (size_t i = 0; i < min_size; ++i)
			if (int res = data.get()->compareAt(offsetAt(n) + i, rhs.offsetAt(m) + i, *rhs.data.get(), nan_direction_hint))
				return res;

		return lhs_size < rhs_size
			? -1
			: (lhs_size == rhs_size
				? 0
				: 1);
	}

	template <bool positive>
	struct less
	{
		const ColumnArray & parent;

		less(const ColumnArray & parent_) : parent(parent_) {}

		bool operator()(size_t lhs, size_t rhs) const
		{
			if (positive)
				return parent.compareAt(lhs, rhs, parent, 1) < 0;
			else
				return parent.compareAt(lhs, rhs, parent, -1) > 0;
		}
	};

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override;

	void reserve(size_t n) override
	{
		getOffsets().reserve(n);
		getData().reserve(n);		/// Средний размер массивов тут никак не учитывается. Или считается, что он не больше единицы.
	}

	size_t byteSize() const override
	{
		return data->byteSize() + getOffsets().size() * sizeof(getOffsets()[0]);
	}

	void getExtremes(Field & min, Field & max) const override
	{
		min = Array();
		max = Array();
	}

	bool hasEqualOffsets(const ColumnArray & other) const
	{
		if (offsets == other.offsets)
			return true;

		const Offsets_t & offsets1 = getOffsets();
		const Offsets_t & offsets2 = other.getOffsets();
		return offsets1.size() == offsets2.size() && 0 == memcmp(&offsets1[0], &offsets2[0], sizeof(offsets1[0]) * offsets1.size());
	}

	/** Более эффективные методы манипуляции */
	IColumn & getData() { return *data; }
	const IColumn & getData() const { return *data; }

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


	ColumnPtr replicate(const Offsets_t & replicate_offsets) const override;

private:
	ColumnPtr data;
	ColumnPtr offsets;	/// Смещения могут быть разделяемыми для нескольких столбцов - для реализации вложенных структур данных.

	size_t ALWAYS_INLINE offsetAt(size_t i) const	{ return i == 0 ? 0 : getOffsets()[i - 1]; }
	size_t ALWAYS_INLINE sizeAt(size_t i) const		{ return i == 0 ? getOffsets()[0] : (getOffsets()[i] - getOffsets()[i - 1]); }


	/// Размножить значения, если вложенный столбец - ColumnArray<T>.
	template <typename T>
	ColumnPtr replicate(const Offsets_t & replicate_offsets) const;

	/// Размножить значения, если вложенный столбец - ColumnString. Код слишком сложный.
	ColumnPtr replicateString(const Offsets_t & replicate_offsets) const;

	/** Неконстантные массивы константных значений - довольно редкое явление.
	  * Большинство функций не умеет с ними работать, и не создаёт такие столбцы в качестве результата.
	  * Исключение - функция replicate (см. FunctionsMiscellaneous.h), которая имеет служебное значение для реализации лямбда-функций.
	  * Только ради неё сделана реализация метода replicate для ColumnArray(ColumnConst).
	  */
	ColumnPtr replicateConst(const Offsets_t & replicate_offsets) const;
};


}
