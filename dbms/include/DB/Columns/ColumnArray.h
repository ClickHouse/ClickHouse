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

	ColumnPtr cut(size_t start, size_t length) const override
	{
		if (length == 0)
			return new ColumnArray(data);

		if (start + length > getOffsets().size())
			throw Exception("Parameter out of bound in IColumnArray::cut() method.",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		size_t nested_offset = offsetAt(start);
		size_t nested_length = getOffsets()[start + length - 1] - nested_offset;

		ColumnArray * res_ = new ColumnArray(data);
		ColumnPtr res = res_;

		res_->data = data->cut(nested_offset, nested_length);
		Offsets_t & res_offsets = res_->getOffsets();

		if (start == 0)
		{
			res_offsets.assign(getOffsets().begin(), getOffsets().begin() + length);
		}
		else
		{
			res_offsets.resize(length);

			for (size_t i = 0; i < length; ++i)
				res_offsets[i] = getOffsets()[start + i] - nested_offset;
		}

		return res;
	}

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

	ColumnPtr filter(const Filter & filt) const override
	{
		size_t size = getOffsets().size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return new ColumnArray(data);

		/// Не слишком оптимально. Можно сделать специализацию для массивов известных типов.
		Filter nested_filt(getOffsets().back());
		for (size_t i = 0; i < size; ++i)
		{
			if (filt[i])
				memset(&nested_filt[offsetAt(i)], 1, sizeAt(i));
			else
				memset(&nested_filt[offsetAt(i)], 0, sizeAt(i));
		}

		ColumnArray * res_ = new ColumnArray(data);
		ColumnPtr res = res_;
		res_->data = data->filter(nested_filt);

		Offsets_t & res_offsets = res_->getOffsets();
		res_offsets.reserve(size);

		size_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			if (filt[i])
			{
				current_offset += sizeAt(i);
				res_offsets.push_back(current_offset);
			}
		}

		return res;
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		size_t size = getOffsets().size();

		if (limit == 0)
			limit = size;
		else
			limit = std::min(size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (limit == 0)
 			return new ColumnArray(data);

		Permutation nested_perm(getOffsets().back());

		ColumnArray * res_ = new ColumnArray(data->cloneEmpty());
		ColumnPtr res = res_;

		Offsets_t & res_offsets = res_->getOffsets();
		res_offsets.resize(limit);
		size_t current_offset = 0;

		for (size_t i = 0; i < limit; ++i)
		{
			for (size_t j = 0; j < sizeAt(perm[i]); ++j)
				nested_perm[current_offset + j] = offsetAt(perm[i]) + j;
			current_offset += sizeAt(perm[i]);
			res_offsets[i] = current_offset;
		}

		if (current_offset != 0)
			res_->data = data->permute(nested_perm, current_offset);

		return res;
	}

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

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		size_t s = size();
		if (limit >= s)
			limit = 0;

		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		if (limit)
		{
			if (reverse)
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<false>(*this));
			else
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<true>(*this));
		}
		else
		{
			if (reverse)
				std::sort(res.begin(), res.end(), less<false>(*this));
			else
				std::sort(res.begin(), res.end(), less<true>(*this));
		}
	}

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


	ColumnPtr replicate(const Offsets_t & replicate_offsets) const override
	{
		/// Не получается реализовать в общем случае.

		if (typeid_cast<const ColumnUInt8 *>(&*data))		return replicate<UInt8>(replicate_offsets);
		if (typeid_cast<const ColumnUInt16 *>(&*data))		return replicate<UInt16>(replicate_offsets);
		if (typeid_cast<const ColumnUInt32 *>(&*data))		return replicate<UInt32>(replicate_offsets);
		if (typeid_cast<const ColumnUInt64 *>(&*data))		return replicate<UInt64>(replicate_offsets);
		if (typeid_cast<const ColumnInt8 *>(&*data))		return replicate<Int8>(replicate_offsets);
		if (typeid_cast<const ColumnInt16 *>(&*data))		return replicate<Int16>(replicate_offsets);
		if (typeid_cast<const ColumnInt32 *>(&*data))		return replicate<Int32>(replicate_offsets);
		if (typeid_cast<const ColumnInt64 *>(&*data))		return replicate<Int64>(replicate_offsets);
		if (typeid_cast<const ColumnFloat32 *>(&*data))		return replicate<Float32>(replicate_offsets);
		if (typeid_cast<const ColumnFloat64 *>(&*data))		return replicate<Float64>(replicate_offsets);
		if (typeid_cast<const ColumnString *>(&*data))		return replicateString(replicate_offsets);

		throw Exception("Replication of column " + getName() + " is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
	}

private:
	ColumnPtr data;
	ColumnPtr offsets;	/// Смещения могут быть разделяемыми для нескольких столбцов - для реализации вложенных структур данных.

	size_t ALWAYS_INLINE offsetAt(size_t i) const	{ return i == 0 ? 0 : getOffsets()[i - 1]; }
	size_t ALWAYS_INLINE sizeAt(size_t i) const		{ return i == 0 ? getOffsets()[0] : (getOffsets()[i] - getOffsets()[i - 1]); }


	/// Размножить значения, если вложенный столбец - ColumnArray<T>.
	template <typename T>
	ColumnPtr replicate(const Offsets_t & replicate_offsets) const
	{
		size_t col_size = size();
		if (col_size != replicate_offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnPtr res = cloneEmpty();

		if (0 == col_size)
			return res;

		ColumnArray & res_ = typeid_cast<ColumnArray &>(*res);

		const typename ColumnVector<T>::Container_t & cur_data = typeid_cast<const ColumnVector<T> &>(*data).getData();
		const Offsets_t & cur_offsets = getOffsets();

		typename ColumnVector<T>::Container_t & res_data = typeid_cast<ColumnVector<T> &>(res_.getData()).getData();
		Offsets_t & res_offsets = res_.getOffsets();

		res_data.reserve(data->size() / col_size * replicate_offsets.back());
		res_offsets.reserve(replicate_offsets.back());

		Offset_t prev_replicate_offset = 0;
		Offset_t prev_data_offset = 0;
		Offset_t current_new_offset = 0;

		for (size_t i = 0; i < col_size; ++i)
		{
			size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
			size_t value_size = cur_offsets[i] - prev_data_offset;

			for (size_t j = 0; j < size_to_replicate; ++j)
			{
				current_new_offset += value_size;
				res_offsets.push_back(current_new_offset);

				res_data.resize(res_data.size() + value_size);
				memcpy(&res_data[res_data.size() - value_size], &cur_data[prev_data_offset], value_size * sizeof(T));
			}

			prev_replicate_offset = replicate_offsets[i];
			prev_data_offset = cur_offsets[i];
		}

		return res;
	}

	/// Размножить значения, если вложенный столбец - ColumnString. Код слишком сложный.
	ColumnPtr replicateString(const Offsets_t & replicate_offsets) const
	{
		size_t col_size = size();
		if (col_size != replicate_offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnPtr res = cloneEmpty();

		if (0 == col_size)
			return res;

		ColumnArray & res_ = typeid_cast<ColumnArray &>(*res);

		const ColumnString & cur_string = typeid_cast<const ColumnString &>(*data);
		const ColumnString::Chars_t & cur_chars = cur_string.getChars();
		const Offsets_t & cur_string_offsets = cur_string.getOffsets();
		const Offsets_t & cur_offsets = getOffsets();

		ColumnString::Chars_t & res_chars = typeid_cast<ColumnString &>(res_.getData()).getChars();
		Offsets_t & res_string_offsets = typeid_cast<ColumnString &>(res_.getData()).getOffsets();
		Offsets_t & res_offsets = res_.getOffsets();

		res_chars.reserve(cur_chars.size() / col_size * replicate_offsets.back());
		res_string_offsets.reserve(cur_string_offsets.size() / col_size * replicate_offsets.back());
		res_offsets.reserve(replicate_offsets.back());

		Offset_t prev_replicate_offset = 0;

		Offset_t prev_cur_offset = 0;
		Offset_t prev_cur_string_offset = 0;

		Offset_t current_res_offset = 0;
		Offset_t current_res_string_offset = 0;

		for (size_t i = 0; i < col_size; ++i)
		{
			/// Насколько размножить массив.
			size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
			/// Количество строк в массиве.
			size_t value_size = cur_offsets[i] - prev_cur_offset;

			size_t sum_chars_size = 0;

			for (size_t j = 0; j < size_to_replicate; ++j)
			{
				current_res_offset += value_size;
				res_offsets.push_back(current_res_offset);

				sum_chars_size = 0;

				size_t prev_cur_string_offset_local = prev_cur_string_offset;
				for (size_t k = 0; k < value_size; ++k)
				{
					/// Размер одной строки.
					size_t chars_size = cur_string_offsets[k + prev_cur_offset] - prev_cur_string_offset_local;

					current_res_string_offset += chars_size;
					res_string_offsets.push_back(current_res_string_offset);

					/// Копирование символов одной строки.
					res_chars.resize(res_chars.size() + chars_size);
					memcpy(&res_chars[res_chars.size() - chars_size], &cur_chars[prev_cur_string_offset_local], chars_size);

					sum_chars_size += chars_size;
					prev_cur_string_offset_local += chars_size;
				}
			}

			prev_replicate_offset = replicate_offsets[i];
			prev_cur_offset = cur_offsets[i];
			prev_cur_string_offset += sum_chars_size;
		}

		return res;
	}
};


}
