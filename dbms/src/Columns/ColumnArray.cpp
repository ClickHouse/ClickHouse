#include <DB/Columns/ColumnArray.h>


namespace DB
{


ColumnPtr ColumnArray::cut(size_t start, size_t length) const
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


ColumnPtr ColumnArray::filter(const Filter & filt) const
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


ColumnPtr ColumnArray::permute(const Permutation & perm, size_t limit) const
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


void ColumnArray::getPermutation(bool reverse, size_t limit, Permutation & res) const
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


ColumnPtr ColumnArray::replicate(const Offsets_t & replicate_offsets) const
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
	if (dynamic_cast<const IColumnConst *>(&*data))		return replicateConst(replicate_offsets);

	throw Exception("Replication of column " + getName() + " is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
}


template <typename T>
ColumnPtr ColumnArray::replicate(const Offsets_t & replicate_offsets) const
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


ColumnPtr ColumnArray::replicateString(const Offsets_t & replicate_offsets) const
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


ColumnPtr ColumnArray::replicateConst(const Offsets_t & replicate_offsets) const
{
	size_t col_size = size();
	if (col_size != replicate_offsets.size())
		throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	if (0 == col_size)
		return cloneEmpty();

	const Offsets_t & cur_offsets = getOffsets();

	ColumnOffsets_t * res_column_offsets = new ColumnOffsets_t;
	ColumnPtr res_column_offsets_holder = res_column_offsets;
	Offsets_t & res_offsets = res_column_offsets->getData();
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
		}

		prev_replicate_offset = replicate_offsets[i];
		prev_data_offset = cur_offsets[i];
	}

	return new ColumnArray(getData().cloneResized(current_new_offset), res_column_offsets_holder);
}


}
