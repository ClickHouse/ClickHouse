#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnsCommon.h>


namespace DB
{


void ColumnArray::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
	if (length == 0)
		return;

	const ColumnArray & src_concrete = static_cast<const ColumnArray &>(src);

	if (start + length > src_concrete.getOffsets().size())
		throw Exception("Parameter out of bound in ColumnArray::insertRangeFrom method.",
			ErrorCodes::PARAMETER_OUT_OF_BOUND);

	size_t nested_offset = src_concrete.offsetAt(start);
	size_t nested_length = src_concrete.getOffsets()[start + length - 1] - nested_offset;

	data->insertRangeFrom(src_concrete.getData(), nested_offset, nested_length);

	Offsets_t & cur_offsets = getOffsets();
	const Offsets_t & src_offsets = src_concrete.getOffsets();

	if (start == 0 && cur_offsets.empty())
	{
		cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
	}
	else
	{
		size_t old_size = cur_offsets.size();
		size_t prev_max_offset = old_size ? cur_offsets.back() : 0;
		cur_offsets.resize(old_size + length);

		for (size_t i = 0; i < length; ++i)
			cur_offsets[old_size + i] = src_offsets[start + i] - nested_offset + prev_max_offset;
	}
}


ColumnPtr ColumnArray::filter(const Filter & filt, ssize_t result_size_hint) const
{
	if (typeid_cast<const ColumnUInt8 *>(data.get()))		return filterNumber<UInt8>(filt, result_size_hint);
	if (typeid_cast<const ColumnUInt16 *>(data.get()))		return filterNumber<UInt16>(filt, result_size_hint);
	if (typeid_cast<const ColumnUInt32 *>(data.get()))		return filterNumber<UInt32>(filt, result_size_hint);
	if (typeid_cast<const ColumnUInt64 *>(data.get()))		return filterNumber<UInt64>(filt, result_size_hint);
	if (typeid_cast<const ColumnInt8 *>(data.get()))		return filterNumber<Int8>(filt, result_size_hint);
	if (typeid_cast<const ColumnInt16 *>(data.get()))		return filterNumber<Int16>(filt, result_size_hint);
	if (typeid_cast<const ColumnInt32 *>(data.get()))		return filterNumber<Int32>(filt, result_size_hint);
	if (typeid_cast<const ColumnInt64 *>(data.get()))		return filterNumber<Int64>(filt, result_size_hint);
	if (typeid_cast<const ColumnFloat32 *>(data.get()))		return filterNumber<Float32>(filt, result_size_hint);
	if (typeid_cast<const ColumnFloat64 *>(data.get()))		return filterNumber<Float64>(filt, result_size_hint);
	if (typeid_cast<const ColumnString *>(data.get()))		return filterString(filt, result_size_hint);
	return filterGeneric(filt, result_size_hint);
}

template <typename T>
ColumnPtr ColumnArray::filterNumber(const Filter & filt, ssize_t result_size_hint) const
{
	if (getOffsets().size() == 0)
		return std::make_shared<ColumnArray>(data);

	auto res = std::make_shared<ColumnArray>(data->cloneEmpty());

	auto & res_elems = static_cast<ColumnVector<T> &>(res->getData()).getData();
	Offsets_t & res_offsets = res->getOffsets();

	filterArraysImpl<T>(static_cast<const ColumnVector<T> &>(*data).getData(), getOffsets(), res_elems, res_offsets, filt, result_size_hint);
	return res;
}

ColumnPtr ColumnArray::filterString(const Filter & filt, ssize_t result_size_hint) const
{
	size_t col_size = getOffsets().size();
	if (col_size != filt.size())
		throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	if (0 == col_size)
		return std::make_shared<ColumnArray>(data);

	auto res = std::make_shared<ColumnArray>(data->cloneEmpty());

	const ColumnString & src_string = typeid_cast<const ColumnString &>(*data);
	const ColumnString::Chars_t & src_chars = src_string.getChars();
	const Offsets_t & src_string_offsets = src_string.getOffsets();
	const Offsets_t & src_offsets = getOffsets();

	ColumnString::Chars_t & res_chars = typeid_cast<ColumnString &>(res->getData()).getChars();
	Offsets_t & res_string_offsets = typeid_cast<ColumnString &>(res->getData()).getOffsets();
	Offsets_t & res_offsets = res->getOffsets();

	if (result_size_hint < 0)	/// Остальные случаи не рассматриваем.
	{
		res_chars.reserve(src_chars.size());
		res_string_offsets.reserve(src_string_offsets.size());
		res_offsets.reserve(col_size);
	}

	Offset_t prev_src_offset = 0;
	Offset_t prev_src_string_offset = 0;

	Offset_t prev_res_offset = 0;
	Offset_t prev_res_string_offset = 0;

	for (size_t i = 0; i < col_size; ++i)
	{
		/// Количество строк в массиве.
		size_t array_size = src_offsets[i] - prev_src_offset;

		if (filt[i])
		{
			/// Если массив не пуст - копируем внутренности.
			if (array_size)
			{
				size_t chars_to_copy = src_string_offsets[array_size + prev_src_offset - 1] - prev_src_string_offset;
				size_t res_chars_prev_size = res_chars.size();
				res_chars.resize(res_chars_prev_size + chars_to_copy);
				memcpy(&res_chars[res_chars_prev_size], &src_chars[prev_src_string_offset], chars_to_copy);

				for (size_t j = 0; j < array_size; ++j)
					res_string_offsets.push_back(src_string_offsets[j + prev_src_offset] + prev_res_string_offset - prev_src_string_offset);

				prev_res_string_offset = res_string_offsets.back();
			}

			prev_res_offset += array_size;
			res_offsets.push_back(prev_res_offset);
		}

		if (array_size)
		{
			prev_src_offset += array_size;
			prev_src_string_offset = src_string_offsets[prev_src_offset - 1];
		}
	}

	return res;
}

ColumnPtr ColumnArray::filterGeneric(const Filter & filt, ssize_t result_size_hint) const
{
	size_t size = getOffsets().size();
	if (size != filt.size())
		throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	if (size == 0)
		return std::make_shared<ColumnArray>(data);

	Filter nested_filt(getOffsets().back());
	for (size_t i = 0; i < size; ++i)
	{
		if (filt[i])
			memset(&nested_filt[offsetAt(i)], 1, sizeAt(i));
		else
			memset(&nested_filt[offsetAt(i)], 0, sizeAt(i));
	}

	std::shared_ptr<ColumnArray> res = std::make_shared<ColumnArray>(data);

	ssize_t nested_result_size_hint = 0;
	if (result_size_hint < 0)
		nested_result_size_hint = result_size_hint;
	else if (result_size_hint && result_size_hint < 1000000000 && data->size() < 1000000000)	/// Избегаем переполнения.
		nested_result_size_hint = result_size_hint * data->size() / size;

	res->data = data->filter(nested_filt, nested_result_size_hint);

	Offsets_t & res_offsets = res->getOffsets();
	if (result_size_hint)
		res_offsets.reserve(result_size_hint > 0 ? result_size_hint : size);

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
		return std::make_shared<ColumnArray>(data);

	Permutation nested_perm(getOffsets().back());

	std::shared_ptr<ColumnArray> res = std::make_shared<ColumnArray>(data->cloneEmpty());

	Offsets_t & res_offsets = res->getOffsets();
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
		res->data = data->permute(nested_perm, current_offset);

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

	if (typeid_cast<const ColumnUInt8 *>(data.get()))		return replicateNumber<UInt8>(replicate_offsets);
	if (typeid_cast<const ColumnUInt16 *>(data.get()))		return replicateNumber<UInt16>(replicate_offsets);
	if (typeid_cast<const ColumnUInt32 *>(data.get()))		return replicateNumber<UInt32>(replicate_offsets);
	if (typeid_cast<const ColumnUInt64 *>(data.get()))		return replicateNumber<UInt64>(replicate_offsets);
	if (typeid_cast<const ColumnInt8 *>(data.get()))		return replicateNumber<Int8>(replicate_offsets);
	if (typeid_cast<const ColumnInt16 *>(data.get()))		return replicateNumber<Int16>(replicate_offsets);
	if (typeid_cast<const ColumnInt32 *>(data.get()))		return replicateNumber<Int32>(replicate_offsets);
	if (typeid_cast<const ColumnInt64 *>(data.get()))		return replicateNumber<Int64>(replicate_offsets);
	if (typeid_cast<const ColumnFloat32 *>(data.get()))		return replicateNumber<Float32>(replicate_offsets);
	if (typeid_cast<const ColumnFloat64 *>(data.get()))		return replicateNumber<Float64>(replicate_offsets);
	if (typeid_cast<const ColumnString *>(data.get()))		return replicateString(replicate_offsets);
	if (dynamic_cast<const IColumnConst *>(data.get()))		return replicateConst(replicate_offsets);

	throw Exception("Replication of column " + getName() + " is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
}


template <typename T>
ColumnPtr ColumnArray::replicateNumber(const Offsets_t & replicate_offsets) const
{
	size_t col_size = size();
	if (col_size != replicate_offsets.size())
		throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	ColumnPtr res = cloneEmpty();

	if (0 == col_size)
		return res;

	ColumnArray & res_ = typeid_cast<ColumnArray &>(*res);

	const typename ColumnVector<T>::Container_t & src_data = typeid_cast<const ColumnVector<T> &>(*data).getData();
	const Offsets_t & src_offsets = getOffsets();

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
		size_t value_size = src_offsets[i] - prev_data_offset;

		for (size_t j = 0; j < size_to_replicate; ++j)
		{
			current_new_offset += value_size;
			res_offsets.push_back(current_new_offset);

			res_data.resize(res_data.size() + value_size);
			memcpy(&res_data[res_data.size() - value_size], &src_data[prev_data_offset], value_size * sizeof(T));
		}

		prev_replicate_offset = replicate_offsets[i];
		prev_data_offset = src_offsets[i];
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

	ColumnArray & res_ = static_cast<ColumnArray &>(*res);

	const ColumnString & src_string = typeid_cast<const ColumnString &>(*data);
	const ColumnString::Chars_t & src_chars = src_string.getChars();
	const Offsets_t & src_string_offsets = src_string.getOffsets();
	const Offsets_t & src_offsets = getOffsets();

	ColumnString::Chars_t & res_chars = typeid_cast<ColumnString &>(res_.getData()).getChars();
	Offsets_t & res_string_offsets = typeid_cast<ColumnString &>(res_.getData()).getOffsets();
	Offsets_t & res_offsets = res_.getOffsets();

	res_chars.reserve(src_chars.size() / col_size * replicate_offsets.back());
	res_string_offsets.reserve(src_string_offsets.size() / col_size * replicate_offsets.back());
	res_offsets.reserve(replicate_offsets.back());

	Offset_t prev_replicate_offset = 0;

	Offset_t prev_src_offset = 0;
	Offset_t prev_src_string_offset = 0;

	Offset_t current_res_offset = 0;
	Offset_t current_res_string_offset = 0;

	for (size_t i = 0; i < col_size; ++i)
	{
		/// Насколько размножить массив.
		size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
		/// Количество строк в массиве.
		size_t value_size = src_offsets[i] - prev_src_offset;
		/// Количество символов в строках массива, включая нулевые байты.
		size_t sum_chars_size = value_size == 0 ? 0 : (src_string_offsets[prev_src_offset + value_size - 1] - prev_src_string_offset);

		for (size_t j = 0; j < size_to_replicate; ++j)
		{
			current_res_offset += value_size;
			res_offsets.push_back(current_res_offset);

			size_t prev_src_string_offset_local = prev_src_string_offset;
			for (size_t k = 0; k < value_size; ++k)
			{
				/// Размер одной строки.
				size_t chars_size = src_string_offsets[k + prev_src_offset] - prev_src_string_offset_local;

				current_res_string_offset += chars_size;
				res_string_offsets.push_back(current_res_string_offset);

				prev_src_string_offset_local += chars_size;
			}

			/// Копирование символов массива строк.
			res_chars.resize(res_chars.size() + sum_chars_size);
			memcpySmallAllowReadWriteOverflow15(
				&res_chars[res_chars.size() - sum_chars_size], &src_chars[prev_src_string_offset], sum_chars_size);
		}

		prev_replicate_offset = replicate_offsets[i];
		prev_src_offset = src_offsets[i];
		prev_src_string_offset += sum_chars_size;
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

	const Offsets_t & src_offsets = getOffsets();

	auto res_column_offsets = std::make_shared<ColumnOffsets_t>();
	Offsets_t & res_offsets = res_column_offsets->getData();
	res_offsets.reserve(replicate_offsets.back());

	Offset_t prev_replicate_offset = 0;
	Offset_t prev_data_offset = 0;
	Offset_t current_new_offset = 0;

	for (size_t i = 0; i < col_size; ++i)
	{
		size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
		size_t value_size = src_offsets[i] - prev_data_offset;

		for (size_t j = 0; j < size_to_replicate; ++j)
		{
			current_new_offset += value_size;
			res_offsets.push_back(current_new_offset);
		}

		prev_replicate_offset = replicate_offsets[i];
		prev_data_offset = src_offsets[i];
	}

	return std::make_shared<ColumnArray>(getData().cloneResized(current_new_offset), res_column_offsets);
}


}
