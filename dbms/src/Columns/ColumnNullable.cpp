#include <DB/Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

ColumnNullable::ColumnNullable(ColumnPtr nested_column_, ColumnPtr null_map_)
	: nested_column{nested_column_}, null_map{null_map_}
{
	if (nested_column->isNullable())
		throw Exception{"A nullable column cannot contain another nullable column", ErrorCodes::LOGICAL_ERROR};
}


ColumnPtr ColumnNullable::convertToFullColumnIfConst() const
{
	ColumnPtr new_col_holder;

	if (auto full_col = nested_column->convertToFullColumnIfConst())
		new_col_holder = std::make_shared<ColumnNullable>(full_col, null_map);

	return new_col_holder;
}


void ColumnNullable::updateHashWithValue(size_t n, SipHash & hash) const
{
	const auto & arr = getNullMap();
	hash.update(reinterpret_cast<const char *>(&arr[n]), sizeof(arr[0]));
	if (arr[n] == 0)
		nested_column->updateHashWithValue(n, hash);
}


ColumnPtr ColumnNullable::cloneResized(size_t size) const
{
	ColumnPtr new_nested_col = nested_column->cloneResized(size);
	ColumnPtr new_null_map = getNullMapConcreteColumn().cloneResized(size);		/// TODO Completely wrong.
	return std::make_shared<ColumnNullable>(new_nested_col, new_null_map);
}


Field ColumnNullable::operator[](size_t n) const
{
	if (isNullAt(n))
		return Null();
	else
	{
		const IColumn & col = *nested_column;
		return col[n];
	}
}


void ColumnNullable::get(size_t n, Field & res) const
{
	if (isNullAt(n))
		res = Null();
	else
		nested_column->get(n, res);
}

StringRef ColumnNullable::getDataAt(size_t n) const
{
	throw Exception{"Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED};
}

void ColumnNullable::insertData(const char * pos, size_t length)
{
	throw Exception{"Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED};
}

StringRef ColumnNullable::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
	const auto & arr = getNullMap();
	static constexpr auto s = sizeof(arr[0]);

	auto pos = arena.allocContinue(s, begin);
	memcpy(pos, &arr[n], s);

	size_t nested_size = 0;

	if (arr[n] == 0)
		nested_size = nested_column->serializeValueIntoArena(n, arena, begin).size;

	return StringRef{begin, s + nested_size};
}

const char * ColumnNullable::deserializeAndInsertFromArena(const char * pos)
{
	UInt8 val = *reinterpret_cast<const UInt8 *>(pos);
	pos += sizeof(val);

	getNullMap().push_back(val);

	if (val == 0)
		pos = nested_column->deserializeAndInsertFromArena(pos);
	else
		nested_column->insertDefault();

	return pos;
}

void ColumnNullable::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
	const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(src);
	getNullMapConcreteColumn().insertRangeFrom(*nullable_col.null_map, start, length);
	nested_column->insertRangeFrom(*nullable_col.nested_column, start, length);
}

void ColumnNullable::insert(const Field & x)
{
	if (x.isNull())
	{
		nested_column->insertDefault();
		getNullMap().push_back(1);
	}
	else
	{
		nested_column->insert(x);
		getNullMap().push_back(0);
	}
}

void ColumnNullable::insertDefault()
{
	nested_column->insertDefault();
	getNullMap().push_back(1);
}

void ColumnNullable::popBack(size_t n)
{
	nested_column->popBack(n);
	getNullMapConcreteColumn().popBack(n);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
	ColumnPtr filtered_data = nested_column->filter(filt, result_size_hint);
	ColumnPtr filtered_null_map = getNullMapConcreteColumn().filter(filt, result_size_hint);
	return std::make_shared<ColumnNullable>(filtered_data, filtered_null_map);
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
	ColumnPtr permuted_data = nested_column->permute(perm, limit);
	ColumnPtr permuted_null_map = getNullMapConcreteColumn().permute(perm, limit);
	return std::make_shared<ColumnNullable>(permuted_data, permuted_null_map);
}

int ColumnNullable::compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
{
	/// NULL values share the properties of NaN values.
	/// Here the last parameter of compareAt is called null_direction_hint
	/// instead of the usual nan_direction_hint and is used to implement
	/// the ordering specified by either NULLS FIRST or NULLS LAST in the
	/// ORDER BY construction.

	const ColumnNullable & nullable_rhs = static_cast<const ColumnNullable &>(rhs_);

	bool lval_is_null = isNullAt(n);
	bool rval_is_null = nullable_rhs.isNullAt(m);

	if (unlikely(lval_is_null || rval_is_null))
	{
		if (lval_is_null && rval_is_null)
			return 0;
		else
			return lval_is_null ? null_direction_hint : -null_direction_hint;
	}

	const IColumn & nested_rhs = *(nullable_rhs.getNestedColumn());
	return nested_column->compareAt(n, m, nested_rhs, null_direction_hint);
}

void ColumnNullable::getPermutation(bool reverse, size_t limit, Permutation & res) const
{
	nested_column->getPermutation(reverse, limit, res);
	size_t s = res.size();

	/// Since we have created a permutation "res" that sorts a subset of the column values
	/// and some of these values may actually be nulls, there is no guarantee that
	/// these null values are well positioned. So we create a permutation "p" which
	/// operates on the result of "res" by moving all the null values to the required
	/// direction and leaving the order of the remaining elements unchanged.

	/// Create the permutation p.
	Permutation p;
	p.resize(s);

	size_t pos_left = 0;
	size_t pos_right = s - 1;

	if (reverse)
	{
		/// Move the null elements to the right.
		for (size_t i = 0; i < s; ++i)
		{
			if (isNullAt(res[i]))
			{
				p[i] = pos_right;
				--pos_right;
			}
			else
			{
				p[i] = pos_left;
				++pos_left;
			}
		}
	}
	else
	{
		/// Move the null elements to the left.
		for (size_t i = 0; i < s; ++i)
		{
			size_t j = s - i - 1;

			if (isNullAt(res[j]))
			{
				p[j] = pos_left;
				++pos_left;
			}
			else
			{
				p[j] = pos_right;
				--pos_right;
			}
		}
	}

	/// Combine the permutations res and p.
	Permutation res2;
	res2.resize(s);

	for (size_t i = 0; i < s; ++i)
		res2[i] = res[p[i]];

	res = std::move(res2);
}

void ColumnNullable::reserve(size_t n)
{
	nested_column->reserve(n);
	getNullMap().reserve(n);
}

size_t ColumnNullable::byteSize() const
{
	return nested_column->byteSize() + getNullMapConcreteColumn().byteSize();
}

namespace
{

/// The following function implements a slightly more general version
/// of getExtremes() than the implementation from ColumnVector.
/// It takes into account the possible presence of nullable values.
template <typename T>
void getExtremesFromNullableContent(const ColumnVector<T> & col, const NullValuesByteMap & null_map, Field & min, Field & max)
{
	const auto & data = col.getData();
	size_t size = data.size();

	if (size == 0)
	{
		min = Null();
		max = Null();
		return;
	}

	bool has_not_null = false;
	bool has_not_nan = false;

	T cur_min = 0;
	T cur_max = 0;

	for (size_t i = 0; i < size; ++i)
	{
		const T x = data[i];

		if (null_map[i])
			continue;

		if (!has_not_null)
		{
			cur_min = x;
			cur_max = x;
			has_not_null = true;
			continue;
		}

		if (isNaN(x))
			continue;

		if (!has_not_nan)
		{
			cur_min = x;
			cur_max = x;
			has_not_nan = true;
			continue;
		}

		if (x < cur_min)
			cur_min = x;

		if (x > cur_max)
			cur_max = x;
	}

	if (has_not_null)
	{
		min = typename NearestFieldType<T>::Type(cur_min);
		max = typename NearestFieldType<T>::Type(cur_max);
	}
}

}


void ColumnNullable::getExtremes(Field & min, Field & max) const
{
	min = Null();
	max = Null();

	const auto & null_map = getNullMap();

	if (const auto col = typeid_cast<const ColumnInt8 *>(nested_column.get()))
		getExtremesFromNullableContent<Int8>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnInt16 *>(nested_column.get()))
		getExtremesFromNullableContent<Int16>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnInt32 *>(nested_column.get()))
		getExtremesFromNullableContent<Int32>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnInt64 *>(nested_column.get()))
		getExtremesFromNullableContent<Int64>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnUInt8 *>(nested_column.get()))
		getExtremesFromNullableContent<UInt8>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnUInt16 *>(nested_column.get()))
		getExtremesFromNullableContent<UInt16>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnUInt32 *>(nested_column.get()))
		getExtremesFromNullableContent<UInt32>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnUInt64 *>(nested_column.get()))
		getExtremesFromNullableContent<UInt64>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnFloat32 *>(nested_column.get()))
		getExtremesFromNullableContent<Float32>(*col, null_map, min, max);
	else if (const auto col = typeid_cast<const ColumnFloat64 *>(nested_column.get()))
		getExtremesFromNullableContent<Float64>(*col, null_map, min, max);
}


ColumnPtr ColumnNullable::replicate(const Offsets_t & offsets) const
{
	ColumnPtr replicated_data = nested_column->replicate(offsets);
	ColumnPtr replicated_null_map = getNullMapConcreteColumn().replicate(offsets);
	return std::make_shared<ColumnNullable>(replicated_data, replicated_null_map);
}


void ColumnNullable::applyNullValuesByteMap(const ColumnNullable & other)
{
	NullValuesByteMap & arr1 = getNullMap();
	const NullValuesByteMap & arr2 = other.getNullMap();

	if (arr1.size() != arr2.size())
		throw Exception{"Inconsistent sizes of ColumnNullable objects", ErrorCodes::LOGICAL_ERROR};

	for (size_t i = 0; i < arr1.size(); ++i)
		arr1[i] |= arr2[i];
}

}
