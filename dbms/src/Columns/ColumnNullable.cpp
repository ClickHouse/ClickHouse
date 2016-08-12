#include <DB/Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

ColumnNullable::ColumnNullable(ColumnPtr nested_column_)
	: nested_column{nested_column_}
{
	if (nested_column->isNullable())
		throw Exception{"A nullable column cannot contain another nullable column", ErrorCodes::LOGICAL_ERROR};
}

ColumnNullable::ColumnNullable(ColumnPtr nested_column_, bool fill_with_nulls)
	: nested_column{nested_column_},
	null_map{std::make_shared<ColumnUInt8>()}
{
	if (nested_column->isNullable())
		throw Exception{"A nullable column cannot contain another nullable column", ErrorCodes::LOGICAL_ERROR};

	size_t n = nested_column->size();
	if (n > 0)
		getNullMapContent().getData().resize_fill(n, (fill_with_nulls ? 1 : 0));
}

std::string ColumnNullable::getName() const
{
	return "ColumnNullable(" + nested_column->getName() + ")";
}

bool ColumnNullable::isNumeric() const
{
	return nested_column->isNumeric();
}

bool ColumnNullable::isConst() const
{
	return nested_column->isConst();
}

bool ColumnNullable::isFixed() const
{
	return nested_column->isFixed();
}

bool ColumnNullable::isNullable() const
{
	return true;
}

ColumnPtr ColumnNullable::convertToFullColumnIfConst() const
{
	ColumnPtr new_col_holder;

	if (auto full_col = nested_column->convertToFullColumnIfConst())
	{
		new_col_holder = std::make_shared<ColumnNullable>(full_col);
		ColumnNullable & new_col = static_cast<ColumnNullable &>(*new_col_holder);

		if (!getNullMapContent().empty())
			new_col.null_map = null_map;
		else
		{
			size_t n = nested_column->size();
			if (n > 0)
			{
				new_col.null_map = std::make_shared<ColumnUInt8>();
				new_col.getNullMapContent().getData().resize_fill(n, 0);
			}
		}
	}
	else
		new_col_holder = {};

	return new_col_holder;
}

void ColumnNullable::updateHashWithValue(size_t n, SipHash & hash) const
{
	if (isNullAt(n))
	{
		UInt8 tag = 1;
		hash.update(reinterpret_cast<const char *>(&tag), sizeof(tag));
	}
	else
	{
		UInt8 tag = 0;
		hash.update(reinterpret_cast<const char *>(&tag), sizeof(tag));
		nested_column->updateHashWithValue(n, hash);
	}
}

ColumnPtr ColumnNullable::cloneResized(size_t size) const
{
	ColumnPtr new_col_holder = std::make_shared<ColumnNullable>(nested_column->cloneResized(size));
	auto & new_col = static_cast<ColumnNullable &>(*new_col_holder);

	/// Create a new null byte map for the cloned column.
	/// Resize it if required.
	new_col.null_map = null_map.get()->clone();
	if (size != this->size())
		new_col.getNullMapContent().getData().resize_fill(size, 0);

	return new_col_holder;
}

ColumnPtr ColumnNullable::cloneEmpty() const
{
	ColumnPtr new_col_holder = std::make_shared<ColumnNullable>(nested_column->cloneEmpty());
	auto & new_col = static_cast<ColumnNullable &>(*new_col_holder);
	new_col.null_map = null_map.get()->cloneEmpty();
	return new_col_holder;
}

size_t ColumnNullable::size() const
{
	return nested_column->size();
}

Field ColumnNullable::operator[](size_t n) const
{
	if (isNullAt(n))
		return Field{};
	else
	{
		const IColumn & col = *nested_column;
		return col[n];
	}
}

void ColumnNullable::get(size_t n, Field & res) const
{
	if (isNullAt(n))
		res = Field{};
	else
		nested_column->get(n, res);
}

UInt64 ColumnNullable::get64(size_t n) const
{
	throw Exception{"Method get64 is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED};
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
	if (isNullAt(n))
	{
		const UInt8 tag = 1;
		auto pos = arena.allocContinue(sizeof(tag), begin);
		memcpy(pos, &tag, sizeof(tag));
		return StringRef{pos, sizeof(tag)};
	}
	else
	{
		const UInt8 tag = 0;
		auto pos = arena.allocContinue(sizeof(tag), begin);
		memcpy(pos, &tag, sizeof(tag));
		return nested_column->serializeValueIntoArena(n, arena, begin);
	}
}

const char * ColumnNullable::deserializeAndInsertFromArena(const char * pos)
{
	UInt8 tag = *reinterpret_cast<const UInt8 *>(pos);
	const auto next_pos = pos + sizeof(tag);
	if (tag == 1)
	{
		/// Apppend a null.
		getNullMapContent().insert(1);
		return next_pos;
	}
	else
	{
		getNullMapContent().insert(0);
		return nested_column->deserializeAndInsertFromArena(pos + sizeof(next_pos));
	}
}

void ColumnNullable::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
	if (length == 0)
		return;

	const ColumnNullable & concrete_src = static_cast<const ColumnNullable &>(src);

	if (start > (std::numeric_limits<size_t>::max() - length))
		throw Exception{"ColumnNullable: overflow", ErrorCodes::LOGICAL_ERROR};

	if ((start + length) > concrete_src.size())
		throw Exception{"Parameter out of bound in ColumNullable::insertRangeFrom method.",
			ErrorCodes::PARAMETER_OUT_OF_BOUND};

	getNullMapContent().insertRangeFrom(*concrete_src.null_map, start, length);
	nested_column->insertRangeFrom(*concrete_src.nested_column, start, length);
}

void ColumnNullable::insert(const Field & x)
{
	if (x.isNull())
	{
		nested_column->insertDefault();
		getNullMapContent().insert(1);
	}
	else
	{
		nested_column->insert(x);
		getNullMapContent().insert(0);
	}
}

void ColumnNullable::insertDefault()
{
	nested_column->insertDefault();
	getNullMapContent().insert(0);
}

void ColumnNullable::popBack(size_t n)
{
	nested_column->popBack(n);
	getNullMapContent().popBack(n);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
	ColumnPtr new_data = nested_column->filter(filt, result_size_hint);
	ColumnPtr filtered_col_holder = std::make_shared<ColumnNullable>(new_data);
	ColumnNullable & filtered_col = static_cast<ColumnNullable &>(*filtered_col_holder);
	filtered_col.null_map = getNullMapContent().filter(filt, result_size_hint);
	return filtered_col_holder;
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
	ColumnPtr new_data = nested_column->permute(perm, limit);
	ColumnPtr permuted_col_holder = std::make_shared<ColumnNullable>(new_data);
	ColumnNullable & permuted_col = static_cast<ColumnNullable &>(*permuted_col_holder);
	permuted_col.null_map = getNullMapContent().permute(perm, limit);
	return permuted_col_holder;
}

int ColumnNullable::compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
{
	const ColumnNullable & nullable_rhs = static_cast<const ColumnNullable &>(rhs_);

	bool lval_is_null = isNullAt(n);
	bool rval_is_null = nullable_rhs.isNullAt(m);

	/// Two null values are equal.
	if (lval_is_null && rval_is_null)
		return 0;
	/// The null value is always lesser than any value.
	if (lval_is_null)
		return -1;
	/// A non-null value is always greater than the null value.
	if (rval_is_null)
		return 1;

	const IColumn & nested_rhs = *(nullable_rhs.getNestedColumn());
	return nested_column->compareAt(n, m, nested_rhs, nan_direction_hint);
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
	getNullMapContent().reserve(n);
}

size_t ColumnNullable::byteSize() const
{
	return nested_column->byteSize() + getNullMapContent().byteSize();
}

void ColumnNullable::getExtremes(Field & min, Field & max) const
{
	if (auto col = typeid_cast<ColumnInt8 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnInt16 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnInt32 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnInt64 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnUInt8 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnUInt16 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnUInt32 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnUInt64 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnFloat32 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else if (auto col = typeid_cast<ColumnFloat64 *>(nested_column.get()))
		col->getExtremesFromNullableContent(min, max, &getNullMapContent().getData());
	else
		nested_column->getExtremes(min, max);
}

ColumnPtr ColumnNullable::replicate(const Offsets_t & offsets) const
{
	ColumnPtr replicated_col_holder = std::make_shared<ColumnNullable>(nested_column->replicate(offsets));
	ColumnNullable & replicated_col = static_cast<ColumnNullable &>(*replicated_col_holder);
	replicated_col.null_map = getNullMapContent().replicate(offsets);
	return replicated_col_holder;
}

ColumnPtr & ColumnNullable::getNestedColumn()
{
	return nested_column;
}

const ColumnPtr & ColumnNullable::getNestedColumn() const
{
	return nested_column;
}

ColumnPtr & ColumnNullable::getNullValuesByteMap()
{
	return null_map;
}

const ColumnPtr & ColumnNullable::getNullValuesByteMap() const
{
	return null_map;
}

void ColumnNullable::updateNullValuesByteMap(const ColumnNullable & other)
{
	NullValuesByteMap & arr1 = getNullMapContent().getData();
	const NullValuesByteMap & arr2 = other.getNullMapContent().getData();

	if (arr1.size() != arr2.size())
		throw Exception{"Inconsistent sizes", ErrorCodes::LOGICAL_ERROR};

	for (size_t i = 0; i < arr1.size(); ++i)
		arr1[i] |= arr2[i];
}

bool ColumnNullable::isNullAt(size_t n) const
{
	auto & arr = getNullMapContent().getData();
	return arr[n] != 0;
}

ColumnUInt8 & ColumnNullable::getNullMapContent()
{
	return static_cast<ColumnUInt8 &>(*null_map);
}

const ColumnUInt8 & ColumnNullable::getNullMapContent() const
{
	return static_cast<const ColumnUInt8 &>(*null_map);
}

}
