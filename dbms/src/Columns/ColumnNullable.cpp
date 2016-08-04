#include <DB/Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

ColumnNullable::ColumnNullable(ColumnPtr nested_column_, bool fill_with_nulls)
	: nested_column{nested_column_},
	null_map{std::make_shared<ColumnUInt8>()}
{
	if (nested_column.get()->isNullable())
		throw Exception{"A nullable column cannot contain another nullable column", ErrorCodes::LOGICAL_ERROR};

	size_t n = nested_column.get()->size();
	if (n > 0)
		getNullMapContent().getData().resize_fill(n, (fill_with_nulls ? 1 : 0));
}

std::string ColumnNullable::getName() const
{
	return "ColumnNullable(" + nested_column.get()->getName() + ")";
}

bool ColumnNullable::isNumeric() const
{
	return nested_column.get()->isNumeric();
}

bool ColumnNullable::isConst() const
{
	return nested_column.get()->isConst();
}

bool ColumnNullable::isFixed() const
{
	return nested_column.get()->isFixed();
}

bool ColumnNullable::isNullable() const
{
	return true;
}

size_t ColumnNullable::sizeOfField() const
{
	return nested_column.get()->sizeOfField();
}

ColumnPtr ColumnNullable::convertToFullColumnIfConst() const
{
	ColumnPtr new_col_holder;

	if (auto full_col = nested_column.get()->convertToFullColumnIfConst())
	{
		new_col_holder = std::make_shared<ColumnNullable>(full_col);
		ColumnNullable & new_col = static_cast<ColumnNullable &>(*new_col_holder.get());

		if (!getNullMapContent().empty())
			new_col.null_map = null_map.get()->clone();
	}
	else
		new_col_holder = {};

	return new_col_holder;
}

void ColumnNullable::updateHashWithValue(size_t n, SipHash & hash) const
{
	nested_column.get()->updateHashWithValue(n, hash);
}

ColumnPtr ColumnNullable::cloneResized(size_t size) const
{
	ColumnPtr new_col_holder = std::make_shared<ColumnNullable>(nested_column.get()->cloneResized(size));
	auto & new_col = static_cast<ColumnNullable &>(*(new_col_holder.get()));
	new_col.null_map = null_map.get()->clone();
	new_col.getNullMapContent().getData().resize_fill(size);

	return new_col_holder;
}

ColumnPtr ColumnNullable::cloneEmpty() const
{
	return std::make_shared<ColumnNullable>(nested_column.get()->cloneEmpty());
}

size_t ColumnNullable::size() const
{
	return nested_column.get()->size();
}

Field ColumnNullable::operator[](size_t n) const
{
	if (isNullAt(n))
		return Field{};
	else
	{
		const IColumn & col = *(nested_column.get());
		return col[n];
	}
}

void ColumnNullable::get(size_t n, Field & res) const
{
	if (isNullAt(n))
		res = Field{};
	else
		nested_column.get()->get(n, res);
}

UInt64 ColumnNullable::get64(size_t n) const
{
	throw Exception{"Unsupported method", ErrorCodes::LOGICAL_ERROR};
}

StringRef ColumnNullable::getDataAt(size_t n) const
{
	if (isNullAt(n))
		return StringRef{};
	else
		return nested_column.get()->getDataAt(n);
}

void ColumnNullable::insertData(const char * pos, size_t length)
{
	nested_column.get()->insertData(pos, length);
	getNullMapContent().getData().resize_fill(nested_column.get()->size(), 0);
}

StringRef ColumnNullable::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
	return nested_column.get()->serializeValueIntoArena(n, arena, begin);
}

const char * ColumnNullable::deserializeAndInsertFromArena(const char * pos)
{
	return nested_column.get()->deserializeAndInsertFromArena(pos);
}

void ColumnNullable::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
	if (length == 0)
		return;

	const ColumnNullable & concrete_src = static_cast<const ColumnNullable &>(src);

	if (start > (std::numeric_limits<size_t>::max() -  length))
		throw Exception{"ColumnNullable: overflow", ErrorCodes::LOGICAL_ERROR};

	if ((start + length) > concrete_src.size())
		throw Exception{"Parameter out of bound in ColumNullable::insertRangeFrom method.",
			ErrorCodes::PARAMETER_OUT_OF_BOUND};

	getNullMapContent().insertRangeFrom(*(concrete_src.null_map.get()), start, length);
	nested_column.get()->insertRangeFrom(*(concrete_src.nested_column.get()), start, length);
}

void ColumnNullable::insert(const Field & x)
{
	if (x.isNull())
	{
		nested_column.get()->insertDefault();
		getNullMapContent().insert(1);
	}
	else
	{
		nested_column.get()->insert(x);
		getNullMapContent().insert(0);
	}
}

void ColumnNullable::insertDefault()
{
	nested_column.get()->insertDefault();
	getNullMapContent().insert(0);
}

void ColumnNullable::popBack(size_t n)
{
	nested_column.get()->popBack(n);
	getNullMapContent().popBack(n);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
	ColumnPtr new_data = nested_column.get()->filter(filt, result_size_hint);
	ColumnPtr filtered_col_holder = std::make_shared<ColumnNullable>(new_data);
	ColumnNullable & filtered_col = static_cast<ColumnNullable &>(*(filtered_col_holder.get()));
	filtered_col.null_map = getNullMapContent().filter(filt, result_size_hint);
	return filtered_col_holder;
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
	ColumnPtr new_data = nested_column.get()->permute(perm, limit);
	ColumnPtr permuted_col_holder = std::make_shared<ColumnNullable>(new_data);
	ColumnNullable & permuted_col = static_cast<ColumnNullable &>(*permuted_col_holder);
	permuted_col.null_map = getNullMapContent().permute(perm, limit);
	return permuted_col_holder;
}

int ColumnNullable::compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
{
	Field lval;
	get(n, lval);

	Field rval;
	rhs_.get(m, rval);

	/// Two NULL values are equal.
	if (lval.isNull() && rval.isNull())
		return 0;
	/// The NULL value is always lesser than any value.
	if (lval.isNull())
		return -1;
	if (rval.isNull())
		return 1;

	const ColumnNullable & nullable_rhs = static_cast<const ColumnNullable &>(rhs_);
	const IColumn & nested_rhs = *(nullable_rhs.getNestedColumn().get());

	return nested_column.get()->compareAt(n, m, nested_rhs, nan_direction_hint);
}

void ColumnNullable::getPermutation(bool reverse, size_t limit, Permutation & res) const
{
	nested_column.get()->getPermutation(reverse, limit, res);
}

void ColumnNullable::reserve(size_t n)
{
	nested_column.get()->reserve(n);
	getNullMapContent().reserve(n);
}

size_t ColumnNullable::byteSize() const
{
	return nested_column.get()->byteSize() + getNullMapContent().byteSize();
}

void ColumnNullable::getExtremesImpl(Field & min, Field & max, const NullValuesByteMap * null_map_) const
{
	return nested_column.get()->getExtremes(min, max, &(getNullMapContent().getData()));
}

ColumnPtr ColumnNullable::replicate(const Offsets_t & offsets) const
{
	ColumnPtr replicated_col_holder = std::make_shared<ColumnNullable>(nested_column.get()->replicate(offsets));
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
	return static_cast<ColumnUInt8 &>(*(null_map.get()));
}

const ColumnUInt8 & ColumnNullable::getNullMapContent() const
{
	return static_cast<const ColumnUInt8 &>(*(null_map.get()));
}

}
