#include <DB/Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

ColumnNullable::ColumnNullable(ColumnPtr nested_column)
	: data_holder{nested_column},
	data{*(data_holder.get())},
	null_map_holder{std::make_shared<ColumnUInt8>()},
	null_map{static_cast<ColumnUInt8 &>(*null_map_holder.get())}
{
	if (data.isNullable())
		throw Exception{"A nullable column cannot contain another nullable column", ErrorCodes::LOGICAL_ERROR};

	size_t n = data.size();
	if (n > 0)
		null_map.getData().resize_fill(n, 0);
}

std::string ColumnNullable::getName() const
{
	return "ColumnNullable(" + data.getName() + ")";
}

bool ColumnNullable::isNumeric() const
{
	return data.isNumeric();
}

bool ColumnNullable::isConst() const
{
	return data.isConst();
}

bool ColumnNullable::isFixed() const
{
	return data.isFixed();
}

bool ColumnNullable::isNullable() const
{
	return true;
}

size_t ColumnNullable::sizeOfField() const
{
	return data.sizeOfField();
}

ColumnPtr ColumnNullable::convertToFullColumnIfConst() const
{
	ColumnPtr new_col_holder;

	if (auto full_col = data.convertToFullColumnIfConst())
	{
		new_col_holder = std::make_shared<ColumnNullable>(full_col);
		ColumnNullable & new_col = static_cast<ColumnNullable &>(*new_col_holder.get());

		/// Create a byte map.
		if (new_col[0].isNull())
			new_col.null_map_holder = std::make_shared<ColumnUInt8>(new_col.size(), 1);
		else
			new_col.null_map_holder = std::make_shared<ColumnUInt8>(new_col.size(), 0);
	}
	else
		new_col_holder = {};

	return new_col_holder;
}

ColumnPtr ColumnNullable::cloneResized(size_t size) const
{
	return std::make_shared<ColumnNullable>(data.cloneResized(size));
}

ColumnPtr ColumnNullable::cloneEmpty() const
{
	return std::make_shared<ColumnNullable>(data.cloneEmpty());
}

size_t ColumnNullable::size() const
{
	return data.size();
}

Field ColumnNullable::operator[](size_t n) const
{
	if (isNullAt(n))
		return Field{};
	else
		return data[n];
}

void ColumnNullable::get(size_t n, Field & res) const
{
	if (isNullAt(n))
		res = Field{};
	else
		data.get(n, res);
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
		return data.getDataAt(n);
}

void ColumnNullable::insertData(const char * pos, size_t length)
{
	data.insertData(pos, length);
	null_map.getData().resize_fill(data.size(), 0);
}

StringRef ColumnNullable::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
	return data.serializeValueIntoArena(n, arena, begin);
}

const char * ColumnNullable::deserializeAndInsertFromArena(const char * pos)
{
	return data.deserializeAndInsertFromArena(pos);
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

	null_map.insertRangeFrom(*(concrete_src.null_map_holder.get()), start, length);
	data.insertRangeFrom(concrete_src.data, start, length);
}

void ColumnNullable::insert(const Field & x)
{
	if (x.isNull())
	{
		data.insertDefault();
		null_map.insert(1);
	}
	else
	{
		data.insert(x);
		null_map.insert(0);
	}
}

void ColumnNullable::insertDefault()
{
	data.insertDefault();
	null_map.insert(0);
}

void ColumnNullable::popBack(size_t n)
{
	data.popBack(n);
	null_map.popBack(n);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
	ColumnPtr new_data = data.filter(filt, result_size_hint);
	ColumnPtr filtered_col_holder = std::make_shared<ColumnNullable>(new_data);
	ColumnNullable & filtered_col = static_cast<ColumnNullable &>(*(filtered_col_holder.get()));
	filtered_col.null_map_holder = null_map.filter(filt, result_size_hint);
	return filtered_col_holder;
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
	ColumnPtr new_data = data.permute(perm, limit);
	ColumnPtr permuted_col_holder = std::make_shared<ColumnNullable>(new_data);
	ColumnNullable & permuted_col = static_cast<ColumnNullable &>(*permuted_col_holder);
	permuted_col.null_map_holder = null_map.permute(perm, limit);
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

	return data.compareAt(n, m, nested_rhs, nan_direction_hint);
}

void ColumnNullable::getPermutation(bool reverse, size_t limit, Permutation & res) const
{
	data.getPermutation(reverse, limit, res);
}

void ColumnNullable::reserve(size_t n)
{
	data.reserve(n);
	null_map.reserve(n);
}

size_t ColumnNullable::byteSize() const
{
	return data.byteSize() + null_map.byteSize();
}

void ColumnNullable::getExtremesImpl(Field & min, Field & max, const NullValuesByteMap * null_map_) const
{
	return data.getExtremes(min, max, &(null_map.getData()));
}

ColumnPtr ColumnNullable::replicate(const Offsets_t & offsets) const
{
	ColumnPtr replicated_col_holder = std::make_shared<ColumnNullable>(data.replicate(offsets));
	ColumnNullable & replicated_col = static_cast<ColumnNullable &>(*replicated_col_holder);
	replicated_col.null_map_holder = null_map.replicate(offsets);
	return replicated_col_holder;
}

ColumnPtr & ColumnNullable::getNestedColumn()
{
	return data_holder;
}

const ColumnPtr & ColumnNullable::getNestedColumn() const
{
	return data_holder;
}

ColumnPtr & ColumnNullable::getNullValuesByteMap()
{
	return null_map_holder;
}

const ColumnPtr & ColumnNullable::getNullValuesByteMap() const
{
	return null_map_holder;
}

void ColumnNullable::updateNullValuesByteMap(const ColumnNullable & other)
{
	NullValuesByteMap & arr1 = null_map.getData();
	const NullValuesByteMap & arr2 = other.null_map.getData();

	if (arr1.size() != arr2.size())
		throw Exception{"Inconsistent sizes", ErrorCodes::LOGICAL_ERROR};

	for (size_t i = 0; i < arr1.size(); ++i)
		arr1[i] |= arr2[i];
}

bool ColumnNullable::isNullAt(size_t n) const
{
	auto & arr = null_map.getData();
	return arr[n] != 0;
}

}
