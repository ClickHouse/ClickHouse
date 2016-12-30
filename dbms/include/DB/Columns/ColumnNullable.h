#pragma once

#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsNumber.h>

namespace DB
{

using NullValuesByteMap = PaddedPODArray<UInt8>;

/// Class that specifies nullable columns. A nullable column represents
/// a column, which may have any type, provided with the possibility of
/// storing NULL values. For this purpose, a ColumNullable object stores
/// an ordinary column along with a special column, namely a byte map,
/// whose type is ColumnUInt8. The latter column indicates whether the
/// value of a given row is a NULL or not. Such a design is preferred
/// over a bitmap because columns are usually stored on disk as compressed
/// files. In this regard, using a bitmap instead of a byte map would
/// greatly complicate the implementation with little to no benefits.
class ColumnNullable final : public IColumn
{
public:
	ColumnNullable(ColumnPtr nested_column_, ColumnPtr null_map_);
	std::string getName() const override { 	return "ColumnNullable(" + nested_column->getName() + ")"; }
	bool isNumeric() const override { return nested_column->isNumeric(); }
	bool isNumericNotNullable() const override { return false; }
	bool isConst() const override { return nested_column->isConst(); }
	bool isFixed() const override { return nested_column->isFixed(); }
	bool isNullable() const override { return true; }
	ColumnPtr cloneResized(size_t size) const override;
	size_t size() const override { return nested_column->size(); }
	bool isNullAt(size_t n) const { return static_cast<const ColumnUInt8 &>(*null_map).getData()[n] != 0;}
	Field operator[](size_t n) const override;
	void get(size_t n, Field & res) const override;
	UInt64 get64(size_t n) const override { return nested_column->get64(n); }
	StringRef getDataAt(size_t n) const override;
	void insertData(const char * pos, size_t length) override;
	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
	const char * deserializeAndInsertFromArena(const char * pos) override;
	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;;
	void insert(const Field & x) override;
	void insertDefault() override;
	void popBack(size_t n) override;
	ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
	ColumnPtr permute(const Permutation & perm, size_t limit) const override;
	int compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const override;
	void getPermutation(bool reverse, size_t limit, Permutation & res) const override;
	void reserve(size_t n) override;
	size_t byteSize() const override;
	ColumnPtr replicate(const Offsets_t & replicate_offsets) const override;
	ColumnPtr convertToFullColumnIfConst() const override;
	void updateHashWithValue(size_t n, SipHash & hash) const override;
	void getExtremes(Field & min, Field & max) const override;

	/// Return the column that represents values.
	ColumnPtr & getNestedColumn() { return nested_column; }
	const ColumnPtr & getNestedColumn() const { return nested_column; }

	/// Return the column that represents the byte map.
	ColumnPtr & getNullMapColumn() { return null_map; }
	const ColumnPtr & getNullMapColumn() const { return null_map; }

	ColumnUInt8 & getNullMapConcreteColumn() { return static_cast<ColumnUInt8 &>(*null_map); }
	const ColumnUInt8 & getNullMapConcreteColumn() const { return static_cast<const ColumnUInt8 &>(*null_map); }

	ColumnUInt8::Container_t & getNullMap() { return getNullMapConcreteColumn().getData(); }
	const ColumnUInt8::Container_t & getNullMap() const { return getNullMapConcreteColumn().getData(); }

	/// Apply the null byte map of a specified nullable column onto the
	/// null byte map of the current column by performing an element-wise OR
	/// between both byte maps. This method is used to determine the null byte
	/// map of the result column of a function taking one or more nullable
	/// columns.
	void applyNullValuesByteMap(const ColumnNullable & other);

private:
	ColumnPtr nested_column;
	ColumnPtr null_map;
};

}
