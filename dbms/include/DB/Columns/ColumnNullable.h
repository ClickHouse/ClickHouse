#pragma once

#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsNumber.h>

namespace DB
{

class ColumnNullable final : public IColumn
{
public:
	ColumnNullable(ColumnPtr nested_column_);
	std::string getName() const override;
	bool isNumeric() const override;
	bool isConst() const override;
	bool isFixed() const override;
	bool isNullable() const override;
	size_t sizeOfField() const override;
	ColumnPtr cloneResized(size_t size) const override;
	ColumnPtr cloneEmpty() const override;
	size_t size() const override;
	bool isNullAt(size_t n) const;
	Field operator[](size_t n) const override;
	void get(size_t n, Field & res) const override;
	UInt64 get64(size_t n) const override;
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
	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
	void getPermutation(bool reverse, size_t limit, Permutation & res) const override;
	void reserve(size_t n) override;
	size_t byteSize() const override;
	ColumnPtr replicate(const Offsets_t & replicate_offsets) const override;
	ColumnPtr convertToFullColumnIfConst() const override;
	void updateHashWithValue(size_t n, SipHash & hash) const override;

	ColumnPtr & getNestedColumn();
	const ColumnPtr & getNestedColumn() const;
	ColumnPtr & getNullValuesByteMap();
	const ColumnPtr & getNullValuesByteMap() const;

	void updateNullValuesByteMap(const ColumnNullable & other);

private:
	void getExtremesImpl(Field & min, Field & max, const NullValuesByteMap * null_map_) const override;
	ColumnUInt8 & getNullMapContent();
	const ColumnUInt8 & getNullMapContent() const;

private:
	ColumnPtr nested_column;
	ColumnPtr null_map;
};

}
