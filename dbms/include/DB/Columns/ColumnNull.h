#pragma once

#include <DB/Columns/IColumn.h>

namespace DB
{

class ColumnNull final : public IColumn
{
public:
	ColumnNull() = default;
	ColumnNull(size_t size);
	std::string getName() const override;
	bool isConst() const override;
	ColumnPtr convertToFullColumnIfConst() const override;
	size_t sizeOfField() const override;
	ColumnPtr cloneResized(size_t size) const override;
	size_t size() const override;
	Field operator[](size_t n) const override;
	void get(size_t n, Field & res) const override;
	StringRef getDataAt(size_t n) const override;
	void insert(const Field & x) override;
	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
	void insertData(const char * pos, size_t length) override;
	void insertDefault() override;
	void popBack(size_t n) override;
	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
	const char * deserializeAndInsertFromArena(const char * pos) override;
	ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
	ColumnPtr permute(const Permutation & perm, size_t limit) const override;
	int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
	void getPermutation(bool reverse, size_t limit, Permutation & res) const override;
	ColumnPtr replicate(const Offsets_t & offsets) const override;
	size_t byteSize() const override;

private:
	void getExtremesImpl(Field & min, Field & max, const NullValuesByteMap * null_map_) const override;

private:
	size_t count = 0;

};

}
