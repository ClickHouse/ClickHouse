#include <DB/Columns/ColumnNull.h>
#include <DB/Common/Arena.h>

namespace DB
{

namespace ErrorCodes
{

extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;

}

ColumnNull::ColumnNull(size_t size_)
	: count{size_}
{
}

std::string ColumnNull::getName() const
{
	return "ColumnNull";
}

bool ColumnNull::isConst() const
{
	return true;
}

ColumnPtr ColumnNull::convertToFullColumnIfConst() const
{
	return std::make_shared<ColumnNull>(count);
}

size_t ColumnNull::sizeOfField() const
{
	return sizeof(UInt8);
}

ColumnPtr ColumnNull::cloneResized(size_t size) const
{
	return std::make_shared<ColumnNull>(size);
}

size_t ColumnNull::size() const
{
	return count;	
}

Field ColumnNull::operator[](size_t n) const
{
	return Field{};
}

void ColumnNull::get(size_t n, Field & res) const
{
	res = Field{};	
}

StringRef ColumnNull::getDataAt(size_t n) const
{
	return StringRef{};
}

void ColumnNull::insert(const Field & x)
{
	++count;
}

void ColumnNull::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
	count += length;
}

void ColumnNull::insertData(const char * pos, size_t length)
{
	++count;	
}

void ColumnNull::insertDefault()
{
	++count;
}

void ColumnNull::popBack(size_t n)
{
	count -= n;
}

StringRef ColumnNull::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
	auto pos = arena.allocContinue(sizeof(UInt8), begin);
	UInt8 val = 0;
	memcpy(pos, &val, sizeof(UInt8));
	return StringRef(pos, sizeof(UInt8));
}

const char * ColumnNull::deserializeAndInsertFromArena(const char * pos)
{
	++count;
	return pos + sizeof(UInt8);
}

ColumnPtr ColumnNull::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
	return std::make_shared<ColumnNull>(count);
}

ColumnPtr ColumnNull::permute(const IColumn::Permutation & perm, size_t limit) const
{
	return std::make_shared<ColumnNull>(count);
}

int ColumnNull::compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const 
{
	return -1;
}

void ColumnNull::getPermutation(bool reverse, size_t limit, Permutation & res) const
{
	res.resize(count);
	for (size_t i = 0; i < count; ++i)
		res[i] = i;
}

ColumnPtr ColumnNull::replicate(const IColumn::Offsets_t & offsets) const
{
	if (offsets.size() != count)
		throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	if (count == 0)
		return std::make_shared<ColumnNull>();

	return std::make_shared<ColumnNull>(offsets.back());
}

size_t ColumnNull::byteSize() const
{
	return count * sizeof(UInt8);
}

void ColumnNull::getExtremesImpl(Field & min, Field & max, const NullValuesByteMap * null_map_) const
{
	min = Field{};
	max = Field{};
}

}
