#include <Columns/ColumnConstAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>


namespace DB
{

ColumnPtr ColumnConstAggregateFunction::convertToFullColumnIfConst() const
{
    return convertToFullColumn();
}

ColumnPtr ColumnConstAggregateFunction::convertToFullColumn() const
{
    auto res = std::make_shared<ColumnAggregateFunction>(getAggregateFunction());

    for (size_t i = 0; i < s; ++i)
        res->insert(value);

    return res;
}

ColumnPtr ColumnConstAggregateFunction::cloneResized(size_t new_size) const
{
    return std::make_shared<ColumnConstAggregateFunction>(new_size, value, data_type);
}

StringRef ColumnConstAggregateFunction::getDataAt(size_t n) const
{
    return value.get<const String &>();
}

void ColumnConstAggregateFunction::insert(const Field & x)
{
    /// NOTE: Cannot check source function of x
    if (value != x)
        throw Exception(
            "Cannot insert different element into constant column " + getName(), ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
    ++s;
}

void ColumnConstAggregateFunction::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (!equalsFuncAndValue(src))
        throw Exception(
            "Cannot insert different element into constant column " + getName(), ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);

    s += length;
}

void ColumnConstAggregateFunction::insertData(const char * pos, size_t length)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

StringRef ColumnConstAggregateFunction::serializeValueIntoArena(size_t n, Arena & arena, const char *& begin) const
{
    throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

const char * ColumnConstAggregateFunction::deserializeAndInsertFromArena(const char * pos)
{
    throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnConstAggregateFunction::updateHashWithValue(size_t n, SipHash & hash) const
{
    throw Exception("Method updateHashWithValue is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

ColumnPtr ColumnConstAggregateFunction::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    if (s != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return std::make_shared<ColumnConstAggregateFunction>(countBytesInFilter(filt), value, data_type);
}

ColumnPtr ColumnConstAggregateFunction::permute(const IColumn::Permutation & perm, size_t limit) const
{
    if (limit == 0)
        limit = s;
    else
        limit = std::min(s, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return std::make_shared<ColumnConstAggregateFunction>(limit, value, data_type);
}

void ColumnConstAggregateFunction::getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

ColumnPtr ColumnConstAggregateFunction::replicate(const IColumn::Offsets_t & offsets) const
{
    if (s != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    size_t replicated_size = 0 == s ? 0 : offsets.back();
    return std::make_shared<ColumnConstAggregateFunction>(replicated_size, value, data_type);
}

void ColumnConstAggregateFunction::getExtremes(Field & min, Field & max) const
{
    min = value;
    max = value;
}

size_t ColumnConstAggregateFunction::byteSize() const
{
    return sizeof(value) + sizeof(s);
}

size_t ColumnConstAggregateFunction::allocatedSize() const
{
    return byteSize();
}

AggregateFunctionPtr ColumnConstAggregateFunction::getAggregateFunction() const
{
    return typeid_cast<const DataTypeAggregateFunction &>(*data_type).getFunction();
}

bool ColumnConstAggregateFunction::equalsFuncAndValue(const IColumn & rhs) const
{
    auto rhs_const = dynamic_cast<const ColumnConstAggregateFunction *>(&rhs);
    return rhs_const && value == rhs_const->value && data_type->equals(*rhs_const->data_type);
}

}
