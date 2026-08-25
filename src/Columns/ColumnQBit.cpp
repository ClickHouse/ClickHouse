#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/Serializations/SerializationQBit.h>
#include <IO/Operators.h>


namespace DB
{


ColumnQBit::ColumnQBit(MutableColumnPtr && tuple_, size_t dimension_)
    : tuple(std::move(tuple_))
    , dimension(dimension_)
{
}

Field ColumnQBit::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

MutableColumnPtr ColumnQBit::cloneResized(size_t new_size) const
{
    return ColumnQBit::create(tuple->cloneResized(new_size), dimension);
}

size_t ColumnQBit::getBitsCount() const
{
    return assert_cast<const ColumnTuple *>(tuple.get())->getColumns().size();
}

std::string ColumnQBit::getName() const
{
    return fmt::format("QBit({}, {})", getBitsCount() == 16 ? "BFloat16" : getBitsCount() == 32 ? "Float32" : "Float64", dimension);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnQBit::insertFrom(const IColumn & src_, size_t n)
{
    tuple->insertFrom(assert_cast<const ColumnQBit &>(src_).getTupleColumn(), n);
}

void ColumnQBit::insertManyFrom(const IColumn & src, size_t position, size_t length)
{
    tuple->insertManyFrom(assert_cast<const ColumnQBit &>(src).getTupleColumn(), position, length);
}

void ColumnQBit::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    tuple->insertRangeFrom(assert_cast<const ColumnQBit &>(src).getTupleColumn(), start, length);
}
#else
void ColumnQBit::doInsertFrom(const IColumn & src_, size_t n)
{
    tuple->insertFrom(assert_cast<const ColumnQBit &>(src_).getTupleColumn(), n);
}

void ColumnQBit::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
{
    tuple->insertManyFrom(assert_cast<const ColumnQBit &>(src).getTupleColumn(), position, length);
}

void ColumnQBit::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    tuple->insertRangeFrom(assert_cast<const ColumnQBit &>(src).getTupleColumn(), start, length);
}
#endif

void ColumnQBit::getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const
{
    if (options.notFull(name_buf))
    {
        name_buf << "qbit(";
        tuple->getValueNameImpl(name_buf, n, options);
        name_buf << ")";
    }
}

void ColumnQBit::get(size_t n, Field & res) const
{
    tuple->get(n, res);
}

ColumnPtr ColumnQBit::filter(const Filter & filt, ssize_t result_size_hint) const
{
    return ColumnQBit::create(tuple->filter(filt, result_size_hint), dimension);
}

void ColumnQBit::filter(const Filter & filt)
{
    tuple->filter(filt);
}

void ColumnQBit::expand(const Filter & mask, bool inverted)
{
    tuple->expand(mask, inverted);
}

ColumnPtr ColumnQBit::permute(const Permutation & perm, size_t limit) const
{
    return ColumnQBit::create(tuple->permute(perm, limit), dimension);
}

ColumnPtr ColumnQBit::index(const IColumn & indexes, size_t limit) const
{
    return ColumnQBit::create(tuple->index(indexes, limit), dimension);
}

ColumnPtr ColumnQBit::replicate(const Offsets & offsets) const
{
    return ColumnQBit::create(tuple->replicate(offsets), dimension);
}

ColumnPtr ColumnQBit::compress(bool force_compression) const
{
    auto compressed = tuple->compress(force_compression);
    const auto byte_size = compressed->byteSize();
    return ColumnCompressed::create(
        size(),
        byte_size,
        [my_compressed = std::move(compressed), my_dimension = dimension]
        { return ColumnQBit::create(my_compressed->decompress(), my_dimension); });
}

void ColumnQBit::forEachMutableSubcolumn(MutableColumnCallback callback)
{
    callback(tuple);
}

void ColumnQBit::forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*tuple);
    tuple->forEachMutableSubcolumnRecursively(callback);
}

void ColumnQBit::forEachSubcolumn(ColumnCallback callback) const
{
    callback(tuple);
}

void ColumnQBit::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    callback(*tuple);
    tuple->forEachSubcolumnRecursively(callback);
}

void ColumnQBit::prepareForSquashing(const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor)
{
    VectorWithMemoryTracking<ColumnPtr> source_tuple_columns;
    source_tuple_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        source_tuple_columns.push_back(assert_cast<const ColumnQBit &>(*source_column).getTuple());
    tuple->prepareForSquashing(source_tuple_columns, factor);
}

}
