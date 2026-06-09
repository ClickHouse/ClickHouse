#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnDenseVector.h>
#include <Columns/ColumnFixedString.h>
#include <IO/Operators.h>


namespace DB
{


ColumnDenseVector::ColumnDenseVector(MutableColumnPtr && fixed_string_, size_t element_size_, size_t dimension_)
    : fixed_string(std::move(fixed_string_))
    , element_size(element_size_)
    , dimension(dimension_)
{
}

MutableColumnPtr ColumnDenseVector::cloneResized(size_t new_size) const
{
    return ColumnDenseVector::create(fixed_string->cloneResized(new_size), element_size, dimension);
}

std::string ColumnDenseVector::getName() const
{
    const char * element_type_name = element_size == 2 ? "BFloat16" : element_size == 4 ? "Float32" : "Float64";
    return fmt::format("Vector({}, {})", element_type_name, dimension);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDenseVector::insertFrom(const IColumn & src_, size_t n)
{
    fixed_string->insertFrom(assert_cast<const ColumnDenseVector &>(src_).getFixedStringColumn(), n);
}

void ColumnDenseVector::insertManyFrom(const IColumn & src, size_t position, size_t length)
{
    fixed_string->insertManyFrom(assert_cast<const ColumnDenseVector &>(src).getFixedStringColumn(), position, length);
}

void ColumnDenseVector::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    fixed_string->insertRangeFrom(assert_cast<const ColumnDenseVector &>(src).getFixedStringColumn(), start, length);
}
#else
void ColumnDenseVector::doInsertFrom(const IColumn & src_, size_t n)
{
    fixed_string->insertFrom(assert_cast<const ColumnDenseVector &>(src_).getFixedStringColumn(), n);
}

void ColumnDenseVector::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
{
    fixed_string->insertManyFrom(assert_cast<const ColumnDenseVector &>(src).getFixedStringColumn(), position, length);
}

void ColumnDenseVector::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    fixed_string->insertRangeFrom(assert_cast<const ColumnDenseVector &>(src).getFixedStringColumn(), start, length);
}
#endif

void ColumnDenseVector::getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const
{
    if (options.notFull(name_buf))
    {
        name_buf << "vector(";
        fixed_string->getValueNameImpl(name_buf, n, options);
        name_buf << ")";
    }
}

ColumnPtr ColumnDenseVector::filter(const Filter & filt, ssize_t result_size_hint) const
{
    return ColumnDenseVector::create(fixed_string->filter(filt, result_size_hint), element_size, dimension);
}

void ColumnDenseVector::filter(const Filter & filt)
{
    fixed_string->filter(filt);
}

void ColumnDenseVector::expand(const Filter & mask, bool inverted)
{
    fixed_string->expand(mask, inverted);
}

ColumnPtr ColumnDenseVector::permute(const Permutation & perm, size_t limit) const
{
    return ColumnDenseVector::create(fixed_string->permute(perm, limit), element_size, dimension);
}

ColumnPtr ColumnDenseVector::index(const IColumn & indexes, size_t limit) const
{
    return ColumnDenseVector::create(fixed_string->index(indexes, limit), element_size, dimension);
}

ColumnPtr ColumnDenseVector::replicate(const Offsets & offsets) const
{
    return ColumnDenseVector::create(fixed_string->replicate(offsets), element_size, dimension);
}

ColumnPtr ColumnDenseVector::compress(bool force_compression) const
{
    auto compressed = fixed_string->compress(force_compression);
    const auto byte_size = compressed->byteSize();
    return ColumnCompressed::create(
        size(),
        byte_size,
        [my_compressed = std::move(compressed), my_element_size = element_size, my_dimension = dimension]
        { return ColumnDenseVector::create(my_compressed->decompress(), my_element_size, my_dimension); });
}

void ColumnDenseVector::forEachMutableSubcolumn(MutableColumnCallback callback)
{
    callback(fixed_string);
}

void ColumnDenseVector::forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*fixed_string);
    fixed_string->forEachMutableSubcolumnRecursively(callback);
}

void ColumnDenseVector::forEachSubcolumn(ColumnCallback callback) const
{
    callback(fixed_string);
}

void ColumnDenseVector::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    callback(*fixed_string);
    fixed_string->forEachSubcolumnRecursively(callback);
}

}
