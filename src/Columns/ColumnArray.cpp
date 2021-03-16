#include <string.h> // memcpy

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnCompressed.h>

#include <common/unaligned.h>
#include <common/sort.h>

#include <DataStreams/ColumnGathererStream.h>

#include <Common/Exception.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/WeakHash.h>
#include <Common/HashTable/Hash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/** Obtaining array as Field can be slow for large arrays and consume vast amount of memory.
  * Just don't allow to do it.
  * You can increase the limit if the following query:
  *  SELECT range(10000000)
  * will take less than 500ms on your machine.
  */
static constexpr size_t max_array_size_as_field = 1000000;


ColumnArray::ColumnArray(MutableColumnPtr && nested_column, MutableColumnPtr && offsets_column)
    : data(std::move(nested_column)), offsets(std::move(offsets_column))
{
    const ColumnOffsets * offsets_concrete = typeid_cast<const ColumnOffsets *>(offsets.get());

    if (!offsets_concrete)
        throw Exception("offsets_column must be a ColumnUInt64", ErrorCodes::LOGICAL_ERROR);

    if (!offsets_concrete->empty() && nested_column)
    {
        Offset last_offset = offsets_concrete->getData().back();

        /// This will also prevent possible overflow in offset.
        if (nested_column->size() != last_offset)
            throw Exception("offsets_column has data inconsistent with nested_column", ErrorCodes::LOGICAL_ERROR);
    }

    /** NOTE
      * Arrays with constant value are possible and used in implementation of higher order functions (see FunctionReplicate).
      * But in most cases, arrays with constant value are unexpected and code will work wrong. Use with caution.
      */
}

ColumnArray::ColumnArray(MutableColumnPtr && nested_column)
    : data(std::move(nested_column))
{
    if (!data->empty())
        throw Exception("Not empty data passed to ColumnArray, but no offsets passed", ErrorCodes::LOGICAL_ERROR);

    offsets = ColumnOffsets::create();
}


std::string ColumnArray::getName() const { return "Array(" + getData().getName() + ")"; }


MutableColumnPtr ColumnArray::cloneResized(size_t to_size) const
{
    auto res = ColumnArray::create(getData().cloneEmpty());

    if (to_size == 0)
        return res;

    size_t from_size = size();

    if (to_size <= from_size)
    {
        /// Just cut column.

        res->getOffsets().assign(getOffsets().begin(), getOffsets().begin() + to_size);
        res->getData().insertRangeFrom(getData(), 0, getOffsets()[to_size - 1]);
    }
    else
    {
        /// Copy column and append empty arrays for extra elements.

        Offset offset = 0;
        if (from_size > 0)
        {
            res->getOffsets().assign(getOffsets().begin(), getOffsets().end());
            res->getData().insertRangeFrom(getData(), 0, getData().size());
            offset = getOffsets().back();
        }

        res->getOffsets().resize(to_size);
        for (size_t i = from_size; i < to_size; ++i)
            res->getOffsets()[i] = offset;
    }

    return res;
}


size_t ColumnArray::size() const
{
    return getOffsets().size();
}


Field ColumnArray::operator[](size_t n) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);

    if (size > max_array_size_as_field)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array of size {} is too large to be manipulated as single field, maximum size {}",
            size, max_array_size_as_field);

    Array res(size);

    for (size_t i = 0; i < size; ++i)
        res[i] = getData()[offset + i];

    return res;
}


void ColumnArray::get(size_t n, Field & res) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);

    if (size > max_array_size_as_field)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array of size {} is too large to be manipulated as single field, maximum size {}",
            size, max_array_size_as_field);

    res = Array(size);
    Array & res_arr = DB::get<Array &>(res);

    for (size_t i = 0; i < size; ++i)
        getData().get(offset + i, res_arr[i]);
}


StringRef ColumnArray::getDataAt(size_t n) const
{
    /** Returns the range of memory that covers all elements of the array.
      * Works for arrays of fixed length values.
      * For arrays of strings and arrays of arrays, the resulting chunk of memory may not be one-to-one correspondence with the elements,
      *  since it contains only the data laid in succession, but not the offsets.
      */

    size_t offset_of_first_elem = offsetAt(n);
    StringRef first = getData().getDataAtWithTerminatingZero(offset_of_first_elem);

    size_t array_size = sizeAt(n);
    if (array_size == 0)
        return StringRef(first.data, 0);

    size_t offset_of_last_elem = getOffsets()[n] - 1;
    StringRef last = getData().getDataAtWithTerminatingZero(offset_of_last_elem);

    return StringRef(first.data, last.data + last.size - first.data);
}


void ColumnArray::insertData(const char * pos, size_t length)
{
    /** Similarly - only for arrays of fixed length values.
      */
    if (!data->isFixedAndContiguous())
        throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);

    size_t field_size = data->sizeOfValueIfFixed();

    size_t elems = 0;

    if (length)
    {
        const char * end = pos + length;
        for (; pos + field_size <= end; pos += field_size, ++elems)
            data->insertData(pos, field_size);

        if (pos != end)
            throw Exception("Incorrect length argument for method ColumnArray::insertData", ErrorCodes::BAD_ARGUMENTS);
    }

    getOffsets().push_back(getOffsets().back() + elems);
}


StringRef ColumnArray::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    size_t array_size = sizeAt(n);
    size_t offset = offsetAt(n);

    char * pos = arena.allocContinue(sizeof(array_size), begin);
    memcpy(pos, &array_size, sizeof(array_size));

    StringRef res(pos, sizeof(array_size));

    for (size_t i = 0; i < array_size; ++i)
    {
        auto value_ref = getData().serializeValueIntoArena(offset + i, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    return res;
}


const char * ColumnArray::deserializeAndInsertFromArena(const char * pos)
{
    size_t array_size = unalignedLoad<size_t>(pos);
    pos += sizeof(array_size);

    for (size_t i = 0; i < array_size; ++i)
        pos = getData().deserializeAndInsertFromArena(pos);

    getOffsets().push_back(getOffsets().back() + array_size);
    return pos;
}

const char * ColumnArray::skipSerializedInArena(const char * pos) const
{
    size_t array_size = unalignedLoad<size_t>(pos);
    pos += sizeof(array_size);

    for (size_t i = 0; i < array_size; ++i)
        pos = getData().skipSerializedInArena(pos);

    return pos;
}

void ColumnArray::updateHashWithValue(size_t n, SipHash & hash) const
{
    size_t array_size = sizeAt(n);
    size_t offset = offsetAt(n);

    hash.update(array_size);
    for (size_t i = 0; i < array_size; ++i)
        getData().updateHashWithValue(offset + i, hash);
}

void ColumnArray::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = offsets->size();
    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    WeakHash32 internal_hash(data->size());
    data->updateWeakHash32(internal_hash);

    Offset prev_offset = 0;
    const auto & offsets_data = getOffsets();
    auto & hash_data = hash.getData();
    auto & internal_hash_data = internal_hash.getData();

    for (size_t i = 0; i < s; ++i)
    {
        /// This row improves hash a little bit according to integration tests.
        /// It is the same as to use previous hash value as the first element of array.
        hash_data[i] = intHashCRC32(hash_data[i]);

        for (size_t row = prev_offset; row < offsets_data[i]; ++row)
            /// It is probably not the best way to combine hashes.
            /// But much better then xor which lead to similar hash for arrays like [1], [1, 1, 1], [1, 1, 1, 1, 1], ...
            /// Much better implementation - to add offsets as an optional argument to updateWeakHash32.
            hash_data[i] = intHashCRC32(internal_hash_data[row], hash_data[i]);

        prev_offset = offsets_data[i];
    }
}

void ColumnArray::updateHashFast(SipHash & hash) const
{
    offsets->updateHashFast(hash);
    data->updateHashFast(hash);
}

void ColumnArray::insert(const Field & x)
{
    const Array & array = DB::get<const Array &>(x);
    size_t size = array.size();
    for (size_t i = 0; i < size; ++i)
        getData().insert(array[i]);
    getOffsets().push_back(getOffsets().back() + size);
}


void ColumnArray::insertFrom(const IColumn & src_, size_t n)
{
    const ColumnArray & src = assert_cast<const ColumnArray &>(src_);
    size_t size = src.sizeAt(n);
    size_t offset = src.offsetAt(n);

    getData().insertRangeFrom(src.getData(), offset, size);
    getOffsets().push_back(getOffsets().back() + size);
}


void ColumnArray::insertDefault()
{
    /// NOTE 1: We can use back() even if the array is empty (due to zero -1th element in PODArray).
    /// NOTE 2: We cannot use reference in push_back, because reference get invalidated if array is reallocated.
    auto last_offset = getOffsets().back();
    getOffsets().push_back(last_offset);
}


void ColumnArray::popBack(size_t n)
{
    auto & offsets_data = getOffsets();
    size_t nested_n = offsets_data.back() - offsetAt(offsets_data.size() - n);
    if (nested_n)
        getData().popBack(nested_n);
    offsets_data.resize_assume_reserved(offsets_data.size() - n);
}

int ColumnArray::compareAtImpl(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint, const Collator * collator) const
{
    const ColumnArray & rhs = assert_cast<const ColumnArray &>(rhs_);

    /// Suboptimal
    size_t lhs_size = sizeAt(n);
    size_t rhs_size = rhs.sizeAt(m);
    size_t min_size = std::min(lhs_size, rhs_size);
    for (size_t i = 0; i < min_size; ++i)
    {
        int res;
        if (collator)
            res = getData().compareAtWithCollation(offsetAt(n) + i, rhs.offsetAt(m) + i, *rhs.data.get(), nan_direction_hint, *collator);
        else
            res = getData().compareAt(offsetAt(n) + i, rhs.offsetAt(m) + i, *rhs.data.get(), nan_direction_hint);
        if (res)
            return res;
    }

    return lhs_size < rhs_size
        ? -1
        : (lhs_size == rhs_size
            ? 0
            : 1);
}

int ColumnArray::compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
{
    return compareAtImpl(n, m, rhs_, nan_direction_hint);
}

int ColumnArray::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs_, nan_direction_hint, &collator);
}

void ColumnArray::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnArray>(assert_cast<const ColumnArray &>(rhs), rhs_row_num, row_indexes,
                                        compare_results, direction, nan_direction_hint);
}

bool ColumnArray::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnArray>();
}

namespace
{

template <bool positive>
struct Cmp
{
    const ColumnArray & parent;
    int nan_direction_hint;
    const Collator * collator;

    Cmp(const ColumnArray & parent_, int nan_direction_hint_, const Collator * collator_=nullptr)
        : parent(parent_), nan_direction_hint(nan_direction_hint_), collator(collator_) {}

    int operator()(size_t lhs, size_t rhs) const
    {
        int res;
        if (collator)
            res = parent.compareAtWithCollation(lhs, rhs, parent, nan_direction_hint, *collator);
        else
            res = parent.compareAt(lhs, rhs, parent, nan_direction_hint);
        return positive ? res : -res;
    }
};

}


void ColumnArray::reserve(size_t n)
{
    getOffsets().reserve(n);
    getData().reserve(n); /// The average size of arrays is not taken into account here. Or it is considered to be no more than 1.
}


size_t ColumnArray::byteSize() const
{
    return getData().byteSize() + getOffsets().size() * sizeof(getOffsets()[0]);
}


size_t ColumnArray::byteSizeAt(size_t n) const
{
    const auto & offsets_data = getOffsets();

    size_t pos = offsets_data[n - 1];
    size_t end = offsets_data[n];

    size_t res = sizeof(offsets_data[0]);
    for (; pos < end; ++pos)
        res += getData().byteSizeAt(pos);

    return res;
}


size_t ColumnArray::allocatedBytes() const
{
    return getData().allocatedBytes() + getOffsets().allocated_bytes();
}


void ColumnArray::protect()
{
    getData().protect();
    getOffsets().protect();
}


bool ColumnArray::hasEqualOffsets(const ColumnArray & other) const
{
    if (offsets == other.offsets)
        return true;

    const Offsets & offsets1 = getOffsets();
    const Offsets & offsets2 = other.getOffsets();
    return offsets1.size() == offsets2.size()
        && (offsets1.empty() || 0 == memcmp(offsets1.data(), offsets2.data(), sizeof(offsets1[0]) * offsets1.size()));
}


ColumnPtr ColumnArray::convertToFullColumnIfConst() const
{
    /// It is possible to have an array with constant data and non-constant offsets.
    /// Example is the result of expression: replicate('hello', [1])
    return ColumnArray::create(data->convertToFullColumnIfConst(), offsets);
}

void ColumnArray::getExtremes(Field & min, Field & max) const
{
    min = Array();
    max = Array();

    size_t col_size = size();

    if (col_size == 0)
        return;

    size_t min_idx = 0;
    size_t max_idx = 0;

    for (size_t i = 1; i < col_size; ++i)
    {
        if (compareAt(i, min_idx, *this, /* nan_direction_hint = */ 1) < 0)
            min_idx = i;
        else if (compareAt(i, max_idx, *this, /* nan_direction_hint = */ -1) > 0)
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}


void ColumnArray::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    const ColumnArray & src_concrete = assert_cast<const ColumnArray &>(src);

    if (start + length > src_concrete.getOffsets().size())
        throw Exception("Parameter out of bound in ColumnArray::insertRangeFrom method. [start(" + std::to_string(start) + ") + length(" + std::to_string(length) + ") > offsets.size(" + std::to_string(src_concrete.getOffsets().size()) + ")]",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.getOffsets()[start + length - 1] - nested_offset;

    getData().insertRangeFrom(src_concrete.getData(), nested_offset, nested_length);

    Offsets & cur_offsets = getOffsets();
    const Offsets & src_offsets = src_concrete.getOffsets();

    if (start == 0 && cur_offsets.empty())
    {
        cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
    }
    else
    {
        size_t old_size = cur_offsets.size();
        size_t prev_max_offset = old_size ? cur_offsets.back() : 0;
        cur_offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            cur_offsets[old_size + i] = src_offsets[start + i] - nested_offset + prev_max_offset;
    }
}


ColumnPtr ColumnArray::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (typeid_cast<const ColumnUInt8 *>(data.get()))      return filterNumber<UInt8>(filt, result_size_hint);
    if (typeid_cast<const ColumnUInt16 *>(data.get()))     return filterNumber<UInt16>(filt, result_size_hint);
    if (typeid_cast<const ColumnUInt32 *>(data.get()))     return filterNumber<UInt32>(filt, result_size_hint);
    if (typeid_cast<const ColumnUInt64 *>(data.get()))     return filterNumber<UInt64>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt8 *>(data.get()))       return filterNumber<Int8>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt16 *>(data.get()))      return filterNumber<Int16>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt32 *>(data.get()))      return filterNumber<Int32>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt64 *>(data.get()))      return filterNumber<Int64>(filt, result_size_hint);
    if (typeid_cast<const ColumnFloat32 *>(data.get()))    return filterNumber<Float32>(filt, result_size_hint);
    if (typeid_cast<const ColumnFloat64 *>(data.get()))    return filterNumber<Float64>(filt, result_size_hint);
    if (typeid_cast<const ColumnString *>(data.get()))     return filterString(filt, result_size_hint);
    if (typeid_cast<const ColumnTuple *>(data.get()))      return filterTuple(filt, result_size_hint);
    if (typeid_cast<const ColumnNullable *>(data.get()))   return filterNullable(filt, result_size_hint);
    return filterGeneric(filt, result_size_hint);
}

template <typename T>
ColumnPtr ColumnArray::filterNumber(const Filter & filt, ssize_t result_size_hint) const
{
    if (getOffsets().empty())
        return ColumnArray::create(data);

    auto res = ColumnArray::create(data->cloneEmpty());

    auto & res_elems = assert_cast<ColumnVector<T> &>(res->getData()).getData();
    Offsets & res_offsets = res->getOffsets();

    filterArraysImpl<T>(assert_cast<const ColumnVector<T> &>(*data).getData(), getOffsets(), res_elems, res_offsets, filt, result_size_hint);
    return res;
}

ColumnPtr ColumnArray::filterString(const Filter & filt, ssize_t result_size_hint) const
{
    size_t col_size = getOffsets().size();
    if (col_size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == col_size)
        return ColumnArray::create(data);

    auto res = ColumnArray::create(data->cloneEmpty());

    const ColumnString & src_string = typeid_cast<const ColumnString &>(*data);
    const ColumnString::Chars & src_chars = src_string.getChars();
    const Offsets & src_string_offsets = src_string.getOffsets();
    const Offsets & src_offsets = getOffsets();

    ColumnString::Chars & res_chars = typeid_cast<ColumnString &>(res->getData()).getChars();
    Offsets & res_string_offsets = typeid_cast<ColumnString &>(res->getData()).getOffsets();
    Offsets & res_offsets = res->getOffsets();

    if (result_size_hint < 0)    /// Other cases are not considered.
    {
        res_chars.reserve(src_chars.size());
        res_string_offsets.reserve(src_string_offsets.size());
        res_offsets.reserve(col_size);
    }

    Offset prev_src_offset = 0;
    Offset prev_src_string_offset = 0;

    Offset prev_res_offset = 0;
    Offset prev_res_string_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        /// Number of rows in the array.
        size_t array_size = src_offsets[i] - prev_src_offset;

        if (filt[i])
        {
            /// If the array is not empty - copy content.
            if (array_size)
            {
                size_t chars_to_copy = src_string_offsets[array_size + prev_src_offset - 1] - prev_src_string_offset;
                size_t res_chars_prev_size = res_chars.size();
                res_chars.resize(res_chars_prev_size + chars_to_copy);
                memcpy(&res_chars[res_chars_prev_size], &src_chars[prev_src_string_offset], chars_to_copy);

                for (size_t j = 0; j < array_size; ++j)
                    res_string_offsets.push_back(src_string_offsets[j + prev_src_offset] + prev_res_string_offset - prev_src_string_offset);

                prev_res_string_offset = res_string_offsets.back();
            }

            prev_res_offset += array_size;
            res_offsets.push_back(prev_res_offset);
        }

        if (array_size)
        {
            prev_src_offset += array_size;
            prev_src_string_offset = src_string_offsets[prev_src_offset - 1];
        }
    }

    return res;
}

ColumnPtr ColumnArray::filterGeneric(const Filter & filt, ssize_t result_size_hint) const
{
    size_t size = getOffsets().size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (size == 0)
        return ColumnArray::create(data);

    Filter nested_filt(getOffsets().back());
    for (size_t i = 0; i < size; ++i)
    {
        if (filt[i])
            memset(&nested_filt[offsetAt(i)], 1, sizeAt(i));
        else
            memset(&nested_filt[offsetAt(i)], 0, sizeAt(i));
    }

    auto res = ColumnArray::create(data->cloneEmpty());

    ssize_t nested_result_size_hint = 0;
    if (result_size_hint < 0)
        nested_result_size_hint = result_size_hint;
    else if (result_size_hint && result_size_hint < 1000000000 && data->size() < 1000000000)    /// Avoid overflow.
         nested_result_size_hint = result_size_hint * data->size() / size;

    res->data = data->filter(nested_filt, nested_result_size_hint);

    Offsets & res_offsets = res->getOffsets();
    if (result_size_hint)
        res_offsets.reserve(result_size_hint > 0 ? result_size_hint : size);

    size_t current_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        if (filt[i])
        {
            current_offset += sizeAt(i);
            res_offsets.push_back(current_offset);
        }
    }

    return res;
}

ColumnPtr ColumnArray::filterNullable(const Filter & filt, ssize_t result_size_hint) const
{
    if (getOffsets().empty())
        return ColumnArray::create(data);

    const ColumnNullable & nullable_elems = assert_cast<const ColumnNullable &>(*data);

    auto array_of_nested = ColumnArray::create(nullable_elems.getNestedColumnPtr(), offsets);
    auto filtered_array_of_nested_owner = array_of_nested->filter(filt, result_size_hint);
    const auto & filtered_array_of_nested = assert_cast<const ColumnArray &>(*filtered_array_of_nested_owner);
    const auto & filtered_offsets = filtered_array_of_nested.getOffsetsPtr();

    auto res_null_map = ColumnUInt8::create();

    filterArraysImplOnlyData(nullable_elems.getNullMapData(), getOffsets(), res_null_map->getData(), filt, result_size_hint);

    return ColumnArray::create(
        ColumnNullable::create(
            filtered_array_of_nested.getDataPtr(),
            std::move(res_null_map)),
        filtered_offsets);
}

ColumnPtr ColumnArray::filterTuple(const Filter & filt, ssize_t result_size_hint) const
{
    if (getOffsets().empty())
        return ColumnArray::create(data);

    const ColumnTuple & tuple = assert_cast<const ColumnTuple &>(*data);

    /// Make temporary arrays for each components of Tuple, then filter and collect back.

    size_t tuple_size = tuple.tupleSize();

    if (tuple_size == 0)
        throw Exception("Logical error: empty tuple", ErrorCodes::LOGICAL_ERROR);

    Columns temporary_arrays(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        temporary_arrays[i] = ColumnArray(tuple.getColumns()[i]->assumeMutable(), getOffsetsPtr()->assumeMutable())
                .filter(filt, result_size_hint);

    Columns tuple_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        tuple_columns[i] = assert_cast<const ColumnArray &>(*temporary_arrays[i]).getDataPtr();

    return ColumnArray::create(
        ColumnTuple::create(tuple_columns),
        assert_cast<const ColumnArray &>(*temporary_arrays.front()).getOffsetsPtr());
}


ColumnPtr ColumnArray::permute(const Permutation & perm, size_t limit) const
{
    size_t size = getOffsets().size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (limit == 0)
        return ColumnArray::create(data);

    Permutation nested_perm(getOffsets().back());

    auto res = ColumnArray::create(data->cloneEmpty());

    Offsets & res_offsets = res->getOffsets();
    res_offsets.resize(limit);
    size_t current_offset = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        for (size_t j = 0; j < sizeAt(perm[i]); ++j)
            nested_perm[current_offset + j] = offsetAt(perm[i]) + j;
        current_offset += sizeAt(perm[i]);
        res_offsets[i] = current_offset;
    }

    if (current_offset != 0)
        res->data = data->permute(nested_perm, current_offset);

    return res;
}

ColumnPtr ColumnArray::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename T>
ColumnPtr ColumnArray::indexImpl(const PaddedPODArray<T> & indexes, size_t limit) const
{
    if (limit == 0)
        return ColumnArray::create(data);

    /// Convert indexes to UInt64 in case of overflow.
    auto nested_indexes_column = ColumnUInt64::create();
    PaddedPODArray<UInt64> & nested_indexes = nested_indexes_column->getData();
    nested_indexes.reserve(getOffsets().back());

    auto res = ColumnArray::create(data->cloneEmpty());

    Offsets & res_offsets = res->getOffsets();
    res_offsets.resize(limit);
    size_t current_offset = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        for (size_t j = 0; j < sizeAt(indexes[i]); ++j)
            nested_indexes.push_back(offsetAt(indexes[i]) + j);
        current_offset += sizeAt(indexes[i]);
        res_offsets[i] = current_offset;
    }

    if (current_offset != 0)
        res->data = data->index(*nested_indexes_column, current_offset);

    return res;
}

INSTANTIATE_INDEX_IMPL(ColumnArray)

template <typename Comparator>
void ColumnArray::getPermutationImpl(size_t limit, Permutation & res, Comparator cmp) const
{
    size_t s = size();
    if (limit >= s)
        limit = 0;

    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    auto less = [&cmp](size_t lhs, size_t rhs){ return cmp(lhs, rhs) < 0; };

    if (limit)
        partial_sort(res.begin(), res.begin() + limit, res.end(), less);
    else
        std::sort(res.begin(), res.end(), less);
}

template <typename Comparator>
void ColumnArray::updatePermutationImpl(size_t limit, Permutation & res, EqualRanges & equal_range, Comparator cmp) const
{
    if (equal_range.empty())
        return;

    if (limit >= size() || limit >= equal_range.back().second)
        limit = 0;

    size_t number_of_ranges = equal_range.size();

    if (limit)
        --number_of_ranges;

    auto less = [&cmp](size_t lhs, size_t rhs){ return cmp(lhs, rhs) < 0; };

    EqualRanges new_ranges;
    for (size_t i = 0; i < number_of_ranges; ++i)
    {
        const auto & [first, last] = equal_range[i];

        std::sort(res.begin() + first, res.begin() + last, less);
        auto new_first = first;

        for (auto j = first + 1; j < last; ++j)
        {
            if (cmp(res[new_first], res[j]) != 0)
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }

        if (last - new_first > 1)
            new_ranges.emplace_back(new_first, last);
    }

    if (limit)
    {
        const auto & [first, last] = equal_range.back();

        if (limit < first || limit > last)
            return;

        /// Since then we are working inside the interval.
        partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last, less);
        auto new_first = first;
        for (auto j = first + 1; j < limit; ++j)
        {
            if (cmp(res[new_first], res[j]) != 0)
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }
        auto new_last = limit;
        for (auto j = limit; j < last; ++j)
        {
            if (cmp(res[new_first], res[j]) == 0)
            {
                std::swap(res[new_last], res[j]);
                ++new_last;
            }
        }
        if (new_last - new_first > 1)
        {
            new_ranges.emplace_back(new_first, new_last);
        }
    }
    equal_range = std::move(new_ranges);
}

void ColumnArray::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (reverse)
        getPermutationImpl(limit, res, Cmp<false>(*this, nan_direction_hint));
    else
        getPermutationImpl(limit, res, Cmp<true>(*this, nan_direction_hint));

}

void ColumnArray::updatePermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_range) const
{
    if (reverse)
        updatePermutationImpl(limit, res, equal_range, Cmp<false>(*this, nan_direction_hint));
    else
        updatePermutationImpl(limit, res, equal_range, Cmp<true>(*this, nan_direction_hint));
}

void ColumnArray::getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (reverse)
        getPermutationImpl(limit, res, Cmp<false>(*this, nan_direction_hint, &collator));
    else
        getPermutationImpl(limit, res, Cmp<true>(*this, nan_direction_hint, &collator));
}

void ColumnArray::updatePermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_range) const
{
    if (reverse)
        updatePermutationImpl(limit, res, equal_range, Cmp<false>(*this, nan_direction_hint, &collator));
    else
        updatePermutationImpl(limit, res, equal_range, Cmp<true>(*this, nan_direction_hint, &collator));
}

ColumnPtr ColumnArray::compress() const
{
    ColumnPtr data_compressed = data->compress();
    ColumnPtr offsets_compressed = offsets->compress();

    size_t byte_size = data_compressed->byteSize() + offsets_compressed->byteSize();

    return ColumnCompressed::create(size(), byte_size,
        [data_compressed = std::move(data_compressed), offsets_compressed = std::move(offsets_compressed)]
        {
            return ColumnArray::create(data_compressed->decompress(), offsets_compressed->decompress());
        });
}


ColumnPtr ColumnArray::replicate(const Offsets & replicate_offsets) const
{
    if (replicate_offsets.empty())
        return cloneEmpty();

    if (typeid_cast<const ColumnUInt8 *>(data.get()))    return replicateNumber<UInt8>(replicate_offsets);
    if (typeid_cast<const ColumnUInt16 *>(data.get()))   return replicateNumber<UInt16>(replicate_offsets);
    if (typeid_cast<const ColumnUInt32 *>(data.get()))   return replicateNumber<UInt32>(replicate_offsets);
    if (typeid_cast<const ColumnUInt64 *>(data.get()))   return replicateNumber<UInt64>(replicate_offsets);
    if (typeid_cast<const ColumnInt8 *>(data.get()))     return replicateNumber<Int8>(replicate_offsets);
    if (typeid_cast<const ColumnInt16 *>(data.get()))    return replicateNumber<Int16>(replicate_offsets);
    if (typeid_cast<const ColumnInt32 *>(data.get()))    return replicateNumber<Int32>(replicate_offsets);
    if (typeid_cast<const ColumnInt64 *>(data.get()))    return replicateNumber<Int64>(replicate_offsets);
    if (typeid_cast<const ColumnFloat32 *>(data.get()))  return replicateNumber<Float32>(replicate_offsets);
    if (typeid_cast<const ColumnFloat64 *>(data.get()))  return replicateNumber<Float64>(replicate_offsets);
    if (typeid_cast<const ColumnString *>(data.get()))   return replicateString(replicate_offsets);
    if (typeid_cast<const ColumnConst *>(data.get()))    return replicateConst(replicate_offsets);
    if (typeid_cast<const ColumnNullable *>(data.get())) return replicateNullable(replicate_offsets);
    if (typeid_cast<const ColumnTuple *>(data.get()))    return replicateTuple(replicate_offsets);
    return replicateGeneric(replicate_offsets);
}


template <typename T>
ColumnPtr ColumnArray::replicateNumber(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    MutableColumnPtr res = cloneEmpty();

    if (0 == col_size)
        return res;

    ColumnArray & res_arr = typeid_cast<ColumnArray &>(*res);

    const typename ColumnVector<T>::Container & src_data = typeid_cast<const ColumnVector<T> &>(*data).getData();
    const Offsets & src_offsets = getOffsets();

    typename ColumnVector<T>::Container & res_data = typeid_cast<ColumnVector<T> &>(res_arr.getData()).getData();
    Offsets & res_offsets = res_arr.getOffsets();

    res_data.reserve(data->size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_data_offset = 0;
    Offset current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_data_offset;

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_new_offset += value_size;
            res_offsets.push_back(current_new_offset);

            if (value_size)
            {
                res_data.resize(res_data.size() + value_size);
                memcpy(&res_data[res_data.size() - value_size], &src_data[prev_data_offset], value_size * sizeof(T));
            }
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_data_offset = src_offsets[i];
    }

    return res;
}


ColumnPtr ColumnArray::replicateString(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    MutableColumnPtr res = cloneEmpty();

    if (0 == col_size)
        return res;

    ColumnArray & res_arr = assert_cast<ColumnArray &>(*res);

    const ColumnString & src_string = typeid_cast<const ColumnString &>(*data);
    const ColumnString::Chars & src_chars = src_string.getChars();
    const Offsets & src_string_offsets = src_string.getOffsets();
    const Offsets & src_offsets = getOffsets();

    ColumnString::Chars & res_chars = typeid_cast<ColumnString &>(res_arr.getData()).getChars();
    Offsets & res_string_offsets = typeid_cast<ColumnString &>(res_arr.getData()).getOffsets();
    Offsets & res_offsets = res_arr.getOffsets();

    res_chars.reserve(src_chars.size() / col_size * replicate_offsets.back());
    res_string_offsets.reserve(src_string_offsets.size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    Offset prev_replicate_offset = 0;

    Offset prev_src_offset = 0;
    Offset prev_src_string_offset = 0;

    Offset current_res_offset = 0;
    Offset current_res_string_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        /// How many times to replicate the array.
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        /// The number of strings in the array.
        size_t value_size = src_offsets[i] - prev_src_offset;
        /// Number of characters in strings of the array, including zero bytes.
        size_t sum_chars_size = src_string_offsets[prev_src_offset + value_size - 1] - prev_src_string_offset;  /// -1th index is Ok, see PaddedPODArray.

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_res_offset += value_size;
            res_offsets.push_back(current_res_offset);

            size_t prev_src_string_offset_local = prev_src_string_offset;
            for (size_t k = 0; k < value_size; ++k)
            {
                /// Size of single string.
                size_t chars_size = src_string_offsets[k + prev_src_offset] - prev_src_string_offset_local;

                current_res_string_offset += chars_size;
                res_string_offsets.push_back(current_res_string_offset);

                prev_src_string_offset_local += chars_size;
            }

            if (sum_chars_size)
            {
                /// Copies the characters of the array of strings.
                res_chars.resize(res_chars.size() + sum_chars_size);
                memcpySmallAllowReadWriteOverflow15(
                    &res_chars[res_chars.size() - sum_chars_size], &src_chars[prev_src_string_offset], sum_chars_size);
            }
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_src_offset = src_offsets[i];
        prev_src_string_offset += sum_chars_size;
    }

    return res;
}


ColumnPtr ColumnArray::replicateConst(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == col_size)
        return cloneEmpty();

    const Offsets & src_offsets = getOffsets();

    auto res_column_offsets = ColumnOffsets::create();
    Offsets & res_offsets = res_column_offsets->getData();
    res_offsets.reserve(replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_data_offset = 0;
    Offset current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_data_offset;

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_new_offset += value_size;
            res_offsets.push_back(current_new_offset);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_data_offset = src_offsets[i];
    }

    return ColumnArray::create(getData().cloneResized(current_new_offset), std::move(res_column_offsets));
}


ColumnPtr ColumnArray::replicateGeneric(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    MutableColumnPtr res = cloneEmpty();
    ColumnArray & res_concrete = assert_cast<ColumnArray &>(*res);

    if (0 == col_size)
        return res;

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_offset;
        prev_offset = replicate_offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
            res_concrete.insertFrom(*this, i);
    }

    return res;
}


ColumnPtr ColumnArray::replicateNullable(const Offsets & replicate_offsets) const
{
    const ColumnNullable & nullable = assert_cast<const ColumnNullable &>(*data);

    /// Make temporary arrays for each components of Nullable. Then replicate them independently and collect back to result.
    /// NOTE Offsets are calculated twice and it is redundant.

    auto array_of_nested = ColumnArray(nullable.getNestedColumnPtr()->assumeMutable(), getOffsetsPtr()->assumeMutable())
            .replicate(replicate_offsets);
    auto array_of_null_map = ColumnArray(nullable.getNullMapColumnPtr()->assumeMutable(), getOffsetsPtr()->assumeMutable())
            .replicate(replicate_offsets);

    return ColumnArray::create(
        ColumnNullable::create(
            assert_cast<const ColumnArray &>(*array_of_nested).getDataPtr(),
            assert_cast<const ColumnArray &>(*array_of_null_map).getDataPtr()),
        assert_cast<const ColumnArray &>(*array_of_nested).getOffsetsPtr());
}


ColumnPtr ColumnArray::replicateTuple(const Offsets & replicate_offsets) const
{
    const ColumnTuple & tuple = assert_cast<const ColumnTuple &>(*data);

    /// Make temporary arrays for each components of Tuple. In the same way as for Nullable.

    size_t tuple_size = tuple.tupleSize();

    if (tuple_size == 0)
        throw Exception("Logical error: empty tuple", ErrorCodes::LOGICAL_ERROR);

    Columns temporary_arrays(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        temporary_arrays[i] = ColumnArray(tuple.getColumns()[i]->assumeMutable(), getOffsetsPtr()->assumeMutable())
                .replicate(replicate_offsets);

    Columns tuple_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        tuple_columns[i] = assert_cast<const ColumnArray &>(*temporary_arrays[i]).getDataPtr();

    return ColumnArray::create(
        ColumnTuple::create(tuple_columns),
        assert_cast<const ColumnArray &>(*temporary_arrays.front()).getOffsetsPtr());
}


void ColumnArray::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

}
