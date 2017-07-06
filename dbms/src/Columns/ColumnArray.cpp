#include <string.h> // memcpy

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>

#include <DataStreams/ColumnGathererStream.h>

#include <Common/Exception.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


ColumnArray::ColumnArray(ColumnPtr nested_column, ColumnPtr offsets_column)
    : data(nested_column), offsets(offsets_column)
{
    if (!offsets_column)
    {
        offsets = std::make_shared<ColumnOffsets_t>();
    }
    else
    {
        if (!typeid_cast<ColumnOffsets_t *>(&*offsets_column))
            throw Exception("offsets_column must be a ColumnUInt64", ErrorCodes::ILLEGAL_COLUMN);
    }

    /** NOTE
      * Arrays with constant value are possible and used in implementation of higher order functions and in ARRAY JOIN.
      * But in most cases, arrays with constant value are unexpected and code will work wrong. Use with caution.
      */
}


std::string ColumnArray::getName() const { return "ColumnArray(" + getData().getName() + ")"; }


ColumnPtr ColumnArray::cloneResized(size_t to_size) const
{
    auto res = std::make_shared<ColumnArray>(getData().cloneEmpty());

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

        Offset_t offset = 0;
        if (from_size > 0)
        {
            res->getOffsets().assign(getOffsets().begin(), getOffsets().end());
            res->getDataPtr() = getData().clone();
            offset = getOffsets().back();
        }

        res->getOffsets().resize(to_size);
        for (size_t i = from_size; i < to_size; ++i)
        {
            ++offset;
            res->getOffsets()[i] = offset;
        }
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
    Array res(size);

    for (size_t i = 0; i < size; ++i)
        res[i] = getData()[offset + i];

    return res;
}


void ColumnArray::get(size_t n, Field & res) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);
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

    size_t array_size = sizeAt(n);
    if (array_size == 0)
        return StringRef();

    size_t offset_of_first_elem = offsetAt(n);
    StringRef first = getData().getDataAtWithTerminatingZero(offset_of_first_elem);

    size_t offset_of_last_elem = getOffsets()[n] - 1;
    StringRef last = getData().getDataAtWithTerminatingZero(offset_of_last_elem);

    return StringRef(first.data, last.data + last.size - first.data);
}


void ColumnArray::insertData(const char * pos, size_t length)
{
    /** Similarly - only for arrays of fixed length values.
      */
    IColumn * data_ = data.get();
    if (!data_->isFixed())
        throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);

    size_t field_size = data_->sizeOfField();

    const char * end = pos + length;
    size_t elems = 0;
    for (; pos + field_size <= end; pos += field_size, ++elems)
        data_->insertData(pos, field_size);

    if (pos != end)
        throw Exception("Incorrect length argument for method ColumnArray::insertData", ErrorCodes::BAD_ARGUMENTS);

    getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + elems);
}


StringRef ColumnArray::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    size_t array_size = sizeAt(n);
    size_t offset = offsetAt(n);

    char * pos = arena.allocContinue(sizeof(array_size), begin);
    memcpy(pos, &array_size, sizeof(array_size));

    size_t values_size = 0;
    for (size_t i = 0; i < array_size; ++i)
        values_size += getData().serializeValueIntoArena(offset + i, arena, begin).size;

    return StringRef(begin, sizeof(array_size) + values_size);
}


const char * ColumnArray::deserializeAndInsertFromArena(const char * pos)
{
    size_t array_size = *reinterpret_cast<const size_t *>(pos);
    pos += sizeof(array_size);

    for (size_t i = 0; i < array_size; ++i)
        pos = getData().deserializeAndInsertFromArena(pos);

    getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + array_size);
    return pos;
}


void ColumnArray::updateHashWithValue(size_t n, SipHash & hash) const
{
    size_t array_size = sizeAt(n);
    size_t offset = offsetAt(n);

    hash.update(reinterpret_cast<const char *>(&array_size), sizeof(array_size));
    for (size_t i = 0; i < array_size; ++i)
        getData().updateHashWithValue(offset + i, hash);
}


void ColumnArray::insert(const Field & x)
{
    const Array & array = DB::get<const Array &>(x);
    size_t size = array.size();
    for (size_t i = 0; i < size; ++i)
        getData().insert(array[i]);
    getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size);
}


void ColumnArray::insertFrom(const IColumn & src_, size_t n)
{
    const ColumnArray & src = static_cast<const ColumnArray &>(src_);
    size_t size = src.sizeAt(n);
    size_t offset = src.offsetAt(n);

    getData().insertRangeFrom(src.getData(), offset, size);
    getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size);
}


void ColumnArray::insertDefault()
{
    getOffsets().push_back(getOffsets().size() == 0 ? 0 : getOffsets().back());
}


void ColumnArray::popBack(size_t n)
{
    auto & offsets = getOffsets();
    size_t nested_n = offsets.back() - offsetAt(offsets.size() - n);
    if (nested_n)
        getData().popBack(nested_n);
    offsets.resize_assume_reserved(offsets.size() - n);
}


int ColumnArray::compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
{
    const ColumnArray & rhs = static_cast<const ColumnArray &>(rhs_);

    /// Suboptimal
    size_t lhs_size = sizeAt(n);
    size_t rhs_size = rhs.sizeAt(m);
    size_t min_size = std::min(lhs_size, rhs_size);
    for (size_t i = 0; i < min_size; ++i)
        if (int res = getData().compareAt(offsetAt(n) + i, rhs.offsetAt(m) + i, *rhs.data.get(), nan_direction_hint))
            return res;

    return lhs_size < rhs_size
        ? -1
        : (lhs_size == rhs_size
            ? 0
            : 1);
}


namespace
{
    template <bool positive>
    struct less
    {
        const ColumnArray & parent;
        int nan_direction_hint;

        less(const ColumnArray & parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}

        bool operator()(size_t lhs, size_t rhs) const
        {
            if (positive)
                return parent.compareAt(lhs, rhs, parent, nan_direction_hint) < 0;
            else
                return parent.compareAt(lhs, rhs, parent, nan_direction_hint) > 0;
        }
    };
}


void ColumnArray::reserve(size_t n)
{
    getOffsets().reserve(n);
    getData().reserve(n);        /// The average size of arrays is not taken into account here. Or it is considered to be no more than 1.
}


size_t ColumnArray::byteSize() const
{
    return getData().byteSize() + getOffsets().size() * sizeof(getOffsets()[0]);
}


size_t ColumnArray::allocatedSize() const
{
    return getData().allocatedSize() + getOffsets().allocated_size() * sizeof(getOffsets()[0]);
}


bool ColumnArray::hasEqualOffsets(const ColumnArray & other) const
{
    if (offsets == other.offsets)
        return true;

    const Offsets_t & offsets1 = getOffsets();
    const Offsets_t & offsets2 = other.getOffsets();
    return offsets1.size() == offsets2.size() && 0 == memcmp(&offsets1[0], &offsets2[0], sizeof(offsets1[0]) * offsets1.size());
}


ColumnPtr ColumnArray::convertToFullColumnIfConst() const
{
    ColumnPtr new_data;
    ColumnPtr new_offsets;

    if (auto full_column = getData().convertToFullColumnIfConst())
        new_data = full_column;
    else
        new_data = data;

    if (auto full_column = offsets.get()->convertToFullColumnIfConst())
        new_offsets = full_column;
    else
        new_offsets = offsets;

    return std::make_shared<ColumnArray>(new_data, new_offsets);
}


void ColumnArray::getExtremes(Field & min, Field & max) const
{
    min = Array();
    max = Array();
}


void ColumnArray::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    const ColumnArray & src_concrete = static_cast<const ColumnArray &>(src);

    if (start + length > src_concrete.getOffsets().size())
        throw Exception("Parameter out of bound in ColumnArray::insertRangeFrom method.",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.getOffsets()[start + length - 1] - nested_offset;

    data->insertRangeFrom(src_concrete.getData(), nested_offset, nested_length);

    Offsets_t & cur_offsets = getOffsets();
    const Offsets_t & src_offsets = src_concrete.getOffsets();

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
    if (getOffsets().size() == 0)
        return std::make_shared<ColumnArray>(data);

    auto res = std::make_shared<ColumnArray>(data->cloneEmpty());

    auto & res_elems = static_cast<ColumnVector<T> &>(res->getData()).getData();
    Offsets_t & res_offsets = res->getOffsets();

    filterArraysImpl<T>(static_cast<const ColumnVector<T> &>(*data).getData(), getOffsets(), res_elems, res_offsets, filt, result_size_hint);
    return res;
}

ColumnPtr ColumnArray::filterString(const Filter & filt, ssize_t result_size_hint) const
{
    size_t col_size = getOffsets().size();
    if (col_size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == col_size)
        return std::make_shared<ColumnArray>(data);

    auto res = std::make_shared<ColumnArray>(data->cloneEmpty());

    const ColumnString & src_string = typeid_cast<const ColumnString &>(*data);
    const ColumnString::Chars_t & src_chars = src_string.getChars();
    const Offsets_t & src_string_offsets = src_string.getOffsets();
    const Offsets_t & src_offsets = getOffsets();

    ColumnString::Chars_t & res_chars = typeid_cast<ColumnString &>(res->getData()).getChars();
    Offsets_t & res_string_offsets = typeid_cast<ColumnString &>(res->getData()).getOffsets();
    Offsets_t & res_offsets = res->getOffsets();

    if (result_size_hint < 0)    /// Other cases are not considered.
    {
        res_chars.reserve(src_chars.size());
        res_string_offsets.reserve(src_string_offsets.size());
        res_offsets.reserve(col_size);
    }

    Offset_t prev_src_offset = 0;
    Offset_t prev_src_string_offset = 0;

    Offset_t prev_res_offset = 0;
    Offset_t prev_res_string_offset = 0;

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
        return std::make_shared<ColumnArray>(data);

    Filter nested_filt(getOffsets().back());
    for (size_t i = 0; i < size; ++i)
    {
        if (filt[i])
            memset(&nested_filt[offsetAt(i)], 1, sizeAt(i));
        else
            memset(&nested_filt[offsetAt(i)], 0, sizeAt(i));
    }

    std::shared_ptr<ColumnArray> res = std::make_shared<ColumnArray>(data);

    ssize_t nested_result_size_hint = 0;
    if (result_size_hint < 0)
        nested_result_size_hint = result_size_hint;
    else if (result_size_hint && result_size_hint < 1000000000 && data->size() < 1000000000)    /// Avoid overflow.
         nested_result_size_hint = result_size_hint * data->size() / size;

    res->data = data->filter(nested_filt, nested_result_size_hint);

    Offsets_t & res_offsets = res->getOffsets();
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
    if (getOffsets().size() == 0)
        return std::make_shared<ColumnArray>(data);

    const ColumnNullable & nullable_elems = static_cast<const ColumnNullable &>(*data);

    auto array_of_nested = std::make_shared<ColumnArray>(nullable_elems.getNestedColumn(), offsets);
    auto filtered_array_of_nested_owner = array_of_nested->filter(filt, result_size_hint);
    auto & filtered_array_of_nested = static_cast<ColumnArray &>(*filtered_array_of_nested_owner);
    auto & filtered_offsets = filtered_array_of_nested.getOffsetsColumn();

    auto res_null_map = std::make_shared<ColumnUInt8>();
    auto res = std::make_shared<ColumnArray>(
        std::make_shared<ColumnNullable>(
            filtered_array_of_nested.getDataPtr(),
            res_null_map),
        filtered_offsets);

    filterArraysImplOnlyData(nullable_elems.getNullMap(), getOffsets(), res_null_map->getData(), filt, result_size_hint);
    return res;
}

ColumnPtr ColumnArray::filterTuple(const Filter & filt, ssize_t result_size_hint) const
{
    if (getOffsets().size() == 0)
        return std::make_shared<ColumnArray>(data);

    const ColumnTuple & tuple = static_cast<const ColumnTuple &>(*data);

    /// Make temporary arrays for each components of Tuple, then filter and collect back.

    size_t tuple_size = tuple.getColumns().size();

    if (tuple_size == 0)
        throw Exception("Logical error: empty tuple", ErrorCodes::LOGICAL_ERROR);

    Columns temporary_arrays(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        temporary_arrays[i] = ColumnArray(tuple.getColumns()[i], getOffsetsColumn()).filter(filt, result_size_hint);

    Block tuple_block = tuple.getData().cloneEmpty();
    for (size_t i = 0; i < tuple_size; ++i)
        tuple_block.getByPosition(i).column = static_cast<ColumnArray &>(*temporary_arrays[i]).getDataPtr();

    return std::make_shared<ColumnArray>(
        std::make_shared<ColumnTuple>(tuple_block),
        static_cast<ColumnArray &>(*temporary_arrays.front()).getOffsetsColumn());
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
        return std::make_shared<ColumnArray>(data);

    Permutation nested_perm(getOffsets().back());

    std::shared_ptr<ColumnArray> res = std::make_shared<ColumnArray>(data->cloneEmpty());

    Offsets_t & res_offsets = res->getOffsets();
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

void ColumnArray::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    size_t s = size();
    if (limit >= s)
        limit = 0;

    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<false>(*this, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<true>(*this, nan_direction_hint));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), less<false>(*this, nan_direction_hint));
        else
            std::sort(res.begin(), res.end(), less<true>(*this, nan_direction_hint));
    }
}


ColumnPtr ColumnArray::replicate(const Offsets_t & replicate_offsets) const
{
    if (typeid_cast<const ColumnUInt8 *>(data.get()))     return replicateNumber<UInt8>(replicate_offsets);
    if (typeid_cast<const ColumnUInt16 *>(data.get()))    return replicateNumber<UInt16>(replicate_offsets);
    if (typeid_cast<const ColumnUInt32 *>(data.get()))    return replicateNumber<UInt32>(replicate_offsets);
    if (typeid_cast<const ColumnUInt64 *>(data.get()))    return replicateNumber<UInt64>(replicate_offsets);
    if (typeid_cast<const ColumnInt8 *>(data.get()))      return replicateNumber<Int8>(replicate_offsets);
    if (typeid_cast<const ColumnInt16 *>(data.get()))     return replicateNumber<Int16>(replicate_offsets);
    if (typeid_cast<const ColumnInt32 *>(data.get()))     return replicateNumber<Int32>(replicate_offsets);
    if (typeid_cast<const ColumnInt64 *>(data.get()))     return replicateNumber<Int64>(replicate_offsets);
    if (typeid_cast<const ColumnFloat32 *>(data.get()))   return replicateNumber<Float32>(replicate_offsets);
    if (typeid_cast<const ColumnFloat64 *>(data.get()))   return replicateNumber<Float64>(replicate_offsets);
    if (typeid_cast<const ColumnString *>(data.get()))    return replicateString(replicate_offsets);
    if (dynamic_cast<const IColumnConst *>(data.get()))   return replicateConst(replicate_offsets);
    if (typeid_cast<const ColumnNullable *>(data.get()))  return replicateNullable(replicate_offsets);
    if (typeid_cast<const ColumnTuple *>(data.get()))     return replicateTuple(replicate_offsets);
    return replicateGeneric(replicate_offsets);
}


template <typename T>
ColumnPtr ColumnArray::replicateNumber(const Offsets_t & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ColumnPtr res = cloneEmpty();

    if (0 == col_size)
        return res;

    ColumnArray & res_ = typeid_cast<ColumnArray &>(*res);

    const typename ColumnVector<T>::Container_t & src_data = typeid_cast<const ColumnVector<T> &>(*data).getData();
    const Offsets_t & src_offsets = getOffsets();

    typename ColumnVector<T>::Container_t & res_data = typeid_cast<ColumnVector<T> &>(res_.getData()).getData();
    Offsets_t & res_offsets = res_.getOffsets();

    res_data.reserve(data->size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    Offset_t prev_replicate_offset = 0;
    Offset_t prev_data_offset = 0;
    Offset_t current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_data_offset;

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_new_offset += value_size;
            res_offsets.push_back(current_new_offset);

            res_data.resize(res_data.size() + value_size);
            memcpy(&res_data[res_data.size() - value_size], &src_data[prev_data_offset], value_size * sizeof(T));
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_data_offset = src_offsets[i];
    }

    return res;
}


ColumnPtr ColumnArray::replicateString(const Offsets_t & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ColumnPtr res = cloneEmpty();

    if (0 == col_size)
        return res;

    ColumnArray & res_ = static_cast<ColumnArray &>(*res);

    const ColumnString & src_string = typeid_cast<const ColumnString &>(*data);
    const ColumnString::Chars_t & src_chars = src_string.getChars();
    const Offsets_t & src_string_offsets = src_string.getOffsets();
    const Offsets_t & src_offsets = getOffsets();

    ColumnString::Chars_t & res_chars = typeid_cast<ColumnString &>(res_.getData()).getChars();
    Offsets_t & res_string_offsets = typeid_cast<ColumnString &>(res_.getData()).getOffsets();
    Offsets_t & res_offsets = res_.getOffsets();

    res_chars.reserve(src_chars.size() / col_size * replicate_offsets.back());
    res_string_offsets.reserve(src_string_offsets.size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    Offset_t prev_replicate_offset = 0;

    Offset_t prev_src_offset = 0;
    Offset_t prev_src_string_offset = 0;

    Offset_t current_res_offset = 0;
    Offset_t current_res_string_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        /// How much to replicate the array.
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        /// The number of rows in the array.
        size_t value_size = src_offsets[i] - prev_src_offset;
        /// Number of characters in rows of the array, including zero/null bytes.
        size_t sum_chars_size = value_size == 0 ? 0 : (src_string_offsets[prev_src_offset + value_size - 1] - prev_src_string_offset);

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_res_offset += value_size;
            res_offsets.push_back(current_res_offset);

            size_t prev_src_string_offset_local = prev_src_string_offset;
            for (size_t k = 0; k < value_size; ++k)
            {
                /// Size of one row.
                size_t chars_size = src_string_offsets[k + prev_src_offset] - prev_src_string_offset_local;

                current_res_string_offset += chars_size;
                res_string_offsets.push_back(current_res_string_offset);

                prev_src_string_offset_local += chars_size;
            }

            /// Copies the characters of the array of rows.
            res_chars.resize(res_chars.size() + sum_chars_size);
            memcpySmallAllowReadWriteOverflow15(
                &res_chars[res_chars.size() - sum_chars_size], &src_chars[prev_src_string_offset], sum_chars_size);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_src_offset = src_offsets[i];
        prev_src_string_offset += sum_chars_size;
    }

    return res;
}


ColumnPtr ColumnArray::replicateConst(const Offsets_t & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == col_size)
        return cloneEmpty();

    const Offsets_t & src_offsets = getOffsets();

    auto res_column_offsets = std::make_shared<ColumnOffsets_t>();
    Offsets_t & res_offsets = res_column_offsets->getData();
    res_offsets.reserve(replicate_offsets.back());

    Offset_t prev_replicate_offset = 0;
    Offset_t prev_data_offset = 0;
    Offset_t current_new_offset = 0;

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

    return std::make_shared<ColumnArray>(getData().cloneResized(current_new_offset), res_column_offsets);
}


ColumnPtr ColumnArray::replicateGeneric(const Offsets_t & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ColumnPtr res = cloneEmpty();
    ColumnArray & res_concrete = static_cast<ColumnArray &>(*res);

    if (0 == col_size)
        return res;

    IColumn::Offset_t prev_offset = 0;
    const auto & offsets_data = getOffsets();
    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = offsets_data[i] - prev_offset;
        prev_offset = offsets_data[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
            res_concrete.insertFrom(*this, i);
    }

    return res;
}


ColumnPtr ColumnArray::replicateNullable(const Offsets_t & replicate_offsets) const
{
    const ColumnNullable & nullable = static_cast<const ColumnNullable &>(*data);

    /// Make temporary arrays for each components of Nullable. Then replicate them independently and collect back to result.
    /// NOTE Offsets are calculated twice and it is redundant.

    auto array_of_nested = ColumnArray(nullable.getNestedColumn(), getOffsetsColumn()).replicate(replicate_offsets);
    auto array_of_null_map = ColumnArray(nullable.getNullMapColumn(), getOffsetsColumn()).replicate(replicate_offsets);

    return std::make_shared<ColumnArray>(
        std::make_shared<ColumnNullable>(
            static_cast<ColumnArray &>(*array_of_nested).getDataPtr(),
            static_cast<ColumnArray &>(*array_of_null_map).getDataPtr()),
        static_cast<ColumnArray &>(*array_of_nested).getOffsetsColumn());
}


ColumnPtr ColumnArray::replicateTuple(const Offsets_t & replicate_offsets) const
{
    const ColumnTuple & tuple = static_cast<const ColumnTuple &>(*data);

    /// Make temporary arrays for each components of Tuple. In the same way as for Nullable.

    size_t tuple_size = tuple.getColumns().size();

    if (tuple_size == 0)
        throw Exception("Logical error: empty tuple", ErrorCodes::LOGICAL_ERROR);

    Columns temporary_arrays(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        temporary_arrays[i] = ColumnArray(tuple.getColumns()[i], getOffsetsColumn()).replicate(replicate_offsets);

    Block tuple_block = tuple.getData().cloneEmpty();
    for (size_t i = 0; i < tuple_size; ++i)
        tuple_block.getByPosition(i).column = static_cast<ColumnArray &>(*temporary_arrays[i]).getDataPtr();

    return std::make_shared<ColumnArray>(
        std::make_shared<ColumnTuple>(tuple_block),
        static_cast<ColumnArray &>(*temporary_arrays.front()).getOffsetsColumn());
}


void ColumnArray::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

}
