#include <IO/WriteHelpers.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <Common/WeakHash.h>
#include <Common/HashTable/Hash.h>

#include <base/defines.h>

#if defined(MEMORY_SANITIZER)
    #include <sanitizer/msan_interface.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

ColumnConst::ColumnConst(const ColumnPtr & data_, size_t s_)
    : data(data_), s(s_)
{
    /// Squash Const of Const.
    while (const ColumnConst * const_data = typeid_cast<const ColumnConst *>(data.get()))
        data = const_data->getDataColumnPtr();

    if (data->size() != 1)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                        "Incorrect size of nested column in constructor of ColumnConst: {}, must be 1.", data->size());

    /// Check that the value is initialized. We do it earlier, before it will be used, to ease debugging.
#if defined(MEMORY_SANITIZER)
    if (data->isFixedAndContiguous())
    {
        StringRef value = data->getDataAt(0);
        __msan_check_mem_is_initialized(value.data, value.size);
    }
#endif
}

ColumnPtr ColumnConst::convertToFullColumn() const
{
    return data->replicate(Offsets(1, s));
}

ColumnPtr ColumnConst::removeLowCardinality() const
{
    return ColumnConst::create(data->convertToFullColumnIfLowCardinality(), s);
}

ColumnPtr ColumnConst::filter(const Filter & filt, ssize_t /*result_size_hint*/) const
{
    if (s != filt.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})",
            filt.size(), toString(s));

    size_t new_size = countBytesInFilter(filt);
    return ColumnConst::create(data, new_size);
}

void ColumnConst::expand(const Filter & mask, bool inverted)
{
    if (mask.size() < s)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mask size should be no less than data size.");

    size_t bytes_count = countBytesInFilter(mask);
    if (inverted)
        bytes_count = mask.size() - bytes_count;

    if (bytes_count < s)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not enough bytes in mask");
    else if (bytes_count > s)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many bytes in mask");

    s = mask.size();
}


ColumnPtr ColumnConst::replicate(const Offsets & offsets) const
{
    if (s != offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets ({}) doesn't match size of column ({})",
            offsets.size(), toString(s));

    size_t replicated_size = 0 == s ? 0 : offsets.back();
    return ColumnConst::create(data, replicated_size);
}

ColumnPtr ColumnConst::permute(const Permutation & perm, size_t limit) const
{
    limit = getLimitForPermutation(size(), perm.size(), limit);
    return ColumnConst::create(data, limit);
}

ColumnPtr ColumnConst::index(const IColumn & indexes, size_t limit) const
{
    if (limit == 0)
        limit = indexes.size();

    if (indexes.size() < limit)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of indexes ({}) is less than required ({})",
                        indexes.size(), toString(limit));

    return ColumnConst::create(data, limit);
}

MutableColumns ColumnConst::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    if (s != selector.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of selector ({}) doesn't match size of column ({})",
            selector.size(), toString(s));

    std::vector<size_t> counts = countColumnsSizeInSelector(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        res[i] = cloneResized(counts[i]);

    return res;
}

void ColumnConst::getPermutation(PermutationSortDirection /*direction*/, PermutationSortStability /*stability*/,
                                size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res) const
{
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

void ColumnConst::updatePermutation(PermutationSortDirection /*direction*/, PermutationSortStability /*stability*/,
                                size_t, int, Permutation &, EqualRanges &) const
{
}

void ColumnConst::updateWeakHash32(WeakHash32 & hash) const
{
    if (hash.getData().size() != s)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of WeakHash32 does not match size of column: "
                        "column size is {}, hash size is {}", std::to_string(s), std::to_string(hash.getData().size()));

    WeakHash32 element_hash(1);
    data->updateWeakHash32(element_hash);
    size_t data_hash = element_hash.getData()[0];

    for (auto & value : hash.getData())
        value = static_cast<UInt32>(intHashCRC32(data_hash, value));
}

void ColumnConst::compareColumn(
    const IColumn & rhs, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> & compare_results, int, int nan_direction_hint)
    const
{
    Int8 res = compareAt(1, 1, rhs, nan_direction_hint);
    std::fill(compare_results.begin(), compare_results.end(), res);
}

}
