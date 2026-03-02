#ifdef ENABLE_FSST

#include <algorithm>
#include <cassert>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <Columns/ColumnFSST.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>


#include <fsst.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int PARAMETER_OUT_OF_BOUND;
}

std::optional<size_t> ColumnFSST::batchByRow(size_t row) const
{
    if (decoders.empty() || decoders[0].batch_start_index > row)
    {
        return std::nullopt;
    }

    size_t batch_ind
        = --std::lower_bound(
              decoders.begin(), decoders.end(), row, [](const BatchDsc & dsc, size_t value) { return dsc.batch_start_index <= value; })
        - decoders.begin();

    return batch_ind;
}

void ColumnFSST::decompressRow(size_t row_num, String & out) const
{
    auto batch_ind = batchByRow(row_num);
    if (!batch_ind.has_value())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "access out of bound");
    }

    auto compressed_data = string_column->getDataAt(row_num);
    const auto & batch_dsc = decoders[batch_ind.value()];

    if (out.capacity() < origin_lengths[row_num])
    {
        out.reserve(origin_lengths[row_num]);
    }

    fsst_decompress(
        reinterpret_cast<::fsst_decoder_t *>(batch_dsc.decoder.get()),
        compressed_data.size(),
        reinterpret_cast<const unsigned char *>(compressed_data.data()),
        origin_lengths[row_num],
        reinterpret_cast<unsigned char *>(out.data()));
}

Field ColumnFSST::operator[](size_t n) const
{
    String string(n, ' ');
    Field result(std::move(string));
    get(n, result);
    return result;
}

void ColumnFSST::get(size_t n, Field & res) const
{
    String uncompressed_string(origin_lengths[n], ' ');
    decompressRow(n, uncompressed_string);
    res = std::move(uncompressed_string);
}

void ColumnFSST::appendNewBatch(const CompressedField & x, std::shared_ptr<fsst_decoder_t> decoder)
{
    decoders.emplace_back(BatchDsc{.decoder = decoder, .batch_start_index = string_column->size()});
    append(x);
}
void ColumnFSST::append(const CompressedField & x)
{
    string_column->insert(x.value);
    origin_lengths.push_back(x.uncompressed_size);
}

void ColumnFSST::popBack(size_t n)
{
    string_column->popBack(n);
    while (n-- && !origin_lengths.empty())
    {
        origin_lengths.pop_back();
    }
    while (!decoders.empty() && decoders.back().batch_start_index >= origin_lengths.size())
    {
        decoders.pop_back();
    }
}

void ColumnFSST::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto * src_fsst = assert_cast<const ColumnFSST *>(&src);
    if (src.size() < start + length)
    {
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Parameter out of bound in ColumnFSST::insertRangeFrom method.");
    }

    size_t length_before_insert = string_column->size();
    string_column->insertRangeFrom(*src_fsst->string_column, start, length);
    origin_lengths.insert(origin_lengths.end(), src_fsst->origin_lengths.begin() + start, src_fsst->origin_lengths.begin() + length);

    auto first_batch_to_insert = src_fsst->batchByRow(start).value();
    while (first_batch_to_insert < src_fsst->decoders.size()
           && src_fsst->decoders[first_batch_to_insert].batch_start_index < start + length)
    {
        decoders.emplace_back(src_fsst->decoders[first_batch_to_insert]);
        decoders.back().batch_start_index
            = length_before_insert + std::max(0ul, src_fsst->decoders[first_batch_to_insert].batch_start_index - start);
        ++first_batch_to_insert;
    }
}

/*
    Just decompress for now
    TODO: implement comparison on compressed data
*/
int ColumnFSST::doCompareAt(size_t n, size_t m, const IColumn & rhs, int /* nan_direction_hint */) const
{
    const auto * rhs_fsst = assert_cast<const ColumnFSST *>(&rhs);

    String lhs_val;
    String rhs_val;

    decompressRow(n, lhs_val);
    rhs_fsst->decompressRow(m, rhs_val);

    return memcmpSmallAllowOverflow15(lhs_val.data(), lhs_val.size(), rhs_val.data(), rhs_val.size());
}

size_t ColumnFSST::byteSize() const
{
    return string_column->byteSize() + origin_lengths.size() * sizeof(UInt64) + decoders.size() * sizeof(BatchDsc);
}

size_t ColumnFSST::byteSizeAt(size_t n) const
{
    return string_column->byteSizeAt(n) + sizeof(origin_lengths[n]);
}

size_t ColumnFSST::allocatedBytes() const
{
    return byteSize();
}

bool ColumnFSST::isDefaultAt(size_t n) const
{
    return string_column->isDefaultAt(n);
}

void ColumnFSST::filterInnerData(const Filter & filt, std::vector<UInt64> & lengths_out, std::vector<BatchDsc> & decoders_out) const
{
    size_t dsc_ind = 0;
    for (size_t row = 0; row < std::min(filt.size(), origin_lengths.size()); row++)
    {
        if (!filt[row])
        {
            continue;
        }

        lengths_out.emplace_back(origin_lengths[row]);
        while (dsc_ind < decoders.size() && decoders[dsc_ind].batch_start_index <= row)
        {
            decoders_out.emplace_back(decoders[dsc_ind]);
            decoders_out.back().batch_start_index = lengths_out.size() - 1;
            ++dsc_ind;
        }
    }
}

struct ColumnFSST::ComparatorBase
{
    const ColumnFSST & parent;

    explicit ComparatorBase(const ColumnFSST & parent_)
        : parent(parent_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        String lhs_val;
        String rhs_val;

        parent.decompressRow(lhs, lhs_val);
        parent.decompressRow(rhs, rhs_val);

        return memcmpSmallAllowOverflow15(lhs_val.data(), lhs_val.size(), rhs_val.data(), rhs_val.size());
    }
};

void ColumnFSST::getPermutation(
    PermutationSortDirection direction,
    PermutationSortStability stability,
    size_t limit,
    int /*nan_direction_hint*/,
    Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this), DefaultSort(), DefaultPartialSort());
}

void ColumnFSST::updatePermutation(
    PermutationSortDirection direction,
    PermutationSortStability stability,
    size_t limit,
    int /*nan_direction_hint*/,
    Permutation & res,
    EqualRanges & equal_ranges) const
{
    auto eq_cmp = ComparatorEqual(*this);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this), eq_cmp, DefaultSort(), DefaultPartialSort());
}

void ColumnFSST::getExtremes(Field & min, Field & max) const
{
    min = String();
    max = String();

    size_t min_idx = 0;
    size_t max_idx = 0;

    ComparatorBase cmp_op(*this);

    for (size_t i = 0; i < size(); ++i)
    {
        if (cmp_op.compare(i, min_idx) < 0)
            min_idx = i;
        else if (cmp_op.compare(max_idx, i) < 0)
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}


ColumnPtr ColumnFSST::replicate(const Offsets & offsets) const
{
    auto replicated_string_column = string_column->replicate(offsets);

    std::vector<UInt64> replicated_origin_lengths;
    std::vector<BatchDsc> replicated_decoders;

    replicated_origin_lengths.reserve(replicated_string_column->size());
    replicated_decoders.reserve(decoders.size());

    size_t dsc_ind = 0;
    for (size_t row = 0; row < offsets.size(); row++)
    {
        if (offsets[row] == 0)
        {
            continue;
        }

        while (dsc_ind < decoders.size() && decoders[dsc_ind].batch_start_index <= row)
        {
            ++dsc_ind;
        }

        if (dsc_ind > 0 && dsc_ind <= decoders.size() && decoders[dsc_ind - 1].batch_start_index <= row)
        {
            replicated_decoders.emplace_back(decoders[dsc_ind]);
            replicated_decoders.back().batch_start_index = replicated_origin_lengths.size();
            ++dsc_ind;
        }


        for (size_t ind = 0; ind < offsets[row]; ind++)
        {
            replicated_origin_lengths.emplace_back(origin_lengths[row]);
        }
    }

    return ColumnFSST::create(std::move(replicated_string_column), replicated_decoders, replicated_origin_lengths);
}

ColumnPtr ColumnFSST::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (string_column->empty())
    {
        return cloneEmpty();
    }

    auto filtered_string_column = string_column->filter(filt, result_size_hint);

    std::vector<BatchDsc> filtered_decoders;
    std::vector<UInt64> filtered_origin_lengths;

    filtered_origin_lengths.reserve(result_size_hint > 0 ? result_size_hint : string_column->size());
    filterInnerData(filt, filtered_origin_lengths, filtered_decoders);

    return ColumnFSST::create(std::move(filtered_string_column), filtered_decoders, filtered_origin_lengths);
}

void ColumnFSST::filter(const Filter & filt)
{
    if (string_column->empty())
    {
        return;
    }

    string_column->filter(filt);

    std::vector<BatchDsc> filtered_decoders;
    std::vector<UInt64> filtered_origin_lengths;

    filterInnerData(filt, filtered_origin_lengths, filtered_decoders);

    origin_lengths = std::move(filtered_origin_lengths);
    decoders = std::move(filtered_decoders);
}

ColumnPtr recursiveRemoveFSST(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_fsst = typeid_cast<const ColumnFSST *>(column.get()))
    {
        auto column_string = ColumnString::create();
        for (size_t ind = 0; ind < column_fsst->size(); ind++)
        {
            column_string->insert((*column_fsst)[ind]);
        }
        return column_string;
    }

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        if (columns.empty())
            return column;

        for (auto & element : columns)
            element = recursiveRemoveFSST(element);

        return ColumnTuple::create(columns);
    }

    return column;
}

};

#endif
