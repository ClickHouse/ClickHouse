#pragma once

#include <Columns/ColumnLowCardinality.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/ColumnsHashing.h>

#include <unordered_map>

namespace DB
{

class DistinctTransform : public ISimpleTransform
{
public:
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    SetVariants data;
    Sizes key_sizes;
    const UInt64 limit_hint;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;

    using LCDictionaryKey = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKey;
    using LCDictionaryKeyHash = ColumnsHashing::LowCardinalityDictionaryCache::DictionaryKeyHash;

    struct LCDictState
    {
        /// seen_indices[idx] == 1 means dictionary index `idx` has been seen
        /// at least once for this dictionary identity.
        PaddedPODArray<UInt8> seen_indices;

        /// Number of dictionary indices we have seen at least once. When this
        /// reaches the dictionary size, any future row for the parent chunk cannot
        /// introduce a new distinct value.
        UInt64 seen_count = 0;
    };

    /// Per-dictionary state which may cover multiple IColumns.
    std::unordered_map<LCDictionaryKey, LCDictState, LCDictionaryKeyHash> lc_dict_states;

    /// mask[i] == 0 -> row i is known duplicate (by LC index) and is never inserted.
    template <typename Method>
    void buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants,
        const IColumn::Filter * mask) const;

    /// For a single LowCardinality key column, build a mask of rows that are
    /// the first occurrence of their LC dictionary index for this dictionary identity. Then, only those
    /// rows need to be checked for distinctness.
    IColumn::Filter buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows);
};

}
