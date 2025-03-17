#pragma once
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/BloomFilter.h>
#include "base/types.h"

namespace DB
{

class DistinctTransform : public ISimpleTransform
{
public:
    DistinctTransform(
        const Block & header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        bool is_pre_distinct_);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    SetVariants data;
    std::unique_ptr<BloomFilter> bloom_filter;

    size_t bf_passed = 0;
    size_t new_passes = 0;
    const bool is_pre_distinct;
    bool leaky = false;

    Sizes key_sizes;
    const UInt64 limit_hint;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;

    template <typename Method>
    void buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variant,
        size_t & passed) const;
};

}
