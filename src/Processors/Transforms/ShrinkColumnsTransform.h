#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Shrinks over-allocated columns of each chunk to fit. Placed right after a column is produced
/// (parsing, materialization) where the chunk is uniquely owned, so shrinking does not clone shared
/// columns. Reduces peak memory on INSERT. Controlled by `shrink_over_allocated_columns_min_waste_ratio`
/// (> 1 to enable) and `shrink_over_allocated_columns_min_waste_bytes`.
class ShrinkColumnsTransform : public ISimpleTransform
{
public:
    ShrinkColumnsTransform(SharedHeader header, double min_waste_ratio_, size_t min_waste_bytes_);

    String getName() const override { return "ShrinkColumnsTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    double min_waste_ratio;
    size_t min_waste_bytes;
};

}
