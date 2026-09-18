#pragma once

#include <Core/Field.h>
#include <Processors/IAccumulatingTransform.h>
#include <Common/HashTable/HashSet.h>

#include <vector>


namespace DB
{

/// Applies the bounded PromQL `topk`/`bottomk` operator to the already merged
/// `(group, values)` output of `PromQLRangeSumByTransform`.
///
/// The input is a single merged stream. Selection is performed independently
/// for every grid step; an output row is retained if it wins at least one step,
/// and non-winning steps are represented by NULL values.
class PromQLRangeTopKByTransform final : public IAccumulatingTransform
{
public:
    PromQLRangeTopKByTransform(SharedHeader input_header_, UInt64 k_, bool bottomk_);

    String getName() const override { return "PromQLRangeTopKBy"; }

    static SharedHeader transformHeader(const SharedHeader & input_header);

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    struct Row
    {
        UInt64 group = 0;
        Array values;
    };

    static bool isBetter(const Field & lhs, UInt64 lhs_group, const Field & rhs, UInt64 rhs_group, bool bottomk);
    void validateValues(const Array & values) const;

    const UInt64 k;
    const bool bottomk;
    size_t group_position = 0;
    size_t values_position = 0;
    size_t num_steps = 0;
    bool has_num_steps = false;
    bool generated = false;

    std::vector<Row> rows;
    HashSet<UInt64, HashCRC32<UInt64>> seen_groups;
};

}
