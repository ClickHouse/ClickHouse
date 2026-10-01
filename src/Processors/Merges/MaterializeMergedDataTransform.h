#pragma once

#include <Processors/ISimpleTransform.h>

#include <memory>
#include <utility>

namespace DB
{

class MergingSortedTransformStats;

/// Materializes output chunks planned by a `MergingSortedAlgorithm` running in
/// deferred mode. Multiple instances may replay independent plans in parallel.
class MaterializeMergedDataTransform final : public ISimpleTransform
{
public:
    MaterializeMergedDataTransform(SharedHeader header, std::shared_ptr<MergingSortedTransformStats> stats_)
        : ISimpleTransform(header, header, false)
        , stats(std::move(stats_))
    {
    }

    String getName() const override { return "MaterializeMergedDataTransform"; }

    void transform(Chunk & chunk) override;

protected:
    void onFinish() override;

private:
    std::shared_ptr<MergingSortedTransformStats> stats;
    UInt64 materialized_bytes = 0;
    UInt64 materialization_elapsed_ns = 0;
};

}
