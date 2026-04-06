#pragma once

#include <Processors/IAccumulatingTransform.h>
#include <Processors/Port.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Core/Types.h>

namespace DB
{
    struct ShuffleSamplingInfo;
    using ShuffleSamplingInfoPtr = std::shared_ptr<ShuffleSamplingInfo>;

    class ShuffleTransform final : public IAccumulatingTransform
    {
    public:
        ShuffleTransform(SharedHeader header, size_t limit_);
        String getName() const override { return "ShuffleTransform"; }

    protected:
        void consume(Chunk chunk) override;
        Chunk generate() override;

    private:
        size_t limit;
        size_t total_rows = 0;
        Chunks accumulated;
        pcg64 rng{randomSeed()};

        Chunks reservoir;

    };

    class PartialShuffleTransform final : public IAccumulatingTransform
    {
    public:
        PartialShuffleTransform(SharedHeader header, size_t limit_);
        String getName() const override { return "PartialShuffleTransform"; }

    protected:
        void consume(Chunk chunk) override;
        Chunk generate() override;

    private:
        struct SampledRow
        {
            UInt64 priority;
            Chunk row;
        };

        struct SampledRowComparator
        {
            bool operator()(const SampledRow & lhs, const SampledRow & rhs) const
            {
                return lhs.priority < rhs.priority;
            }
        };

        size_t limit;
        bool generated = false;
        pcg64 rng{randomSeed()};
        std::vector<SampledRow> reservoir;
    };

    class MergingShuffleTransform final : public IAccumulatingTransform
    {
    public:
        MergingShuffleTransform(SharedHeader header, size_t limit_);
        String getName() const override { return "MergingShuffleTransform"; }

    protected:
        void consume(Chunk chunk) override;
        Chunk generate() override;

    private:
        struct SampledRow
        {
            UInt64 priority;
            Chunk row;
        };

        struct SampledRowComparator
        {
            bool operator()(const SampledRow & lhs, const SampledRow & rhs) const
            {
                return lhs.priority < rhs.priority;
            }
        };

        size_t limit;
        bool generated = false;
        std::vector<SampledRow> reservoir;
    };
}
