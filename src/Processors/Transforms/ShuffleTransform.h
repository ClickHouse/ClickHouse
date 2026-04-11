#pragma once

#include <Columns/IColumn_fwd.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Port.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Core/Types.h>
#include <limits>

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
        MutableColumns reservoir_columns;
        std::vector<UInt64> reservoir_priorities;
        UInt64 reservoir_threshold = std::numeric_limits<UInt64>::max();
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
        size_t limit;
        bool generated = false;
        pcg64 rng{randomSeed()};
        MutableColumns reservoir_columns;
        std::vector<UInt64> reservoir_priorities;
        UInt64 reservoir_threshold = std::numeric_limits<UInt64>::max();
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
        size_t limit;
        bool generated = false;
        MutableColumns reservoir_columns;
        std::vector<UInt64> reservoir_priorities;
        UInt64 reservoir_threshold = std::numeric_limits<UInt64>::max();
    };
}
