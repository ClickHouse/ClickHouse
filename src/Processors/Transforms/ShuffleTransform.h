#pragma once

#include <Processors/IAccumulatingTransform.h>
#include <Processors/Port.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

namespace DB
{
    class ShuffleTransform : public IAccumulatingTransform
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
}
