#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <Processors/ISimpleTransform.h>
#include <Poco/Logger.h>
#include <Interpreters/Set.h>

namespace DB
{

class ColumnPermuteTransform : public ISimpleTransform
{
public:
    ColumnPermuteTransform(const Block & header_, const std::vector<size_t> & permutation_);

    String getName() const override { return "ColumnPermuteTransform"; }

    void transform(Chunk & chunk) override;

    static Block permute(const Block & block, const std::vector<size_t> & permutation);

private:
    Names column_names;
    std::vector<size_t> permutation;
};


}
