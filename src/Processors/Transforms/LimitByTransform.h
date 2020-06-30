#pragma once
#include <Processors/ISimpleTransform.h>
#include <Common/HashTable/HashMap.h>
#include <Common/UInt128.h>

namespace DB
{

/// Executes LIMIT BY for specified columns.
class LimitByTransform : public ISimpleTransform
{
public:
    LimitByTransform(const Block & header, size_t group_length_, size_t group_offset_, const Names & columns);

    String getName() const override { return "LimitByTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    using MapHashed = HashMap<UInt128, UInt64, UInt128TrivialHash>;

    MapHashed keys_counts;
    std::vector<size_t> key_positions;
    const size_t group_length;
    const size_t group_offset;
};

}
