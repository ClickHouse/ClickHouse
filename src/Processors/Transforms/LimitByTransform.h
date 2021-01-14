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
    LimitByTransform(const Block & header, UInt64 group_length_, UInt64 group_offset_, const Names & columns);

    String getName() const override { return "LimitByTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    using MapHashed = HashMap<UInt128, UInt64, UInt128TrivialHash>;

    MapHashed keys_counts;
    std::vector<size_t> key_positions;
    const UInt64 group_length;
    const UInt64 group_offset;
};

}
