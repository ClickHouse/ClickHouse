#pragma once
#include <Processors/ISource.h>
#include <Processors/Sources/InputStreamHolder.h>

namespace DB
{

class SourceFromInputStream : public ISource
{
public:
    explicit SourceFromInputStream(InputStreamHolderPtr holder_, bool force_add_aggregating_info = false);
    String getName() const override { return "SourceFromInputStream"; }

    Chunk generate() override;

    IBlockInputStream & getStream() { return holder->getStream(); }

private:
    bool has_aggregate_functions = false;
    bool force_add_aggregating_info;
    InputStreamHolderPtr holder;
};

}
