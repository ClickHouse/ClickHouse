#pragma once
#include <Processors/ISource.h>

namespace DB
{

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

class SourceFromInputStream : public ISource
{
public:
    explicit SourceFromInputStream(BlockInputStreamPtr stream_, bool force_add_aggregating_info_ = false);
    String getName() const override { return "SourceFromInputStream"; }

    Status prepare() override;
    void work() override;

    Chunk generate() override;

    IBlockInputStream & getStream() { return *stream; }

    void addTotalsPort();

private:
    bool has_aggregate_functions = false;
    bool force_add_aggregating_info;
    BlockInputStreamPtr stream;

    Chunk totals;
    bool has_totals_port = false;
    bool has_totals = false;

    bool is_generating_finished = false;
    bool is_stream_finished = false;
    bool is_stream_started = false;
};

}
