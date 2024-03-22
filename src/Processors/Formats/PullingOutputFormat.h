#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <QueryPipeline/ProfileInfo.h>

namespace DB
{

/// Output format which is used in PullingPipelineExecutor.
class PullingOutputFormat : public IOutputFormat
{
public:
    explicit PullingOutputFormat(const Block & header, std::atomic_bool & consume_data_flag_)
        : IOutputFormat(header, out)
        , has_data_flag(consume_data_flag_)
    {}

    String getName() const override { return "PullingOutputFormat"; }

    Chunk getChunk();
    Chunk getTotals();
    Chunk getExtremes();

    ProfileInfo & getProfileInfo() { return info; }

    void setRowsBeforeLimit(size_t rows_before_limit) override;

    bool expectMaterializedColumns() const override { return false; }

protected:
    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }

private:
    Chunk data;
    Chunk totals;
    Chunk extremes;

    std::atomic_bool & has_data_flag;

    ProfileInfo info;

    /// Is not used.
    static WriteBuffer out;
};

}
