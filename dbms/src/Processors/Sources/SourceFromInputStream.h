#pragma once
#include <Processors/Sources/SourceWithProgress.h>

namespace DB
{

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

/// Wrapper for IBlockInputStream which implements ISourceWithProgress.
class SourceFromInputStream : public ISourceWithProgress
{
public:
    explicit SourceFromInputStream(BlockInputStreamPtr stream_, bool force_add_aggregating_info_ = false);
    String getName() const override { return "SourceFromInputStream"; }

    Status prepare() override;
    void work() override;

    Chunk generate() override;

    IBlockInputStream & getStream() { return *stream; }

    void addTotalsPort();

    /// Implementation for methods from ISourceWithProgress.
    void setLimits(const LocalLimits & limits_) final { stream->setLimits(limits_); }
    void setQuota(const std::shared_ptr<QuotaContext> & quota_) final { stream->setQuota(quota_); }
    void setProcessListElement(QueryStatus * elem) final { stream->setProcessListElement(elem); }
    void setProgressCallback(const ProgressCallback & callback) final { stream->setProgressCallback(callback); }
    void addTotalRowsApprox(size_t value) final { stream->addTotalRowsApprox(value); }

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
