#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Processors/IProcessor.h>

namespace DB
{

class ISourceWithProgress;

class TreeExecutor : public IBlockInputStream
{
public:
    explicit TreeExecutor(Processors processors_) : processors(std::move(processors_)) { init(); }

    String getName() const override { return root->getName(); }
    Block getHeader() const override { return root->getOutputs().front().getHeader(); }

    /// This methods does not affect TreeExecutor as IBlockInputStream itself.
    /// They just passed to all SourceWithProgress processors.
    void setProgressCallback(const ProgressCallback & callback) final;
    void setProcessListElement(QueryStatus * elem) final;
    void setLimits(const LocalLimits & limits_) final;
    void setQuota(QuotaForIntervals & quota_) final;
    void addTotalRowsApprox(size_t value) final;

protected:
    Block readImpl() override;

private:
    Processors processors;
    IProcessor * root = nullptr;
    std::unique_ptr<InputPort> port;

    /// Remember sources that support progress.
    std::vector<ISourceWithProgress *> sources_with_progress;

    void init();
    void execute();
};

}
