#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Processors/Pipe.h>
#include <Processors/RowsBeforeLimitCounter.h>

namespace DB
{

class ISourceWithProgress;

/// It's a wrapper from processors tree-shaped pipeline to block input stream.
/// Execute all processors in a single thread, by in-order tree traverse.
/// Also, support for progress and quotas.
class TreeExecutorBlockInputStream : public IBlockInputStream
{
public:
    /// Last processor in list must be a tree root.
    /// It is checked that
    ///  * processors form a tree
    ///  * all processors are attainable from root
    ///  * there is no other connected processors
    explicit TreeExecutorBlockInputStream(Pipe pipe) : output_port(pipe.getPort())
    {
        for (auto & table_lock : pipe.getTableLocks())
            addTableLock(table_lock);

        for (auto & storage : pipe.getStorageHolders())
            storage_holders.emplace_back(storage);

        for (auto & context : pipe.getContexts())
            interpreter_context.emplace_back(context);

        totals_port = pipe.getTotalsPort();
        extremes_port = pipe.getExtremesPort();
        processors = std::move(pipe).detachProcessors();
        init();
    }

    String getName() const override { return "TreeExecutor"; }
    Block getHeader() const override { return root->getOutputs().front().getHeader(); }

    void cancel(bool kill) override;

    /// This methods does not affect TreeExecutor as IBlockInputStream itself.
    /// They just passed to all SourceWithProgress processors.
    void setProgressCallback(const ProgressCallback & callback) final;
    void setProcessListElement(QueryStatus * elem) final;
    void setLimits(const LocalLimits & limits_) final;
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota_) final;
    void addTotalRowsApprox(size_t value) final;

protected:
    Block readImpl() override;

private:
    OutputPort & output_port;
    OutputPort * totals_port = nullptr;
    OutputPort * extremes_port = nullptr;
    Processors processors;
    IProcessor * root = nullptr;
    std::unique_ptr<InputPort> input_port;
    std::unique_ptr<InputPort> input_totals_port;
    std::unique_ptr<InputPort> input_extremes_port;
    RowsBeforeLimitCounterPtr rows_before_limit_at_least;

    /// Remember sources that support progress.
    std::vector<ISourceWithProgress *> sources_with_progress;

    QueryStatus * process_list_element = nullptr;

    void init();
    /// Execute tree step-by-step until root returns next chunk or execution is finished.
    void execute(bool on_totals, bool on_extremes);

    void initRowsBeforeLimit();

    /// Moved from pipe.
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;
};

}
