#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Processors/Pipe.h>

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

        processors = std::move(pipe).detachProcessors();
        init();
    }

    String getName() const override { return root->getName(); }
    Block getHeader() const override { return root->getOutputs().front().getHeader(); }

    /// This methods does not affect TreeExecutor as IBlockInputStream itself.
    /// They just passed to all SourceWithProgress processors.
    void setProgressCallback(const ProgressCallback & callback) final;
    void setProcessListElement(QueryStatus * elem) final;
    void setLimits(const LocalLimits & limits_) final;
    void setQuota(const QuotaContextPtr & quota_) final;
    void addTotalRowsApprox(size_t value) final;

protected:
    Block readImpl() override;

private:
    OutputPort & output_port;
    Processors processors;
    IProcessor * root = nullptr;
    std::unique_ptr<InputPort> input_port;

    /// Remember sources that support progress.
    std::vector<ISourceWithProgress *> sources_with_progress;

    QueryStatus * process_list_element = nullptr;

    void init();
    /// Execute tree step-by-step until root returns next chunk or execution is finished.
    void execute();

    /// Moved from pipe.
    std::vector<std::shared_ptr<Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;
};

}
