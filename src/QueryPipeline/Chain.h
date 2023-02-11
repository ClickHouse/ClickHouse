#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/PipelineResourcesHolder.h>

namespace DB
{

class Chain
{
public:
    Chain() = default;
    Chain(Chain &&) = default;
    Chain(const Chain &) = delete;

    Chain & operator=(Chain &&) = default;
    Chain & operator=(const Chain &) = delete;

    explicit Chain(ProcessorPtr processor);
    explicit Chain(std::list<ProcessorPtr> processors);

    bool empty() const { return processors.empty(); }

    size_t getNumThreads() const { return num_threads; }
    void setNumThreads(size_t num_threads_) { num_threads = num_threads_; }

    void addSource(ProcessorPtr processor);
    void addSink(ProcessorPtr processor);

    IProcessor & getSource();
    IProcessor & getSink();

    InputPort & getInputPort() const;
    OutputPort & getOutputPort() const;

    const Block & getInputHeader() const { return getInputPort().getHeader(); }
    const Block & getOutputHeader() const { return getOutputPort().getHeader(); }

    const std::list<ProcessorPtr> & getProcessors() const { return processors; }
    static std::list<ProcessorPtr> getProcessors(Chain chain) { return std::move(chain.processors); }

    void addTableLock(TableLockHolder lock) { holder.table_locks.emplace_back(std::move(lock)); }
    void addStorageHolder(StoragePtr storage) { holder.storage_holders.emplace_back(std::move(storage)); }
    void attachResources(PipelineResourcesHolder holder_) { holder = std::move(holder_); }
    void addInterpreterContext(ContextPtr context) { holder.interpreter_context.emplace_back(std::move(context)); }
    PipelineResourcesHolder detachResources() { return std::move(holder); }

    void reset();

private:
    PipelineResourcesHolder holder;

    /// -> source -> transform -> ... -> transform -> sink ->
    ///  ^        ->           ->     ->           ->       ^
    ///  input port                               output port
    std::list<ProcessorPtr> processors;
    size_t num_threads = 0;
};

}
