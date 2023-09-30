#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

namespace DB
{

/// Has one unconnected input port and one unconnected output port.
/// There may be other ports on the processors, but they must all be connected.
/// The unconnected input must be on the first processor, output - on the last.
/// The processors don't necessarily form an actual chain.
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

    bool getConcurrencyControl() const { return concurrency_control; }
    void setConcurrencyControl(bool concurrency_control_) { concurrency_control = concurrency_control_; }

    void addSource(ProcessorPtr processor);
    void addSink(ProcessorPtr processor);
    void appendChain(Chain chain);

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
    void addInterpreterContext(ContextPtr context) { holder.interpreter_context.emplace_back(std::move(context)); }

    void attachResources(QueryPlanResourceHolder holder_)
    {
        /// This operator "=" actually merges holder_ into holder, doesn't replace.
        holder = std::move(holder_);
    }
    QueryPlanResourceHolder detachResources() { return std::move(holder); }

    void reset();

private:
    QueryPlanResourceHolder holder;

    /// -> source -> transform -> ... -> transform -> sink ->
    ///  ^        ->           ->     ->           ->       ^
    ///  input port                               output port
    std::list<ProcessorPtr> processors;
    size_t num_threads = 0;
    bool concurrency_control = false;
};

}
