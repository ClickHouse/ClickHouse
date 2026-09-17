#pragma once
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

/// Has one input and one output.
/// Works similarly to ISimpleTransform, but with much care about exceptions.
///
/// If input contain exception, this exception is pushed directly to output port.
/// If input contain data chunk, transform() is called for it.
/// When transform throws exception itself, data chunk is replaced by caught exception.
/// Transformed chunk or newly caught exception is pushed to output.
///
/// There may be any number of exceptions read from input, transform keeps the order.
/// It is expected that output port won't be closed from the other side before all data is processed.
///
/// Method onStart() is called before reading any data.
/// Method onFinish() is called after all data from input is processed, if no exception happened.
/// In case of exception, it is additionally pushed into pipeline.
class ExceptionKeepingTransform : public IProcessor
{
protected:
    InputPort & input;
    OutputPort & output;
    Port::Data data;

    enum class Stage : uint8_t
    {
        Start,
        Consume,
        Generate,
        Finish,
        Exception,
    };

    Stage stage = Stage::Start;
    bool ready_input = false;
    bool ready_output = false;
    const bool ignore_on_start_and_finish = true;

    struct GenerateResult
    {
        Chunk chunk;
        bool is_done = true;
    };

    virtual void onStart() {}
    virtual void onConsume(Chunk chunk) = 0;
    virtual GenerateResult onGenerate() = 0;
    virtual void onFinish() {}
    virtual void onException(std::exception_ptr /* exception */) { }

    virtual bool canGenerate() { return true; }
    virtual GenerateResult getRemaining() { return {};}

public:
    ExceptionKeepingTransform(SharedHeader in_header, SharedHeader out_header, bool ignore_on_start_and_finish_ = true);

    /// Register an additional final-publication guard which must be retained
    /// while consume/onFinish executes. Most transforms have no factories, so
    /// their fast path is a single null check and owns no factory allocation.
    template <typename Factory>
    void addAdditionalCommitGuardFactory(Factory && factory)
    {
        using FactoryType = std::decay_t<Factory>;
        using Guard = std::decay_t<std::invoke_result_t<FactoryType &>>;

        if (!additional_commit_guard_factories)
            additional_commit_guard_factories = std::make_unique<AdditionalCommitGuardFactories>();

        additional_commit_guard_factories->emplace_back(
            [guard_factory = FactoryType(std::forward<Factory>(factory))]() mutable -> AdditionalCommitGuardPtr
            { return std::make_unique<AdditionalCommitGuardHolder<Guard>>(guard_factory()); });
    }

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    void setRuntimeData(ThreadGroupPtr thread_group_);

private:
    class AdditionalCommitGuard
    {
    public:
        virtual ~AdditionalCommitGuard() = default;
    };

    template <typename Guard>
    class AdditionalCommitGuardHolder final : public AdditionalCommitGuard
    {
    public:
        explicit AdditionalCommitGuardHolder(Guard guard_)
            : guard(std::move(guard_))
        {
        }

    private:
        Guard guard;
    };

    using AdditionalCommitGuardPtr = std::unique_ptr<AdditionalCommitGuard>;
    using AdditionalCommitGuardFactory = std::function<AdditionalCommitGuardPtr()>;
    using AdditionalCommitGuardFactories = std::vector<AdditionalCommitGuardFactory>;
    using AdditionalCommitGuards = std::vector<AdditionalCommitGuardPtr>;

    AdditionalCommitGuards acquireAdditionalCommitGuards();

    ThreadGroupPtr thread_group = nullptr;
    std::unique_ptr<AdditionalCommitGuardFactories> additional_commit_guard_factories;
};

}
