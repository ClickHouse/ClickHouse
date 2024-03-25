#pragma once

#include <memory>
#include <functional>

#include <boost/noncopyable.hpp>
#include <Interpreters/StorageID.h>
#include <Common/Priority.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
 * Generic interface for background operations. Simply this is self-made coroutine.
 * The main method is executeStep, which will return true
 * if the task wants to execute another 'step' in near future and false otherwise.
 *
 * Each storage assigns some operations such as merges, mutations, fetches, etc.
 * We need to ask a storage or some another entity to try to assign another operation when current operation is completed.
 *
 * Each task corresponds to a storage, that's why there is a method getStorageID.
 * This is needed to correctly shutdown a storage, e.g. we need to wait for all background operations to complete.
 */
class IExecutableTask
{
public:
    using TaskResultCallback = std::function<void(bool)>;

    virtual bool executeStep() = 0;

    /// Sometimes exceptions from the executeStep() had been already printed to
    /// the log, but with different level (see
    /// ReplicatedMergeMutateTaskBase::executeStep()), but the exception should
    /// be throw, since there are some sanity assertions based on the
    /// std::uncaught_exceptions() (i.e. WriteBuffer::~WriteBuffer())
    virtual bool printExecutionException() const { return true; }

    virtual void onCompleted() = 0;
    virtual StorageID getStorageID() const = 0;
    virtual String getQueryId() const = 0;
    virtual Priority getPriority() const = 0;
    virtual ~IExecutableTask() = default;
};

using ExecutableTaskPtr = std::shared_ptr<IExecutableTask>;


/**
 * Some background operations won't represent a coroutines (don't want to be executed step-by-step). For this we have this wrapper.
 */
class ExecutableLambdaAdapter : public IExecutableTask, boost::noncopyable
{
public:
    template <typename Job, typename Callback>
    explicit ExecutableLambdaAdapter(
        Job && job_to_execute_,
        Callback && job_result_callback_,
        StorageID id_)
        : job_to_execute(std::forward<Job>(job_to_execute_))
        , job_result_callback(std::forward<Callback>(job_result_callback_))
        , id(id_) {}

    bool executeStep() override
    {
        res = job_to_execute();
        job_to_execute = {};
        return false;
    }

    void onCompleted() override { job_result_callback(!res); }
    StorageID getStorageID() const override { return id; }
    Priority getPriority() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getPriority() method is not supported by LambdaAdapter");
    }

    String getQueryId() const override { return id.getShortName() + "::lambda"; }

private:
    bool res = false;
    std::function<bool()> job_to_execute;
    IExecutableTask::TaskResultCallback job_result_callback;
    StorageID id;
};


}
