#pragma once

#include <memory>
#include <functional>

#include <base/shared_ptr_helper.h>
#include <Interpreters/StorageID.h>

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
    virtual void onCompleted() = 0;
    virtual StorageID getStorageID() = 0;
    virtual UInt64 getPriority() = 0;
    virtual ~IExecutableTask() = default;
};

using ExecutableTaskPtr = std::shared_ptr<IExecutableTask>;


/**
 * Some background operations won't represent a coroutines (don't want to be executed step-by-step). For this we have this wrapper.
 */
class ExecutableLambdaAdapter : public shared_ptr_helper<ExecutableLambdaAdapter>, public IExecutableTask
{
public:

    template <typename Job, typename Callback>
    explicit ExecutableLambdaAdapter(
        Job && job_to_execute_,
        Callback && job_result_callback_,
        StorageID id_)
        : job_to_execute(job_to_execute_)
        , job_result_callback(job_result_callback_)
        , id(id_) {}

    bool executeStep() override
    {
        res = job_to_execute();
        job_to_execute = {};
        return false;
    }

    void onCompleted() override { job_result_callback(!res); }
    StorageID getStorageID() override { return id; }
    UInt64 getPriority() override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getPriority() method is not supported by LambdaAdapter");
    }

private:
    bool res = false;
    std::function<bool()> job_to_execute;
    IExecutableTask::TaskResultCallback job_result_callback;
    StorageID id;
};


}
