#pragma once

#include <memory>
#include <functional>

#include <common/shared_ptr_helper.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class ExecutableTask
{
public:
    virtual bool execute() = 0;
    virtual void onCompleted() = 0;
    virtual StorageID getStorageID() = 0;
    virtual ~ExecutableTask() = default;
};

using ExecutableTaskPtr = std::shared_ptr<ExecutableTask>;


class LambdaAdapter : public shared_ptr_helper<LambdaAdapter>, public ExecutableTask
{
public:

    template <typename InnerJob, typename Callback>
    explicit LambdaAdapter(InnerJob && inner_, Callback && callback_, StorageID id_)
        : inner(inner_), callback(callback_), id(id_) {}

    bool execute() override
    {
        res = inner();
        return false;
    }

    void onCompleted() override { callback(!res); }

    StorageID getStorageID() override { return id; }

private:
    bool res = false;
    std::function<bool()> inner;
    std::function<void(bool)> callback;
    StorageID id;
};


}
