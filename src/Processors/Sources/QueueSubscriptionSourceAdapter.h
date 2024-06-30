#pragma once

#include <optional>

#include <Poco/Logger.h>

#include <Common/logger_useful.h>

#include <Processors/ISource.h>

#include <Storages/Streaming/QueueStreamSubscription.h>

namespace DB
{

template <class T>
class QueueSubscriptionSourceAdapter : public ISource
{
public:
    QueueSubscriptionSourceAdapter(Block header_, QueueStreamSubscription<T> & subscription_, String log_name);
    ~QueueSubscriptionSourceAdapter() override = default;

    Status prepare() override;
    int schedule() override;
    std::optional<Chunk> tryGenerate() override;

    /// Stop reading from subscription if output port was finished.
    void onUpdatePorts() override;

    /// Stop reading from subscription if query was cancelled.
    void onCancel() override;

protected:
    std::list<T> cached_data;
    bool need_new_data = false;
    Poco::Logger * log = nullptr;

    virtual Chunk useCachedData() = 0;

private:
    QueueStreamSubscription<T> & subscription;

    std::optional<int> fd;
    bool is_async_state = false;
};


template <class T>
QueueSubscriptionSourceAdapter<T>::QueueSubscriptionSourceAdapter(Block header_, QueueStreamSubscription<T> & subscription_, String log_name)
    : ISource(std::move(header_))
    , log(&Poco::Logger::get(log_name))
    , subscription(subscription_)
    , fd(subscription.fd())
{
}

template <class T>
IProcessor::Status QueueSubscriptionSourceAdapter<T>::prepare()
{
    if (subscription.isDisabled())
        return Status::Finished;

    if (is_async_state)
        return Status::Async;

    auto base_status = ISource::prepare();

    if (base_status == Status::Finished)
        subscription.disable();

    return base_status;
}

template <class T>
std::optional<Chunk> QueueSubscriptionSourceAdapter<T>::tryGenerate()
{
    is_async_state = false;

    if (isCancelled() || subscription.isDisabled())
        return std::nullopt;

    if (need_new_data)
    {
        if (fd.has_value() && subscription.isEmpty())
        {
            is_async_state = true;
            return Chunk();
        }

        auto new_data = subscription.extractAll();
        cached_data.splice(cached_data.end(), new_data);

        need_new_data = false;
    }

    return useCachedData();
}

template <class T>
int QueueSubscriptionSourceAdapter<T>::schedule()
{
    chassert(fd.has_value());
    LOG_INFO(log, "waiting on descriptor: {}", fd.value());
    return fd.value();
}

template <class T>
void QueueSubscriptionSourceAdapter<T>::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        LOG_INFO(log, "output port is finished, disabling subscription");
        subscription.disable();
    }
}

template <class T>
void QueueSubscriptionSourceAdapter<T>::onCancel()
{
    LOG_INFO(log, "query is cancelled, disabling subscription");
    subscription.disable();
}

}
