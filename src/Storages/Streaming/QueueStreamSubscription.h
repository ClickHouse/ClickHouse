#pragma once

#include <Storages/Streaming/IStreamSubscription.h>
#include <Storages/Streaming/SCMPQueue.h>

namespace DB
{

template <class T>
class QueueStreamSubscription : public IStreamSubscription
{
public:
    void push(T value) { queue.add(std::move(value)); }
    std::list<T> extractAll() { return queue.extractAll(); }

    bool isEmpty() const { return queue.isEmpty(); }
    std::optional<int> fd() const { return queue.fd(); }

    void disable() { queue.close(); }

private:
    SCMPQueue<T> queue;
};

}
