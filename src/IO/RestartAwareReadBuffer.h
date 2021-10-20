#pragma once

#include <IO/ReadBufferFromFileDecorator.h>
#include <shared_mutex>

namespace DB
{

using ReadLock = std::shared_lock<std::shared_timed_mutex>;

/// Holds restart read lock till buffer destruction.
class RestartAwareReadBuffer : public ReadBufferFromFileDecorator
{
public:
    RestartAwareReadBuffer(const DiskRestartProxy & disk, std::unique_ptr<ReadBufferFromFileBase> impl_)
        : ReadBufferFromFileDecorator(std::move(impl_)), lock(disk.mutex) { }

    void prefetch() override { impl->prefetch(); }

    void setReadUntilPosition(size_t position) override { impl->setReadUntilPosition(position); }

private:
    ReadLock lock;
};

}
