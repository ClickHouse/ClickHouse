#pragma once
#include <Storages/StorageNull.h>
#include <Processors/ISource.h>
#include <Common/logger_useful.h>

namespace DB
{

class NullSource : public ISource
{
public:
    explicit NullSource(Block header) : ISource(std::move(header)) {}

    NullSource(Block header, 
        std::shared_ptr<StorageNull> storage_, std::shared_ptr<BlocksPtr> blocks_ptr_) 
        : ISource(std::move(header)), 
        storage(std::move(storage_)), 
        blocks_ptr(std::move(blocks_ptr_)),
        is_stream(true) {}

    String getName() const override { return "NullSource"; }

    void onCancel() override
    {
        if (!is_stream) {
            return;
        }
        if (storage->shutdown_called)
            return;
            
        std::lock_guard lock(storage->mutex);
        storage->condition.notify_all();
    }

protected:
    Chunk generate() override { 
        if (!is_stream) {
            return Chunk();
        }
        auto block = tryReadImpl();
        return Chunk(block.getColumns(), block.rows());
    }

    Block tryReadImpl()
    {
        Block res;

        if (!blocks)
        {
            std::lock_guard lock(storage->mutex);
            blocks = (*blocks_ptr);
            it = blocks->begin();
            end = blocks->end();
        }

        if (isCancelled() || storage->shutdown_called)
        {
            return Block();
        }

        if (it == end)
        {
            {
                std::unique_lock lock(storage->mutex);
                if (blocks.get() != (*blocks_ptr).get())
                {
                    blocks = (*blocks_ptr);
                    it = blocks->begin();
                    end = blocks->end();
                }
                else
                {
                    if (!end_of_blocks)
                    {
                        end_of_blocks = true;
                        return getPort().getHeader();
                    }
                    while (true)
                    {
                        UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());

                        bool signaled = std::cv_status::no_timeout == storage->condition.wait_for(lock,
                            std::chrono::microseconds(std::max(UInt64(0), heartbeat_interval_usec - (timestamp_usec - last_event_timestamp_usec))));
                        if (isCancelled() || storage->shutdown_called)
                        {
                            return Block();
                        }
                        if (signaled)
                        {
                            break;
                        }
                        else
                        {
                            last_event_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
                            return getPort().getHeader();
                        }
                    }
                }
            }
            return tryReadImpl();
        }

        res = *it;
        ++it;

        if (it == end)
        {
            end_of_blocks = false;
        }

        last_event_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
        return res;
    }
private:
    std::shared_ptr<StorageNull> storage;
    std::shared_ptr<BlocksPtr> blocks_ptr;
    UInt64 last_event_timestamp_usec;
    UInt64 heartbeat_interval_usec = 15000000;
    bool is_stream = false;
    BlocksPtr blocks;
    Blocks::iterator it;
    Blocks::iterator end;
    bool end_of_blocks = false;
};

}
