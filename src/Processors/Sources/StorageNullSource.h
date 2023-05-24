#pragma once
#include <Common/logger_useful.h>
#include <Processors/ISource.h>

namespace DB
{

class StorageNullSource : public ISource
{
public:
    StorageNullSource(Block header, std::weak_ptr<Subscriber> subscriber_) 
        : ISource(std::move(header)), subscriber(subscriber_) {}

    String getName() const override { return "StorageNullSource"; }

    void onCancel() override
    {
        if (subscriber.expired()) {
            return;
        }
        auto subs = subscriber.lock();
        std::lock_guard lock(subs->mutex);
        subs->condition.notify_all();
    }

protected:
    Chunk generate() override { 
        LOG_FATAL(&Poco::Logger::root(), " {}", "generddte");
        if (subscriber.expired()) {
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
            LOG_FATAL(&Poco::Logger::root(), " {}", "if (vv!blocks)");
            auto subs = subscriber.lock();
            if (!subs->blocks) {
            }
            std::lock_guard lock(subs->mutex);
            blocks = subs->blocks;
            it = blocks->begin();
            end = blocks->end();
        }

        if (isCancelled())
        {
            return Block();
        }

        if (it == end)
        {
            {
                auto subs = subscriber.lock();
                std::unique_lock lock(subs->mutex);
                if (blocks.get() != (subs->blocks).get())
                {
                    blocks = subs->blocks;
                    it = blocks->begin();
                    end = blocks->end();
                }
                else
                {
                    LOG_FATAL(&Poco::Logger::root(), " {}", "2bb");
                    if (!end_of_blocks)
                    {
                        end_of_blocks = true;
                        return getPort().getHeader();
                    }
                    while (true)
                    {
                        LOG_FATAL(&Poco::Logger::root(), " {}", "whilbbe (true)");
                        UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());

                        bool signaled = std::cv_status::no_timeout == subs->condition.wait_for(lock,
                            std::chrono::microseconds(std::max(UInt64(0), heartbeat_interval_usec - (timestamp_usec - last_event_timestamp_usec))));
                        if (isCancelled())
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
    std::weak_ptr<Subscriber> subscriber;
    UInt64 last_event_timestamp_usec;
    UInt64 heartbeat_interval_usec = 15000000;
    BlocksPtr blocks;
    Blocks::iterator it;
    Blocks::iterator end;
    bool end_of_blocks = false;
};

}
