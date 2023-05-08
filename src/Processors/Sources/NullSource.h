#pragma once
#include <Storages/StorageNull.h>
#include <Processors/ISource.h>
#include <Common/logger_useful.h>

namespace DB
{

class NullSource : public ISource
{
using NonBlockingResult = std::pair<Block, bool>;
public:
    explicit NullSource(Block header) : ISource(std::move(header)) {}

    NullSource(Block header, 
        std::shared_ptr<StorageNull> storage_, std::shared_ptr<BlocksPtr> blocks_ptr_) 
        : ISource(std::move(header)), 
        storage(std::move(storage_)), 
        blocks_ptr(std::move(blocks_ptr_)),
        is_stream(true) {}

    String getName() const override { return "NullSource"; }

    NonBlockingResult tryRead()
    {
        return tryReadImpl(false);
    }

protected:
    Chunk generate() override { 
        auto block = tryReadImpl(true).first;
        if (!is_stream) {
            return Chunk();
        }
        return Chunk(block.getColumns(), block.rows());
    }

    NonBlockingResult tryReadImpl(bool blocking)
    {
        if (blocking) {
            LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "22");
        } else {
            LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "33");
        }
        if (!is_stream) {
            LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "not streammm");
            return { Block(), true };
        }
        Block res;

        if (!blocks)
        {
            LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "Нет блоков, инициализвция");
            std::lock_guard lock(storage->mutex);
            blocks = (*blocks_ptr);
            it = blocks->begin();
            begin = blocks->begin();
            end = blocks->end();
        }

        if (isCancelled() || storage->shutdown_called)
        {
            LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "Закрываемся");
            return { Block(), true };
        }

        if (it == end)
        {
            {
                LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "it == end");
                std::unique_lock lock(storage->mutex);
                /// If we are done iterating over our blocks
                /// and there are new blocks available then get them
                if (blocks.get() != (*blocks_ptr).get())
                {
                    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "Появились новые блоки");
                    blocks = (*blocks_ptr);
                    it = blocks->begin();
                    begin = blocks->begin();
                    end = blocks->end();
                }
                /// No new blocks available wait for new ones
                else
                {
                    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "Ждём");
                    if (!blocking)
                    {
                        LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "blocking");
                        return { Block(), false };
                    }
                    if (!end_of_blocks)
                    {
                        LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "end_of_blocks");
                        end_of_blocks = true;
                        return { getPort().getHeader(), true };
                    }
                    while (true)
                    {
                        // LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "while");
                        UInt64 timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());

                        /// Or spurious wakeup.
                        bool signaled = std::cv_status::no_timeout == storage->condition.wait_for(lock,
                            std::chrono::microseconds(std::max(UInt64(0), heartbeat_interval_usec - (timestamp_usec - last_event_timestamp_usec))));
                        if (isCancelled() || storage->shutdown_called)
                        {
                            // LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "11");
                            return { Block(), true };
                        }
                        if (signaled)
                        {
                            // LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "12");
                            break;
                        }
                        else
                        {
                            // LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "13");
                            // heartbeat
                            last_event_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
                            return { getPort().getHeader(), true };
                        }
                    }
                }
            }
            return tryReadImpl(blocking);
        }

        res = *it;

        ++it;

        if (it == end)
        {
            end_of_blocks = false;
        }

        last_event_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds());
        return { res, true };
    }
private:
    std::shared_ptr<StorageNull> storage;
    std::shared_ptr<BlocksPtr> blocks_ptr;
    UInt64 last_event_timestamp_usec = 15000000;
    UInt64 heartbeat_interval_usec;
    bool is_stream = false;
    BlocksPtr blocks;
    Blocks::iterator it;
    Blocks::iterator end;
    Blocks::iterator begin;
    bool end_of_blocks = false;
};

}
