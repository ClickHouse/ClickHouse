#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>


namespace DB
{
/** Implements WINDOW VIEW table WATCH input stream.
 *  Keeps stream alive by outputing blocks with no rows
 *  based on window interval.
 */
class WindowViewBlockInputStream : public IBlockInputStream
{
public:
    WindowViewBlockInputStream(
        std::shared_ptr<StorageWindowView> storage_,
        std::shared_ptr<bool> active_ptr_,
        const bool has_limit_,
        const UInt64 limit_)
        : storage(std::move(storage_))
        , active(std::move(active_ptr_))
        , has_limit(has_limit_)
        , limit(limit_) {}

    String getName() const override { return "WindowViewBlock"; }

    void cancel(bool kill) override
    {
        if (isCancelled() || storage->is_dropped)
            return;
        IBlockInputStream::cancel(kill);
    }

    Block getHeader() const override { return storage->getHeader(); }

    void addFireSignal(UInt32 timestamp)
    {
        std::lock_guard lock(fire_signal_mutex);
        fire_signal.push_back(timestamp);
    }

protected:
    Block readImpl() override
    {
        /// try reading
        return tryReadImpl();
    }

    Block tryReadImpl()
    {
        Block res;

        if (has_limit && num_updates == static_cast<Int64>(limit))
        {
            return Block();
        }
        /// If blocks were never assigned get blocks
        if (!in_stream)
            in_stream = std::make_shared<NullBlockInputStream>(getHeader());

        if (isCancelled() || storage->is_dropped)
        {
            return Block();
        }

        res = in_stream->read();
        if (!res)
        {
            if (!(*active))
                return Block();

            if (!end_of_blocks)
            {
                end_of_blocks = true;
                num_updates += 1;
                return getHeader();
            }

            std::unique_lock lock(mutex);
            storage->condition.wait_for(lock, std::chrono::seconds(5));

            if (isCancelled() || storage->is_dropped)
            {
                return Block();
            }

            while (true)
            {
                UInt32 timestamp_;
                {
                    std::unique_lock lock_(fire_signal_mutex);
                    if (fire_signal.empty())
                        break;
                    timestamp_ = fire_signal.front();
                    fire_signal.pop_front();
                }
                in_stream = storage->getNewBlocksInputStreamPtr(timestamp_);
                res = in_stream->read();
                if (res)
                {
                    end_of_blocks = false;
                    return res;
                }
            }
            return getHeader();
        }
        return res;
    }

private:
    std::shared_ptr<StorageWindowView> storage;
    std::shared_ptr<bool> active;
    const bool has_limit;
    const UInt64 limit;
    std::mutex mutex;
    Int64 num_updates = -1;
    bool end_of_blocks = false;
    BlockInputStreamPtr in_stream;
    std::mutex fire_signal_mutex;
    std::deque<UInt32> fire_signal;
};
}
