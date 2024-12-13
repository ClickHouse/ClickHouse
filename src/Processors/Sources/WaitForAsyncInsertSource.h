#pragma once

#include <Processors/ISource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

/// Source, that allow to wait until processing of
/// asynchronous insert for specified query_id will be finished.
class WaitForAsyncInsertSource : public ISource, WithContext
{
public:
    WaitForAsyncInsertSource(
        std::future<void> insert_future_, size_t timeout_ms_)
        : ISource(Block())
        , insert_future(std::move(insert_future_))
        , timeout_ms(timeout_ms_)
    {
        assert(insert_future.valid());
    }

    String getName() const override { return "WaitForAsyncInsert"; }

protected:
    Chunk generate() override
    {
        auto status = insert_future.wait_for(std::chrono::milliseconds(timeout_ms));
        if (status == std::future_status::deferred)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got future in deferred state");

        if (status == std::future_status::timeout)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout ({} ms) exceeded)", timeout_ms);

        insert_future.get();
        return Chunk();
    }

private:
    std::future<void> insert_future;
    size_t timeout_ms;
};

}
