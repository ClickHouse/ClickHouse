#pragma once

#include <memory>

#include <uv.h>
#include <boost/noncopyable.hpp>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

/// RAII wrapper around uv event loop
class UVLoop : public boost::noncopyable
{
public:
    UVLoop() : loop_ptr(new uv_loop_t())
    {
        int res = uv_loop_init(loop_ptr.get());

        if (res != 0)
            throw Exception(ErrorCodes::SYSTEM_ERROR, "UVLoop could not initialize");
    }

    ~UVLoop()
    {
        if (loop_ptr)
        {
            auto res = uv_loop_close(loop_ptr.get());

            if (res == UV_EBUSY)
            {
                /// Do not close pending handles here, because nats-io library have own order of async functions call on disconnect: uvAsyncDetach->closeSchedulerCb->finalCloseCb
                /// If we close handles here, then this assert fails: closeSchedulerCb->uv_close->assert(!uv__is_closing(handle));

                /// Run the loop until there are no pending callbacks.
                while ((res = uv_run(loop_ptr.get(), UV_RUN_ONCE)) != 0)
                {
                    LOG_DEBUG(log, "Waiting for pending callbacks to finish ({})", res);
                }

                res = uv_loop_close(loop_ptr.get());
                if (res == UV_EBUSY)
                {
                    LOG_ERROR(
                        log, "Failed to close libuv loop (active requests/handles in the loop: {})",
                        uv_loop_alive(loop_ptr.get()));
                    chassert(false);
                }
            }
        }
    }

    uv_loop_t * getLoop() { return loop_ptr.get(); }

    const uv_loop_t * getLoop() const { return loop_ptr.get(); }

private:
    std::unique_ptr<uv_loop_t> loop_ptr;
    LoggerPtr log = getLogger("UVLoop");
};

}
