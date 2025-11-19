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
                LOG_DEBUG(log, "Closing pending handles");
                uv_walk(loop_ptr.get(), onUVWalkClosingCallback, nullptr);

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

    static void onUVWalkClosingCallback(uv_handle_t * handle, void *)
    {
        if (!uv_is_closing(handle))
            uv_close(handle, onUVCloseCallback);
    }

    static void onUVCloseCallback(uv_handle_t *) {}
};

}
