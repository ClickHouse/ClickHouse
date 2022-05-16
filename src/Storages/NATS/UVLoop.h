#pragma once

#include <memory>

#include <uv.h>
#include <boost/noncopyable.hpp>

#include <Common/Exception.h>

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
            throw Exception("UVLoop could not initialize", ErrorCodes::SYSTEM_ERROR);
    }

    ~UVLoop()
    {
        if (loop_ptr)
            uv_loop_close(loop_ptr.get());
    }

    inline uv_loop_t * getLoop() { return loop_ptr.get(); }

    inline const uv_loop_t * getLoop() const { return loop_ptr.get(); }

private:
    std::unique_ptr<uv_loop_t> loop_ptr;
};

}
