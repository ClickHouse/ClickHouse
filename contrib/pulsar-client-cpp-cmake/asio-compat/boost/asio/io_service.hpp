#pragma once

/** Compatibility shim for pulsar-client-cpp: Boost >= 1.87 removed the deprecated
  * `io_service` name together with its `post`, `run(error_code &)` and nested `work`
  * members. This header restores the parts of that API which pulsar-client-cpp uses.
  */

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include <cstddef>
#include <utility>

namespace boost::asio
{

class io_service : public io_context
{
public:
    using io_context::io_context;
    using io_context::run;

    std::size_t run(boost::system::error_code & ec)
    {
        try
        {
            ec.clear();
            return io_context::run();
        }
        catch (const boost::system::system_error & e)
        {
            ec = e.code();
            return 0;
        }
    }

    template <typename Function>
    void post(Function && function)
    {
        boost::asio::post(static_cast<io_context &>(*this), std::forward<Function>(function));
    }

    class work
    {
    public:
        explicit work(io_service & io_service_) : guard(make_work_guard(io_service_)) { }

    private:
        executor_work_guard<io_context::executor_type> guard;
    };
};

}
