#ifndef BOOST_ASIO_STEADY_TIMER_HPP
#define BOOST_ASIO_STEADY_TIMER_HPP

/** Compatibility shim for pulsar-client-cpp: Boost >= 1.87 removed the deprecated
  * `expires_from_now` and `cancel(error_code &)` members of waitable timers.
  * This header replaces <boost/asio/steady_timer.hpp> (the include guard matches the
  * original header, so whichever is included first wins) and defines `steady_timer`
  * as a subclass restoring the members that pulsar-client-cpp uses.
  */

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include <chrono>
#include <cstddef>

namespace boost::asio
{

class steady_timer : public basic_waitable_timer<std::chrono::steady_clock>
{
public:
    using basic_waitable_timer<std::chrono::steady_clock>::basic_waitable_timer;
    using basic_waitable_timer<std::chrono::steady_clock>::cancel;

    std::size_t expires_from_now(const duration & expiry_time)
    {
        return expires_after(expiry_time);
    }

    std::size_t cancel(boost::system::error_code & ec)
    {
        try
        {
            ec.clear();
            return cancel();
        }
        catch (const boost::system::system_error & e)
        {
            ec = e.code();
            return 0;
        }
    }
};

}

#endif // BOOST_ASIO_STEADY_TIMER_HPP
