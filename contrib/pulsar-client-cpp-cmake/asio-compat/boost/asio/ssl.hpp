#pragma once

/** Compatibility shim for pulsar-client-cpp: Boost >= 1.87 removed the deprecated
  * `rfc2818_verification` class. `host_name_verification` is its direct replacement
  * (the same hostname check, based on RFC 6125).
  */

#include_next <boost/asio/ssl.hpp>

#include <boost/asio/ssl/host_name_verification.hpp>

namespace boost::asio::ssl
{

class rfc2818_verification : public host_name_verification
{
public:
    using host_name_verification::host_name_verification;
};

}
