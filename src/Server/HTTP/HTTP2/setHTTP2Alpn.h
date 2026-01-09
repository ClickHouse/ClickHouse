#pragma once

#include "config.h"
#if USE_SSL

#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>

#include <Poco/Net/SecureServerSocket.h>

namespace DB
{

bool setHTTP2Alpn(const Poco::Net::SecureServerSocket & socket, HTTP2ServerParams::Ptr http2_params);

}

#endif
