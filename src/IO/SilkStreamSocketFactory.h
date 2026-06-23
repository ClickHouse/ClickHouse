#pragma once

#if defined(OS_LINUX)

#include <Poco/Net/StreamSocket.h>

#include <functional>
#include <memory>

namespace Silk
{

std::function<std::unique_ptr<Poco::Net::StreamSocket>(bool secure)> StreamSocketFactory();

}

#endif
