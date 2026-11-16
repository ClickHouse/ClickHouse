#pragma once

#include "config.h"

#if USE_SILK

#include <Poco/Net/StreamSocket.h>

#include <functional>
#include <memory>

namespace Silk
{

std::function<std::unique_ptr<Poco::Net::StreamSocket>(bool secure)> streamSocketFactory();

}

#endif
