#include <string>

#include <Common/Exception.h>

#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

static const std::string CONFIG_SECTION_NAME = "http2_server";

HTTP2ServerParams::Ptr HTTP2ServerParams::fromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(CONFIG_SECTION_NAME))
        return nullptr;

#if USE_NGHTTP2
    Ptr res = new HTTP2ServerParams;
    return res;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "HTTP/2 support is disabled, because ClickHouse was built without nghttp2 library");
#endif
}

}
