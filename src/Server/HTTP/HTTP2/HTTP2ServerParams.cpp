#include <string>

#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>

namespace DB
{

static const std::string CONFIG_SECTION_NAME = "http2_server";

HTTP2ServerParams::Ptr HTTP2ServerParams::fromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(CONFIG_SECTION_NAME))
        return nullptr;

    Ptr res = new HTTP2ServerParams;
    return res;
}

}
