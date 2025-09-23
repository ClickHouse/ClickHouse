#include <string>
#include <Common/Exception.h>
#include <Common/Vault.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

Vault & Vault::instance()
{
    static Vault ret;
    return ret;
}

void Vault::load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    reset();

    url = config.getString(config_prefix + ".url", "");

    if (url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "url is not given for vault.");

    token = config.getString(config_prefix + ".token", "");

    if (token.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "token is not given for vault.");

    loaded = true;
}

String Vault::readSecret(const String & secret, const String & key)
{
    LOG_DEBUG(log, "readSecret {} {}", secret, key);

    // TODO

    return std::to_string(0);
}

}
