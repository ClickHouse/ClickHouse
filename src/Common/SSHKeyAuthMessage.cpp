#include <Common/SSHKeyAuthMessage.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/re2.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
}

namespace
{

/// A constant tag at the start of the signed message, so that a signature made for this purpose
/// cannot be mistaken for a signature over any other kind of message made with the same key.
constexpr std::string_view SSH_KEY_AUTH_MESSAGE_TAG = "ClickHouse native ssh_key auth v1";

constexpr std::string_view ACCEPTED_HOSTS_CONFIG = "ssh_key_auth.accepted_hosts";

}

String SSHKeyAuthMessage::serialize() const
{
    WriteBufferFromOwnString buf;
    writeStringBinary(SSH_KEY_AUTH_MESSAGE_TAG, buf);
    writeStringBinary(std::to_string(protocol_version), buf);
    writeStringBinary(default_database, buf);
    writeStringBinary(user, buf);
    writeStringBinary(challenge, buf);
    writeStringBinary(host, buf);
    return buf.str();
}

String SSHKeyAuthMessage::normalizeHost(std::string_view host)
{
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']')
        host = host.substr(1, host.size() - 2);
    else if (host.ends_with('.'))
        host.remove_suffix(1);

    String result(host);
    std::ranges::transform(result, result.begin(), toLowerIfAlphaASCII);
    return result;
}

SSHKeyAuthAcceptedHosts::SSHKeyAuthAcceptedHosts(const Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(String(ACCEPTED_HOSTS_CONFIG), keys);
    for (const String & key : keys)
    {
        String value = config.getString(fmt::format("{}.{}", ACCEPTED_HOSTS_CONFIG, key));
        if (key.starts_with("host_regexp"))
        {
            auto regexp = std::make_unique<re2::RE2>(value, re2::RE2::Quiet);
            if (!regexp->ok())
                throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                    "Cannot compile the regular expression '{}' in `{}`: {}", value, ACCEPTED_HOSTS_CONFIG, regexp->error());
            name_regexps.push_back(std::move(regexp));
        }
        else if (key.starts_with("host"))
        {
            names.push_back(SSHKeyAuthMessage::normalizeHost(value));
        }
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE,
                "Unknown entry `{}` in `{}`, expected `host` or `host_regexp`", key, ACCEPTED_HOSTS_CONFIG);
        }
    }
}

SSHKeyAuthAcceptedHosts::~SSHKeyAuthAcceptedHosts() = default;

bool SSHKeyAuthAcceptedHosts::accepts(std::string_view host) const
{
    if (names.empty() && name_regexps.empty())
        return true;

    String normalized = SSHKeyAuthMessage::normalizeHost(host);
    if (std::ranges::find(names, normalized) != names.end())
        return true;

    return std::ranges::any_of(name_regexps, [&](const auto & regexp) { return re2::RE2::FullMatch(normalized, *regexp); });
}

}
