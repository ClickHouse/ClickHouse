#pragma once

#include <base/types.h>

#include <memory>
#include <string_view>
#include <vector>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace re2
{
    class RE2;
}

namespace DB
{

/// The message that the client signs and the server verifies in `ssh_key` authentication over the native TCP protocol,
/// in the layout used since `DBMS_MIN_REVISION_WITH_SSH_KEY_AUTH_HOST`.
///
/// It includes the name of the host the client connects to, as the client was given it (before resolving it).
/// The client sends this name next to the signature, and the server accepts the login only if the name is one of
/// the names it accepts (see `SSHKeyAuthAcceptedHosts`). So a server that receives a signature cannot use it
/// to log in to a server with another name. The name does not depend on the connection, so the check works
/// through proxies and load balancers that terminate TLS.
struct SSHKeyAuthMessage
{
    UInt64 protocol_version = 0;
    String default_database;
    String user;
    String challenge;
    String host;

    /// The bytes to sign. Every field is length-prefixed, so the boundaries between the fields cannot be confused.
    String serialize() const;

    /// The form of a host name that the client signs and the server compares: ASCII lowercase,
    /// without the square brackets around an IPv6 address and without a trailing dot.
    static String normalizeHost(std::string_view host);
};

/// The host names a server accepts in `ssh_key` authentication, from the server configuration:
///
///     <ssh_key_auth>
///         <accepted_hosts>
///             <host>abc.example.com</host>
///             <host_regexp>^[a-z0-9-]+\.example\.com$</host_regexp>
///         </accepted_hosts>
///     </ssh_key_auth>
///
/// `host` entries are compared after normalization (see `SSHKeyAuthMessage::normalizeHost`), and a `host_regexp`
/// must match the whole normalized name. If no entries are configured, any name is accepted.
class SSHKeyAuthAcceptedHosts
{
public:
    explicit SSHKeyAuthAcceptedHosts(const Poco::Util::AbstractConfiguration & config);
    ~SSHKeyAuthAcceptedHosts();

    bool accepts(std::string_view host) const;

private:
    std::vector<String> names;
    std::vector<std::unique_ptr<re2::RE2>> name_regexps;
};

}
