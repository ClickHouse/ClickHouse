#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>

#include "config.h"

#if USE_SSH

namespace DB
{

/// A client of the OpenSSH authentication agent protocol, see
/// https://datatracker.ietf.org/doc/html/draft-miller-ssh-agent
/// The agent is found by the `SSH_AUTH_SOCK` environment variable.
class SSHAgent
{
public:
    struct Identity
    {
        /// The public key in the SSH wire format.
        /// It is the same as the base64-encoded part of a `.pub` file, but decoded.
        String key_blob;
        String comment;
    };

    /// The path of the socket of the agent, or an empty string if the agent is not available.
    static String getSocketPath();

    static bool isAvailable() { return !getSocketPath().empty(); }

    /// The keys held by the agent.
    static std::vector<Identity> listIdentities();

    /// Asks the agent to sign `data` with the key `key_blob` and returns the signature
    /// in the `SSHSIG` format, the same as `SSHKey::signString` does for a local key.
    static String signString(const String & key_blob, std::string_view data, std::string_view sig_namespace);
};

}

#endif
