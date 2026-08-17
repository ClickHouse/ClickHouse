#pragma once

#include <Common/Exception.h>

#include <functional>
#include <optional>
#include <string_view>
#include <vector>
#include <base/types.h>

#include "config.h"

#if USE_SSH
using ssh_key = struct ssh_key_struct *;

namespace DB
{

class SSHKey
{
public:
    SSHKey() = default;
    ~SSHKey();

    SSHKey(const SSHKey & other);
    SSHKey(SSHKey && other) noexcept;
    SSHKey & operator=(const SSHKey & other);
    SSHKey & operator=(SSHKey && other) noexcept;

    bool operator==(const SSHKey &) const;
    bool isEqual(const SSHKey & other) const;

    bool isEmpty() { return key == nullptr && agent_key_blob.empty(); }
    String signString(std::string_view input) const;
    bool verifySignature(std::string_view signature, std::string_view original) const;

    bool isPublic() const;
    bool isPrivate() const;

    String getBase64() const;
    String getKeyType() const;

    void setNeedsDeallocation(bool needs_deallocation_);

    friend class SSHKeyFactory;

    explicit SSHKey(ssh_key key_) : key(key_) { }

private:
    ssh_key key = nullptr;
    /// If set, the private key is not available here, and the signing is delegated to the ssh-agent.
    /// It is the public key of the key held by the agent, in the SSH wire format.
    String agent_key_blob;
    String agent_socket_path;
    bool needs_deallocation = true;
};


class SSHKeyFactory
{
public:
    /// Called to ask the user for the passphrase of an encrypted private key.
    using PassphraseCallback = std::function<String()>;

    /// The check whether the path is allowed to read for ClickHouse has
    /// (e.g. a file is inside `user_files` directory)
    /// to be done outside of this functions.
    /// If the key is encrypted and `passphrase` is not set, `ask_passphrase` is called to obtain it.
    static SSHKey makePrivateKeyFromFile(const String & filename, const std::optional<String> & passphrase, PassphraseCallback ask_passphrase = {});
    static SSHKey makePublicKeyFromFile(String filename);
    static SSHKey makePublicKeyFromBase64(String base64_key, String type_name);

    /// A key that is held by the ssh-agent: only the public key `key_blob` (in the SSH wire format) is known here,
    /// and every signature is made by the agent.
    static SSHKey makeKeyFromSSHAgent(String key_blob, String agent_socket_path);
};

/// The private key files that `ssh` would try when connecting to `host`:
/// the identity files configured for that host in `~/.ssh/config` (and in the system-wide config),
/// followed by the default identity files, such as `~/.ssh/id_ed25519`.
/// The files do not necessarily exist.
std::vector<String> getSSHIdentityFiles(const String & host);

/// The socket configured by `IdentityAgent` for `host`. `nullopt` means that the
/// configuration did not specify an agent and the `SSH_AUTH_SOCK` environment variable is used.
/// An empty string means that `IdentityAgent none` disables use of the agent.
std::optional<String> getSSHAgentSocketPath(const String & host);

}

#else
class SSHKey
{
public:
    bool operator==(const SSHKey &) const = default;
    [[ noreturn ]] bool isEmpty() { std::terminate(); }
    [[ noreturn ]] String signString(std::string_view) const { std::terminate(); }
};
#endif
