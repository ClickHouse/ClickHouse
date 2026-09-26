#include <Client/ConnectionParameters.h>

#include <Core/Defines.h>
#include <Core/Protocol.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/SSHAgent.h>
#include <Common/isLocalAddress.h>
#include <Common/DNSResolver.h>
#include <Client/ClientBaseHelpers.h>

#include <readpassphrase/readpassphrase.h>

#include <fmt/ranges.h>

#include <cstdio>
#include <filesystem>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SSH_AGENT_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

bool enableSecureConnection(const Poco::Util::AbstractConfiguration & config, const std::string & connection_host,
                            const std::optional<UInt16> & connection_port = std::nullopt)
{
    if (config.getBool("secure", false))
        return true;

    if (config.getBool("no-secure", false))
        return false;

    if (isCloudEndpoint(connection_host))
        return true;

    if (connection_port && connection_port.value() == DBMS_DEFAULT_SECURE_PORT)
        return true;

    return false;
}

#if USE_SSH

namespace fs = std::filesystem;

/// The public key that `ssh-keygen` writes next to a private key, in the SSH wire format.
/// It is only used to recognize the key among the ones the ssh-agent holds, so a `.pub` file that is
/// missing, unreadable, malformed, or stale simply means "this key cannot be matched against the agent":
/// it must not prevent the private key itself from being used, and it must never select a different key.
String readPublicKeyBlob(const String & private_key_filename)
{
    String filename = private_key_filename + ".pub";
    if (!fs::is_regular_file(filename) || ::access(filename.c_str(), R_OK) != 0)
        return {};

    String contents;
    ReadBufferFromFile in(filename);
    readStringUntilEOF(contents, in);

    /// The format of the file is: the type of the key, the base64-encoded key, and an optional comment.
    static constexpr std::string_view whitespace = " \t\r\n";
    size_t key_begin = contents.find_first_of(whitespace);
    if (key_begin != String::npos)
        key_begin = contents.find_first_not_of(whitespace, key_begin);
    if (key_begin == String::npos)
        return {};
    size_t key_end = contents.find_first_of(whitespace, key_begin);

    String key = contents.substr(key_begin, key_end - key_begin);

    /// `base64Decode` throws on anything outside the alphabet, as well as on misplaced padding
    /// (such as `Zm9vYmF=Zm9v`), and a malformed key is not an error here.
    static constexpr std::string_view base64_alphabet
        = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    if (key.empty() || key.size() % 4 != 0)
        return {};
    size_t padding = 0;
    while (padding < key.size() && key[key.size() - 1 - padding] == '=')
        ++padding;
    if (padding > 2 || padding == key.size())
        return {};
    if (std::string_view(key).substr(0, key.size() - padding).find_first_not_of(base64_alphabet) != String::npos)
        return {};

    String blob = base64Decode(key);

    /// A `.pub` file that does not belong to the key file next to it must not decide which key we
    /// authenticate with. It can only be checked when the private key is readable without a passphrase;
    /// an encrypted key - exactly the case the agent is useful for - has to be taken on trust, as `ssh` does.
    if (std::optional<SSHKey> private_key = SSHKeyFactory::tryMakePrivateKeyFromFileWithoutPassphrase(private_key_filename))
        if (base64Decode(private_key->getBase64()) != blob)
            return {};

    return blob;
}

String askPassphrase(const String & key_name)
{
    String prompt = fmt::format("Enter the passphrase for the SSH key {}: ", key_name);
    char buf[1000] = {};
    if (auto * result = readpassphrase(prompt.c_str(), buf, sizeof(buf), 0))
        return result;
    return {};
}

SSHKey loadPrivateKey(const String & filename, const std::optional<String> & passphrase)
{
    SSHKey key = SSHKeyFactory::makePrivateKeyFromFile(filename, passphrase, [&filename] { return askPassphrase(filename); });
    if (!key.isPrivate())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "File {} did not contain a private key (is it a public key?)", filename);
    return key;
}

/// The key held by the ssh-agent that corresponds to one of the identity files, if any.
std::optional<SSHAgent::Identity> findIdentityInSSHAgent(const std::vector<String> & identity_files, const String & agent_socket_path)
{
    if (!SSHAgent::isAvailable(agent_socket_path))
        return {};

    std::vector<SSHAgent::Identity> identities;
    try
    {
        identities = SSHAgent::listIdentities(agent_socket_path);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::SSH_AGENT_ERROR)
            throw;
        return {};
    }
    for (const String & identity_file : identity_files)
    {
        String key_blob = readPublicKeyBlob(identity_file);
        if (key_blob.empty())
            continue;

        for (const SSHAgent::Identity & identity : identities)
            if (identity.key_blob == key_blob)
                return identity;
    }

    return {};
}

/// musl defines `stderr` as `(stderr)`, which triggers `-Wdisabled-macro-expansion` when passed to `fmt::print`.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"

/// Preferring the ssh-agent for an identity must not throw away its local key file: if the agent
/// refuses to sign - a confirmation-required key, a restarted agent, an agent that lists the key but
/// cannot produce the required signature type - the key file of the same identity is used instead.
SSHKey::FallbackKeyLoader makeKeyFileFallback(const String & filename, const std::optional<String> & passphrase)
{
    if (!fs::is_regular_file(filename))
        return {};

    return [filename, passphrase](const String & agent_error)
    {
        fmt::print(stderr, "The ssh-agent could not sign with the selected key ({}). Using the key file {} instead.\n", agent_error, filename);
        return loadPrivateKey(filename, passphrase);
    };
}

/// Finds the key to authenticate with: the one that `ssh` would use for this host,
/// either from the ssh-agent, or from a file in `~/.ssh`, unless the file name is given explicitly.
SSHKey getSSHKey(const String & host, const String & user, UInt16 port, const String & filename, const std::optional<String> & passphrase)
{
    /// An explicitly named key file does not depend on the ssh configuration at all: the configuration
    /// cannot change which key is used here, so a syntax error in an unrelated `Match` or `Include`
    /// stanza of `~/.ssh/config` must not prevent the named file from being used.
    if (!filename.empty())
    {
        String explicit_agent_socket_path = SSHAgent::getSocketPath();

        /// If the key is also held by the ssh-agent, use the agent: this way the passphrase is not needed.
        if (!passphrase.has_value())
            if (auto identity = findIdentityInSSHAgent({filename}, explicit_agent_socket_path))
                return SSHKeyFactory::makeKeyFromSSHAgent(
                    identity->key_blob, explicit_agent_socket_path, makeKeyFileFallback(filename, passphrase));

        return loadPrivateKey(filename, passphrase);
    }

    SSHClientConfiguration configuration = getSSHClientConfiguration(host, user, port);
    String agent_socket_path = configuration.agent_socket_path.value_or(SSHAgent::getSocketPath());

    /// The configured identities are tried in order: the first available one is used.
    /// For each identity, its copy held by the ssh-agent is preferred: it does not need a passphrase.
    /// But if the passphrase is specified, the user obviously wants the key file to be used.
    std::vector<SSHAgent::Identity> agent_identities;
    if (!passphrase.has_value() && SSHAgent::isAvailable(agent_socket_path))
    {
        try
        {
            agent_identities = SSHAgent::listIdentities(agent_socket_path);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::SSH_AGENT_ERROR)
                throw;
        }
    }

    /// The configured identities whose key file exists but could not be imported.
    std::vector<String> unusable_identity_files;

    for (const String & identity_file : configuration.identity_files)
    {
        if (!agent_identities.empty())
        {
            String key_blob = readPublicKeyBlob(identity_file);
            if (!key_blob.empty())
            {
                for (const SSHAgent::Identity & identity : agent_identities)
                {
                    if (identity.key_blob == key_blob)
                    {
                        fmt::print(stderr, "Using the SSH key '{}' from the ssh-agent.\n", identity.comment);
                        return SSHKeyFactory::makeKeyFromSSHAgent(
                            identity.key_blob, agent_socket_path, makeKeyFileFallback(identity_file, passphrase));
                    }
                }
            }
        }

        if (!fs::is_regular_file(identity_file))
            continue;

        /// A configured key file that exists but cannot be imported - unreadable, malformed, or encrypted
        /// with a different passphrase - is not an available identity. `ssh` skips it and moves on to the
        /// next configured identity, and so do we; the error is reported only if no identity can be used.
        SSHKey key;
        try
        {
            key = loadPrivateKey(identity_file, passphrase);
        }
        catch (const Exception & e)
        {
            fmt::print(stderr, "Cannot use the SSH key from {}: {}\n", identity_file, e.message());
            unusable_identity_files.push_back(identity_file);
            continue;
        }

        fmt::print(stderr, "Using the SSH key from {}.\n", identity_file);
        return key;
    }

    /// None of the configured identities is available, but the agent may still hold a key that the server knows about.
    /// `IdentitiesOnly yes` forbids exactly this: only the configured identities may be used.
    if (!configuration.identities_only && !agent_identities.empty())
    {
        fmt::print(stderr, "Using the SSH key '{}' from the ssh-agent.\n", agent_identities.front().comment);
        return SSHKeyFactory::makeKeyFromSSHAgent(agent_identities.front().key_blob, agent_socket_path);
    }

    if (!unusable_identity_files.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No usable SSH key found: none of these files contains a key that could be used: {}. "
            "Specify the key file with --ssh-key-file <path>, or add a key to the ssh-agent",
            fmt::join(unusable_identity_files, ", "));

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "No SSH key found: none of these files exists: {}. "
        "Specify the key file with --ssh-key-file <path>, or add a key to the ssh-agent",
        fmt::join(configuration.identity_files, ", "));
}

#pragma clang diagnostic pop

#endif

}

ConnectionParameters ConnectionParameters::createForEmbedded(const String & user, const String & database)
{
    auto connection_params = ConnectionParameters();
    connection_params.host = "localhost";
    connection_params.security = Protocol::Secure::Disable;
    connection_params.password = "";
    connection_params.user = user;
    connection_params.default_database = database;
    connection_params.compression = Protocol::Compression::Disable;

    /// We don't need to configure the timeouts for the embedded client.

    connection_params.timeouts.sync_request_timeout = Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0);
    return connection_params;
}

ConnectionParameters::ConnectionParameters(const Poco::Util::AbstractConfiguration & config,
                                           const Host & host_,
                                           const Database & database,
                                           std::optional<UInt16> port_)
    : host(host_)
    , port(port_.value_or(getPortFromConfig(config, host_)))
    , default_database(database)
{
    security = enableSecureConnection(config, host_, port) ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    tls_sni_override = config.getString("tls-sni-override", "");

    bind_host = config.getString("bind_host", "");

    /// changed the default value to "default" to fix the issue when the user in the prompt is blank
    user = config.getString("user", "default");

    if (config.has("jwt"))
    {
#if USE_JWT_CPP && USE_SSL
        jwt = config.getString("jwt");
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "JWT is disabled, because ClickHouse is built without JWT or SSL support");
#endif
    }
    else if (config.has("ssh-key-file"))
    {
#if USE_SSH
        /// The file name can be empty: then the key is looked up the same way as `ssh` does it.
        std::string filename = config.getString("ssh-key-file");

        std::optional<std::string> passphrase;
        if (config.has("ssh-key-passphrase"))
            passphrase = config.getString("ssh-key-passphrase");

        ssh_private_key = getSSHKey(host, user, port, filename, passphrase);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSH is disabled, because ClickHouse is built without libssh");
#endif
    }
    else
    {
        bool password_prompt = false;
        if (config.getBool("ask-password", false))
        {
            if (config.has("password"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Specified both --password and --ask-password. Remove one of them");
            password_prompt = true;
        }
        else
        {
            password = config.getString("password", "");
            /// if the value of --password is omitted, the password will be set implicitly to "\n"
            if (password == ASK_PASSWORD)
                password_prompt = true;
        }
        if (password_prompt)
        {
            std::string prompt{"Password for user (" + user + "): "};
            char buf[1000] = {};
            if (auto * result = readpassphrase(prompt.c_str(), buf, sizeof(buf), 0))
                password = result;
        }

        if (config.has("one-time-password"))
        {
            password += "+";
            password += config.getString("one-time-password");
        }
        else if (config.getBool("ask-password-2fa", false))
        {
            std::string prompt{"TOTP for user (" + user + "): "};
            char buf[1000] = {};
            if (auto * result = readpassphrase(prompt.c_str(), buf, sizeof(buf), RPP_ECHO_ON))
                password += "+" + std::string(result);
        }
    }

    proto_send_chunked = config.getString("proto_caps.send", "notchunked");
    proto_recv_chunked = config.getString("proto_caps.recv", "notchunked");

    quota_key = config.getString("quota_key", "");

    /// By default compression is disabled if address looks like localhost.

    /// Avoid DNS request if the host is "localhost".
    /// If ClickHouse is run under QEMU-user with a binary for a different architecture,
    /// and there are all listed startup dependency shared libraries available, but not the runtime dependencies of glibc,
    /// the glibc cannot open "plugins" for DNS resolving, and the DNS resolution does not work.
    /// At the same time, I want clickhouse-local to always work, regardless.
    /// TODO: get rid of glibc, or replace getaddrinfo to c-ares.

    compression = config.getBool("compression", host != "localhost" && !isLocalAddress(DNSResolver::instance().resolveHostAllInOriginOrder(host).front()))
                  ? Protocol::Compression::Enable : Protocol::Compression::Disable;

    timeouts = ConnectionTimeouts()
            .withConnectionTimeout(
                Poco::Timespan(config.getInt("connect_timeout", DBMS_DEFAULT_CONNECT_TIMEOUT_SEC), 0))
            .withSendTimeout(
                Poco::Timespan(config.getInt("send_timeout", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0))
            .withReceiveTimeout(
                Poco::Timespan(config.getInt("receive_timeout", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0))
            .withTCPKeepAliveTimeout(
                Poco::Timespan(config.getInt("tcp_keep_alive_timeout", DEFAULT_TCP_KEEP_ALIVE_TIMEOUT), 0))
            .withHandshakeTimeout(
                Poco::Timespan(config.getInt("handshake_timeout_ms", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC * 1000) * 1000))
            .withSyncRequestTimeout(
                Poco::Timespan(config.getInt("sync_request_timeout", DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC), 0));
}

ConnectionParameters::ConnectionParameters(const Poco::Util::AbstractConfiguration & config_, const Host & host_, const Database & database_)
    : ConnectionParameters(config_, host_, database_, getPortFromConfig(config_, host_))
{

}

UInt16 ConnectionParameters::getPortFromConfig(const Poco::Util::AbstractConfiguration & config,
                                               const std::string & connection_host)
{
    bool is_secure = enableSecureConnection(config, connection_host);
    return static_cast<UInt16>(config.getInt(
        "port",
        static_cast<UInt16>(
            config.getInt(is_secure ? "tcp_port_secure" : "tcp_port", is_secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT))));
}
}
