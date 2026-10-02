#include <Common/SSHWrapper.h>

# if USE_SSH
#    include <Common/Crypto/OpenSSLInitializer.h>
#    include <Common/SSHAgent.h>
#    include <Poco/DigestEngine.h>
#    include <Poco/SHA1Engine.h>
#    include <base/scope_guard.h>

#    include <algorithm>
#    include <cstdlib>
#    include <cctype>
#    include <cstring>
#    include <memory>
#    include <string>
#    include <pwd.h>
#    include <unistd.h>

#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#    pragma clang diagnostic ignored "-Wreserved-identifier"

#    include <libssh/libssh.h>

#    pragma clang diagnostic pop

namespace DB
{

namespace ErrorCodes
{
    extern const int LIBSSH_ERROR;
    extern const int SSH_AGENT_ERROR;
}

namespace
{

/// The namespace of the signatures, as defined by the `SSHSIG` format. The server expects exactly this one.
constexpr const char * SIGNATURE_NAMESPACE = "clickhouse";

struct SSHStringDeleter
{
    void operator()(char * ptr) const { ssh_string_free_char(ptr); }
};
struct CStringDeleter
{
    void operator()(char * ptr) const { std::free(ptr); }
};

bool isEd25519KeyType(enum ssh_keytypes_e key_type)
{
    return key_type == SSH_KEYTYPE_ED25519 || key_type == SSH_KEYTYPE_ED25519_CERT01
        || key_type == SSH_KEYTYPE_SK_ED25519 || key_type == SSH_KEYTYPE_SK_ED25519_CERT01;
}

/// Ed25519 is not FIPS-approved. In FIPS mode libssh "successfully" imports an Ed25519 public key
/// but leaves the underlying EVP_PKEY empty, and the first use of such a half-initialized key
/// (copy, comparison, base64 export) dereferences a null pointer.
void checkKeyIsUsableInFIPSMode(ssh_key key)
{
    if (key != nullptr && OpenSSLInitializer::instance().isFIPSEnabled() && isEd25519KeyType(ssh_key_type(key)))
    {
        ssh_key_free(key);
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Ed25519 SSH keys are not supported in FIPS mode");
    }
}

/// The system-wide client configuration, the same file that libssh and `ssh` read.
constexpr std::string_view GLOBAL_SSH_CONFIG_FILE = "/etc/ssh/ssh_config";

String getHomeDirectory()
{
    const char * home_directory = std::getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    if (home_directory && *home_directory)
        return home_directory;

    /// `HOME` is not set: fall back to the passwd database, the same as libssh does.
    passwd entry{};
    passwd * result = nullptr;
    char buffer[16384];
    if (getpwuid_r(getuid(), &entry, buffer, sizeof(buffer), &result) != 0 || result == nullptr)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot determine the home directory of the current user");

    return entry.pw_dir;
}

/// `HOST_NAME_MAX` is not defined on macOS and FreeBSD, and POSIX only guarantees `_POSIX_HOST_NAME_MAX` (255).
constexpr size_t MAX_HOST_NAME_LENGTH = 256;

/// The host name of this machine, the way `ssh` reports it in `%l`.
String getLocalHostName()
{
    char buffer[MAX_HOST_NAME_LENGTH + 1] = {};
    if (gethostname(buffer, sizeof(buffer) - 1) != 0)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot determine the host name of this machine");
    return buffer;
}

String getLocalUserName()
{
    passwd entry{};
    passwd * result = nullptr;
    char buffer[16384];
    if (getpwuid_r(getuid(), &entry, buffer, sizeof(buffer), &result) != 0 || result == nullptr)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot determine the name of the current user");

    return entry.pw_name;
}

/// The values that `ssh` substitutes for the `%`-tokens of `IdentityFile` and `IdentityAgent`.
struct ExpansionTokens
{
    String home_directory;   /// `%d`
    String local_host;       /// `%l`
    String short_local_host; /// `%L`
    String host;             /// `%h`, the host to connect to, after the `Hostname` substitution
    String original_host;    /// `%n`
    String port;             /// `%p`
    String remote_user;      /// `%r`
    String local_user;       /// `%u`
    String uid;              /// `%i`
};

/// `%C` is a hash of the connection, computed exactly the way `ssh` computes it.
String getConnectionHash(const ExpansionTokens & tokens)
{
    Poco::SHA1Engine engine;
    engine.update(tokens.local_host);
    engine.update(tokens.host);
    engine.update(tokens.port);
    engine.update(tokens.remote_user);
    /// `ssh` also hashes the jump host here, but we never have one, and an empty string contributes nothing.
    return Poco::DigestEngine::digestToHex(engine.digest());
}

/// Substitutes the home directory and the `%`-tokens in the value of `IdentityFile` or `IdentityAgent`.
String expandTokens(std::string_view pattern, const ExpansionTokens & tokens)
{
    const std::string_view original_pattern = pattern;
    String result;

    if (pattern.starts_with("~/"))
    {
        result += tokens.home_directory;
        pattern.remove_prefix(1);
    }

    for (size_t i = 0; i < pattern.size(); ++i)
    {
        if (pattern[i] != '%' || i + 1 == pattern.size())
        {
            result += pattern[i];
            continue;
        }

        ++i;
        switch (pattern[i])
        {
            case '%': result += '%'; break;
            case 'C': result += getConnectionHash(tokens); break;
            case 'd': result += tokens.home_directory; break;
            case 'h': result += tokens.host; break;
            case 'i': result += tokens.uid; break;
            case 'L': result += tokens.short_local_host; break;
            case 'l': result += tokens.local_host; break;
            case 'n': result += tokens.original_host; break;
            case 'p': result += tokens.port; break;
            case 'r': result += tokens.remote_user; break;
            case 'u': result += tokens.local_user; break;
            default:
                /// `%j`, the jump host, is the only token of `ssh` that is left: we never connect through one.
                throw Exception(ErrorCodes::LIBSSH_ERROR,
                    "The token %{} of the SSH configuration value '{}' is not supported", pattern[i], original_pattern);
        }
    }

    return result;
}

std::optional<String> expandSSHConfigEnvironmentVariables(std::string_view value)
{
    String result;
    for (size_t i = 0; i < value.size(); ++i)
    {
        if (value[i] != '$')
        {
            result += value[i];
            continue;
        }

        size_t variable_begin = i + 1;
        size_t variable_end = variable_begin;
        if (variable_begin < value.size() && value[variable_begin] == '{')
        {
            variable_begin += 1;
            variable_end = value.find('}', variable_begin);
            if (variable_end == String::npos)
                return std::nullopt;
            i = variable_end;
        }
        else
        {
            while (variable_end < value.size() && (std::isalnum(static_cast<unsigned char>(value[variable_end])) || value[variable_end] == '_'))
                ++variable_end;
            if (variable_begin == variable_end)
            {
                result += '$';
                continue;
            }
            i = variable_end - 1;
        }

        String variable_name = String(value.substr(variable_begin, variable_end - variable_begin));
        const char * variable_value = std::getenv(variable_name.c_str()); // NOLINT(concurrency-mt-unsafe)
        if (variable_value == nullptr)
            return std::nullopt;
        result += variable_value;
    }
    return result;
}

/// The socket of the ssh-agent configured by `IdentityAgent`, if the configuration specifies one.
std::optional<String> getAgentSocketPath(ssh_session session, const ExpansionTokens & tokens)
{
    char * configured_socket_path = nullptr;
    if (ssh_options_get(session, SSH_OPTIONS_IDENTITY_AGENT, &configured_socket_path) != SSH_OK)
        return std::nullopt;
    std::unique_ptr<char, SSHStringDeleter> configured_socket_path_ptr(configured_socket_path);

    String socket_path = configured_socket_path_ptr.get();
    if (socket_path == "none")
        return String{};
    if (socket_path == "SSH_AUTH_SOCK")
    {
        const char * environment_socket_path = std::getenv("SSH_AUTH_SOCK"); // NOLINT(concurrency-mt-unsafe)
        return environment_socket_path ? String(environment_socket_path) : String{};
    }

    auto expanded_socket_path = expandSSHConfigEnvironmentVariables(socket_path);
    if (!expanded_socket_path)
        return std::nullopt;
    return expandTokens(*expanded_socket_path, tokens);
}

/// Passed to libssh, which calls it only if the private key turns out to be encrypted.
int askPassphrase(const char * /*prompt*/, char * buf, size_t len, int /*echo*/, int /*verify*/, void * userdata)
{
    const auto & callback = *static_cast<const SSHKeyFactory::PassphraseCallback *>(userdata);
    String passphrase = callback();
    if (passphrase.size() >= len)
        return -1;
    memcpy(buf, passphrase.data(), passphrase.size());
    buf[passphrase.size()] = 0;
    return 0;
}

}

SSHKey SSHKeyFactory::makePrivateKeyFromFile(const String & filename, const std::optional<String> & passphrase, PassphraseCallback ask_passphrase)
{
    ssh_key key = nullptr;
    int rc = ssh_pki_import_privkey_file(
        filename.c_str(),
        passphrase ? passphrase->c_str() : nullptr,
        ask_passphrase ? askPassphrase : nullptr,
        &ask_passphrase,
        &key);
    if (rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't import SSH private key from file {}", filename);
    checkKeyIsUsableInFIPSMode(key);
    return SSHKey(key);
}

std::optional<SSHKey> SSHKeyFactory::tryMakePrivateKeyFromFileWithoutPassphrase(const String & filename)
{
    ssh_key key = nullptr;
    /// A non-null passphrase means that the callback asking the user for one is never invoked.
    if (int rc = ssh_pki_import_privkey_file(filename.c_str(), "", nullptr, nullptr, &key); rc != SSH_OK)
        return {};
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicKeyFromFile(String filename)
{
    ssh_key key = nullptr;
    if (int rc = ssh_pki_import_pubkey_file(filename.c_str(), &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't import SSH public key from file");
    checkKeyIsUsableInFIPSMode(key);
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicKeyFromBase64(String base64_key, String type_name)
{
    ssh_key key = nullptr;
    auto key_type = ssh_key_type_from_name(type_name.c_str());
    if (OpenSSLInitializer::instance().isFIPSEnabled() && isEd25519KeyType(key_type))
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Ed25519 SSH keys are not supported in FIPS mode");
    if (int rc = ssh_pki_import_pubkey_base64(base64_key.c_str(), key_type, &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Bad SSH public key provided");
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makeKeyFromSSHAgent(String key_blob, String agent_socket_path, SSHKey::FallbackKeyLoader fallback_loader)
{
    /// The agent signs with a key we never import into OpenSSL, so apply the same FIPS restriction here.
    if (OpenSSLInitializer::instance().isFIPSEnabled()
        && isEd25519KeyType(ssh_key_type_from_name(SSHAgent::getKeyType(key_blob).c_str())))
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Ed25519 SSH keys are not supported in FIPS mode");

    SSHKey key;
    key.agent_key_blob = std::move(key_blob);
    key.agent_socket_path = std::move(agent_socket_path);
    key.agent_fallback_loader = std::move(fallback_loader);
    return key;
}

SSHKey::SSHKey(const SSHKey & other)
{
    key = ssh_key_dup(other.key);
    agent_key_blob = other.agent_key_blob;
    agent_socket_path = other.agent_socket_path;
    agent_fallback_loader = other.agent_fallback_loader;
}

SSHKey::SSHKey(SSHKey && other) noexcept
{
    key = other.key;
    other.key = nullptr;
    agent_key_blob = std::move(other.agent_key_blob);
    agent_socket_path = std::move(other.agent_socket_path);
    agent_fallback_loader = std::move(other.agent_fallback_loader);
}

SSHKey & SSHKey::operator=(const SSHKey & other)
{
    if (&other == this)
        return *this;
    ssh_key_free(key);
    key = ssh_key_dup(other.key);
    agent_key_blob = other.agent_key_blob;
    agent_socket_path = other.agent_socket_path;
    agent_fallback_loader = other.agent_fallback_loader;
    return *this;
}

SSHKey & SSHKey::operator=(SSHKey && other) noexcept
{
    ssh_key_free(key);
    key = other.key;
    other.key = nullptr;
    agent_key_blob = std::move(other.agent_key_blob);
    agent_socket_path = std::move(other.agent_socket_path);
    agent_fallback_loader = std::move(other.agent_fallback_loader);
    return *this;
}

bool SSHKey::operator==(const SSHKey & other) const
{
    return isEqual(other);
}

bool SSHKey::isEqual(const SSHKey & other) const
{
    int rc = ssh_key_cmp(key, other.key, SSH_KEY_CMP_PUBLIC);
    return rc == 0;
}

String SSHKey::signString(std::string_view input) const
{
    if (!agent_key_blob.empty())
    {
        if (!agent_fallback_loader)
            return SSHAgent::signString(agent_key_blob, input, SIGNATURE_NAMESPACE, agent_socket_path);

        /// The agent was only preferred because it saves the passphrase. If it cannot sign after all,
        /// the local key file of the same identity is still there, and `ssh` would use it as well.
        try
        {
            return SSHAgent::signString(agent_key_blob, input, SIGNATURE_NAMESPACE, agent_socket_path);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::SSH_AGENT_ERROR)
                throw;
            return agent_fallback_loader(e.message()).signString(input);
        }
    }

    char * signature = nullptr;
    if (int rc = sshsig_sign(input.data(), input.size(), key, nullptr, SIGNATURE_NAMESPACE, SSHSIG_DIGEST_SHA2_256, &signature); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Error signing with ssh key");
    std::unique_ptr<char, SSHStringDeleter> sig_ptr(signature);
    return String(sig_ptr.get());
}

bool SSHKey::verifySignature(std::string_view signature, std::string_view original) const
{
    ssh_key verify_key = nullptr;
    String sig_str(signature);
    int rc = sshsig_verify(original.data(), original.size(), sig_str.c_str(), SIGNATURE_NAMESPACE, &verify_key);
    if (rc != SSH_OK)
    {
        if (verify_key != nullptr)
            ssh_key_free(verify_key);
        return false;
    }
    bool keys_match = false;
    if (verify_key != nullptr)
    {
        keys_match = (ssh_key_cmp(key, verify_key, SSH_KEY_CMP_PUBLIC) == 0);
        ssh_key_free(verify_key);
    }
    return keys_match;
}

bool SSHKey::isPrivate() const
{
    return ssh_key_is_private(key);
}

bool SSHKey::isPublic() const
{
    return ssh_key_is_public(key);
}

String SSHKey::getBase64() const
{
    char * buf = nullptr;
    if (int rc = ssh_pki_export_pubkey_base64(key, &buf); rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::LIBSSH_ERROR, "Failed to export public key to base64");
    /// Create a String from cstring, which makes a copy of the first one and requires freeing memory after it
    /// This is to safely manage buf memory
    std::unique_ptr<char, CStringDeleter> buf_ptr(buf);
    return String(buf_ptr.get());
}

String SSHKey::getKeyType() const
{
    return ssh_key_type_to_char(ssh_key_type(key));
}

void SSHKey::setNeedsDeallocation(bool needs_deallocation_)
{
    needs_deallocation = needs_deallocation_;
}

SSHKey::~SSHKey()
{
    if (needs_deallocation)
        ssh_key_free(key);
}

SSHClientConfiguration getSSHClientConfiguration(const String & host, const String & user, UInt16 port)
{
    /// libssh already knows how to read `~/.ssh/config`, we only have to ask it for the result.
    ssh_session session = ssh_new();
    if (session == nullptr)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot create an SSH session");
    SCOPE_EXIT({ ssh_free(session); });

    /// The host, the user and the port are needed to select the matching `Host` and `Match` sections of the config.
    unsigned int port_number = port;
    if (ssh_options_set(session, SSH_OPTIONS_HOST, host.c_str()) != SSH_OK
        || ssh_options_set(session, SSH_OPTIONS_USER, user.c_str()) != SSH_OK
        || ssh_options_set(session, SSH_OPTIONS_PORT, &port_number) != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot set the parameters of an SSH session: {}", ssh_get_error(session));

    /// libssh prepends each `IdentityFile` of the config to the list of the built-in default identities
    /// (`~/.ssh/id_ed25519` and so on), so the configured identities come out in the reverse order.
    /// Count the defaults up front to restore the order of the configured identities afterwards.
    size_t default_identity_count = 0;
    {
        char * pattern = nullptr;
        while (ssh_options_get(session, SSH_OPTIONS_NEXT_IDENTITY, &pattern) == SSH_OK)
        {
            std::unique_ptr<char, SSHStringDeleter> pattern_ptr(pattern);
            ++default_identity_count;
        }
    }

    /// The names of the config files are passed explicitly, because libssh takes the home directory
    /// from the passwd database, while the rest of the client honors the `HOME` environment variable.
    String home_directory = getHomeDirectory();
    for (const String & config_file : {home_directory + "/.ssh/config", String(GLOBAL_SSH_CONFIG_FILE)})
        if (ssh_options_parse_config(session, config_file.c_str()) != SSH_OK)
            throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot parse the SSH configuration file {}: {}", config_file, ssh_get_error(session));

    ExpansionTokens tokens;
    tokens.home_directory = home_directory;
    tokens.local_host = getLocalHostName();
    tokens.short_local_host = tokens.local_host.substr(0, tokens.local_host.find('.'));
    tokens.host = host;
    tokens.original_host = host;
    tokens.port = std::to_string(port);
    tokens.remote_user = user;
    tokens.local_user = getLocalUserName();
    tokens.uid = std::to_string(getuid());

    /// `%h` is the host after the `Hostname` substitution of the config, while `%n` is the one we were given.
    char * configured_host = nullptr;
    if (ssh_options_get(session, SSH_OPTIONS_HOST, &configured_host) == SSH_OK)
    {
        std::unique_ptr<char, SSHStringDeleter> configured_host_ptr(configured_host);
        tokens.host = configured_host_ptr.get();
    }

    SSHClientConfiguration result;

    /// The identities from the config come first, in the order they are written in the config,
    /// the built-in default ones (`~/.ssh/id_ed25519` and so on) last.
    /// They are not expanded by libssh, because that would also resolve the home directory from the passwd database.
    std::vector<String> identity_patterns;
    char * pattern = nullptr;
    while (ssh_options_get(session, SSH_OPTIONS_NEXT_IDENTITY, &pattern) == SSH_OK)
    {
        std::unique_ptr<char, SSHStringDeleter> pattern_ptr(pattern);
        identity_patterns.emplace_back(pattern_ptr.get());
    }
    chassert(identity_patterns.size() >= default_identity_count);
    std::reverse(identity_patterns.begin(), identity_patterns.end() - default_identity_count);
    for (const String & identity_pattern : identity_patterns)
    {
        /// `ssh` also substitutes `${...}` environment variables in `IdentityFile`.
        /// An entry referencing an unset variable names no file, so it is skipped, and the other identities still apply.
        auto expanded_pattern = expandSSHConfigEnvironmentVariables(identity_pattern);
        if (!expanded_pattern)
            continue;
        result.identity_files.push_back(expandTokens(*expanded_pattern, tokens));
    }

    result.agent_socket_path = getAgentSocketPath(session, tokens);

    char * identities_only = nullptr;
    if (ssh_options_get(session, SSH_OPTIONS_IDENTITIES_ONLY, &identities_only) == SSH_OK)
    {
        std::unique_ptr<char, SSHStringDeleter> identities_only_ptr(identities_only);
        result.identities_only = identities_only_ptr.get() == std::string_view("yes");
    }

    return result;
}

}

#endif
