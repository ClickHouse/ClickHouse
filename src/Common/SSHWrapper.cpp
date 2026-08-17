#include <Common/SSHWrapper.h>

# if USE_SSH
#    include <Common/Crypto/OpenSSLInitializer.h>
#    include <Common/SSHAgent.h>
#    include <base/scope_guard.h>

#    include <cstdlib>
#    include <cstring>
#    include <fstream>
#    include <fnmatch.h>
#    include <pwd.h>
#    include <sstream>
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

/// Substitutes the home directory and the host name in the name of an identity file.
/// The other placeholders that `ssh` supports are left as is: a file with such a name simply will not be found.
String expandIdentityFileName(std::string_view pattern, const String & home_directory, const String & host)
{
    String result;

    if (pattern.starts_with("~/"))
    {
        result += home_directory;
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
            case 'd': result += home_directory; break;
            case 'h': result += host; break;
            case '%': result += '%'; break;
            default: result += '%'; result += pattern[i]; break;
        }
    }

    return result;
}

bool matchesHostPattern(const String & patterns, const String & host)
{
    std::istringstream input(patterns);
    String pattern;
    bool matched = false;
    while (input >> pattern)
    {
        if (pattern.starts_with('!'))
        {
            if (fnmatch(pattern.c_str() + 1, host.c_str(), 0) == 0)
                return false;
            continue;
        }

        matched |= fnmatch(pattern.c_str(), host.c_str(), 0) == 0;
    }
    return matched;
}

std::optional<String> findSSHAgentSocketPathInConfig(const String & config_file, const String & host, const String & home_directory)
{
    std::ifstream input(config_file);
    if (!input)
        return std::nullopt;

    bool applies = false;
    String line;
    while (std::getline(input, line))
    {
        size_t comment = line.find('#');
        if (comment != String::npos)
            line.resize(comment);

        std::istringstream line_input(line);
        String keyword;
        line_input >> keyword;
        if (keyword.empty())
            continue;

        String argument;
        std::getline(line_input >> std::ws, argument);
        if (keyword == "Host")
        {
            applies = matchesHostPattern(argument, host);
        }
        else if (applies && keyword == "IdentityAgent")
        {
            if (argument == "none")
                return String{};
            return expandIdentityFileName(argument, home_directory, host);
        }
    }
    return std::nullopt;
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

SSHKey SSHKeyFactory::makeKeyFromSSHAgent(String key_blob, String agent_socket_path)
{
    SSHKey key;
    key.agent_key_blob = std::move(key_blob);
    key.agent_socket_path = std::move(agent_socket_path);
    return key;
}

SSHKey::SSHKey(const SSHKey & other)
{
    key = ssh_key_dup(other.key);
    agent_key_blob = other.agent_key_blob;
    agent_socket_path = other.agent_socket_path;
}

SSHKey::SSHKey(SSHKey && other) noexcept
{
    key = other.key;
    other.key = nullptr;
    agent_key_blob = std::move(other.agent_key_blob);
    agent_socket_path = std::move(other.agent_socket_path);
}

SSHKey & SSHKey::operator=(const SSHKey & other)
{
    if (&other == this)
        return *this;
    ssh_key_free(key);
    key = ssh_key_dup(other.key);
    agent_key_blob = other.agent_key_blob;
    agent_socket_path = other.agent_socket_path;
    return *this;
}

SSHKey & SSHKey::operator=(SSHKey && other) noexcept
{
    ssh_key_free(key);
    key = other.key;
    other.key = nullptr;
    agent_key_blob = std::move(other.agent_key_blob);
    agent_socket_path = std::move(other.agent_socket_path);
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
        return SSHAgent::signString(agent_key_blob, input, SIGNATURE_NAMESPACE, agent_socket_path);

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

std::vector<String> getSSHIdentityFiles(const String & host)
{
    /// libssh already knows how to read `~/.ssh/config`, we only have to ask it for the result.
    ssh_session session = ssh_new();
    if (session == nullptr)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot create an SSH session");
    SCOPE_EXIT({ ssh_free(session); });

    /// The host is needed to select the matching `Host` and `Match` sections of the config.
    if (ssh_options_set(session, SSH_OPTIONS_HOST, host.c_str()) != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot set the host of an SSH session: {}", ssh_get_error(session));

    /// The names of the config files are passed explicitly, because libssh takes the home directory
    /// from the passwd database, while the rest of the client honors the `HOME` environment variable.
    String home_directory = getHomeDirectory();
    for (const String & config_file : {home_directory + "/.ssh/config", String(GLOBAL_SSH_CONFIG_FILE)})
        if (ssh_options_parse_config(session, config_file.c_str()) != SSH_OK)
            throw Exception(ErrorCodes::LIBSSH_ERROR, "Cannot parse the SSH configuration file {}: {}", config_file, ssh_get_error(session));

    /// The identities from the config come first, the built-in default ones (`~/.ssh/id_ed25519` and so on) last.
    /// They are not expanded, because expanding them in libssh would also resolve the home directory from the passwd database.
    std::vector<String> result;
    char * pattern = nullptr;
    while (ssh_options_get(session, SSH_OPTIONS_NEXT_IDENTITY, &pattern) == SSH_OK)
    {
        std::unique_ptr<char, SSHStringDeleter> pattern_ptr(pattern);
        result.push_back(expandIdentityFileName(pattern_ptr.get(), home_directory, host));
    }

    return result;
}

std::optional<String> getSSHAgentSocketPath(const String & host)
{
    String home_directory = getHomeDirectory();
    for (const String & config_file : {home_directory + "/.ssh/config", String(GLOBAL_SSH_CONFIG_FILE)})
        if (auto socket_path = findSSHAgentSocketPathInConfig(config_file, host, home_directory))
            return socket_path;
    return std::nullopt;
}

}

#endif
