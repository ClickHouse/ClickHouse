#include <Common/SSHWrapper.h>

#if USE_SSL
#    include <Common/Crypto/OpenSSLInitializer.h>
#endif

#if USE_SSH
#    include <Common/Base64.h>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/ReadHelpers.h>

#    include <string_view>

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
struct SSHStringDeleter
{
    void operator()(char * ptr) const { ssh_string_free_char(ptr); }
};
struct CStringDeleter
{
    void operator()(char * ptr) const { std::free(ptr); }
};

bool isKeyTypeUsableInFIPSBuilds(enum ssh_keytypes_e key_type)
{
    /// Ed25519 is not FIPS-approved: importing it in FIPS mode in libssh will cause bad things to happen.
    /// Every other supported key type (RSA, ECDSA, ...) is usable, so return "not usable" only for the Ed25519 family.
    return !(key_type == SSH_KEYTYPE_ED25519 || key_type == SSH_KEYTYPE_ED25519_CERT01
        || key_type == SSH_KEYTYPE_SK_ED25519 || key_type == SSH_KEYTYPE_SK_ED25519_CERT01);
}

void checkIfKeyCanBeUsedInFIPSBuilds(ssh_key key)
{
    if (key != nullptr
        && (OpenSSLInitializer::instance().isFIPSEnabled() && !isKeyTypeUsableInFIPSBuilds(ssh_key_type(key))))
    {
        ssh_key_free(key);
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Ed25519 SSH keys are not supported in FIPS mode");
    }
}

/// Read an SSH wire-format string (big-endian uint32 length prefix + bytes) from the front of `data`,
/// advancing `data` past it. Returns false when `data` is too short.
bool readSSHWireString(std::string_view & data, std::string_view & out)
{
    if (data.size() < 4)
        return false;
    const auto * p = reinterpret_cast<const unsigned char *>(data.data());
    size_t len = (size_t(p[0]) << 24) | (size_t(p[1]) << 16) | (size_t(p[2]) << 8) | size_t(p[3]);
    if (data.size() - 4 < len)
        return false;
    out = data.substr(4, len);
    data.remove_prefix(4 + len);
    return true;
}

String readFileContents(const String & filename)
{
    String contents;
    ReadBufferFromFile in(filename);
    readStringUntilEOF(contents, in);
    return contents;
}

/// Determine the key type of a private key file WITHOUT importing it into libssh (importing an
/// Ed25519 key under FIPS mode crashes). Only the OpenSSH container ("openssh-key-v1") can hold an
/// Ed25519 key; the unencrypted header always exposes the public key type, so this works even for
/// passphrase-protected keys. Anything else (legacy PEM RSA/EC/DSA) is reported as UNKNOWN, which
/// callers treat as "not Ed25519, safe to import".
enum ssh_keytypes_e detectPrivateKeyType(const String & filename)
{
    String contents;
    try
    {
        contents = readFileContents(filename);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: best-effort detection. An unreadable file is reported as UNKNOWN and handed to
        /// libssh, which surfaces the real error at import time.
        return SSH_KEYTYPE_UNKNOWN;
    }

    static constexpr std::string_view begin_marker = "-----BEGIN OPENSSH PRIVATE KEY-----";
    static constexpr std::string_view end_marker = "-----END OPENSSH PRIVATE KEY-----";
    auto begin_pos = contents.find(begin_marker);
    if (begin_pos == String::npos)
        return SSH_KEYTYPE_UNKNOWN;
    begin_pos += begin_marker.size();
    auto end_pos = contents.find(end_marker, begin_pos);
    if (end_pos == String::npos)
        return SSH_KEYTYPE_UNKNOWN;

    String base64_body;
    for (char c : std::string_view(contents).substr(begin_pos, end_pos - begin_pos))
        if (!isspace(static_cast<unsigned char>(c)))
            base64_body += c;

    String blob;
    try
    {
        blob = base64Decode(base64_body);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: best-effort detection. A malformed base64 body is reported as UNKNOWN and handed to
        /// libssh, which surfaces the real error at import time.
        return SSH_KEYTYPE_UNKNOWN;
    }

    static constexpr std::string_view magic = "openssh-key-v1";
    if (blob.size() < magic.size() + 1 || !std::string_view(blob).starts_with(magic))
        return SSH_KEYTYPE_UNKNOWN;

    /// Layout: magic '\0', string ciphername, string kdfname, string kdfoptions, uint32 nkeys,
    /// string pubkey0 (whose own first wire-string is the key type name), ...
    std::string_view rest = std::string_view(blob).substr(magic.size() + 1);
    std::string_view ciphername;
    std::string_view kdfname;
    std::string_view kdfoptions;
    if (!readSSHWireString(rest, ciphername) || !readSSHWireString(rest, kdfname) || !readSSHWireString(rest, kdfoptions))
        return SSH_KEYTYPE_UNKNOWN;
    if (rest.size() < 4)
        return SSH_KEYTYPE_UNKNOWN;
    rest.remove_prefix(4); /// nkeys
    std::string_view pubkey0;
    if (!readSSHWireString(rest, pubkey0))
        return SSH_KEYTYPE_UNKNOWN;
    std::string_view type_name;
    if (!readSSHWireString(pubkey0, type_name))
        return SSH_KEYTYPE_UNKNOWN;

    return ssh_key_type_from_name(String(type_name).c_str());
}

/// Determine the key type of a public key file ("<type> <base64> [comment]") from its first token,
/// without importing it into libssh.
enum ssh_keytypes_e detectPublicKeyType(const String & filename)
{
    String contents;
    try
    {
        contents = readFileContents(filename);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: best-effort detection. An unreadable file is reported as UNKNOWN and handed to
        /// libssh, which surfaces the real error at import time.
        return SSH_KEYTYPE_UNKNOWN;
    }

    auto start = contents.find_first_not_of(" \t\r\n");
    if (start == String::npos)
        return SSH_KEYTYPE_UNKNOWN;
    auto stop = contents.find_first_of(" \t\r\n", start);
    String token = contents.substr(start, stop == String::npos ? String::npos : stop - start);
    return ssh_key_type_from_name(token.c_str());
}
}

SSHKey SSHKeyFactory::makePrivateKeyFromFile(String filename, String passphrase)
{
    /// Reject unsupported key types BEFORE ssh_pki_import_privkey_file: handing an Ed25519 key to
    /// libssh under FIPS mode crashes (this path is reachable via clickhouse-client --ssh-key-file).
    if (OpenSSLInitializer::instance().isFIPSEnabled() && !isKeyTypeUsableInFIPSBuilds(detectPrivateKeyType(filename)))
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Ed25519 SSH keys are not supported in FIPS mode");
    ssh_key key = nullptr;
    if (int rc = ssh_pki_import_privkey_file(filename.c_str(), passphrase.c_str(), nullptr, nullptr, &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't import SSH private key from file");
    checkIfKeyCanBeUsedInFIPSBuilds(key);
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicKeyFromFile(String filename)
{
    if (OpenSSLInitializer::instance().isFIPSEnabled() && !isKeyTypeUsableInFIPSBuilds(detectPublicKeyType(filename)))
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Ed25519 SSH keys are not supported in FIPS mode");
    ssh_key key = nullptr;
    if (int rc = ssh_pki_import_pubkey_file(filename.c_str(), &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't import SSH public key from file");
    checkIfKeyCanBeUsedInFIPSBuilds(key);
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicKeyFromBase64(String base64_key, String type_name)
{
    ssh_key key = nullptr;
    auto key_type = ssh_key_type_from_name(type_name.c_str());
    if (OpenSSLInitializer::instance().isFIPSEnabled() && !isKeyTypeUsableInFIPSBuilds(key_type))
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Ed25519 SSH keys are not supported in FIPS mode");
    if (int rc = ssh_pki_import_pubkey_base64(base64_key.c_str(), key_type, &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Bad SSH public key provided");
    checkIfKeyCanBeUsedInFIPSBuilds(key);
    return SSHKey(key);
}

bool SSHKeyFactory::isPublicKeyUsableInFIPSBuilds(const String & type_name)
{
    return isKeyTypeUsableInFIPSBuilds(ssh_key_type_from_name(type_name.c_str()));
}

bool SSHKeyFactory::isPrivateKeyFileUsableInFIPSBuilds(const String & filename)
{
    return isKeyTypeUsableInFIPSBuilds(detectPrivateKeyType(filename));
}

bool SSHKeyFactory::isPublicKeyFileUsableInFIPSBuilds(const String & filename)
{
    return isKeyTypeUsableInFIPSBuilds(detectPublicKeyType(filename));
}

SSHKey::SSHKey(const SSHKey & other)
{
    key = ssh_key_dup(other.key);
}

SSHKey::SSHKey(SSHKey && other) noexcept
{
    key = other.key;
    other.key = nullptr;
}

SSHKey & SSHKey::operator=(const SSHKey & other)
{
    if (&other == this)
        return *this;
    ssh_key_free(key);
    key = ssh_key_dup(other.key);
    return *this;
}

SSHKey & SSHKey::operator=(SSHKey && other) noexcept
{
    ssh_key_free(key);
    key = other.key;
    other.key = nullptr;
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
    char * signature = nullptr;
    if (int rc = sshsig_sign(input.data(), input.size(), key, nullptr, "clickhouse", SSHSIG_DIGEST_SHA2_256, &signature); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Error signing with ssh key");
    std::unique_ptr<char, SSHStringDeleter> sig_ptr(signature);
    return String(sig_ptr.get());
}

bool SSHKey::verifySignature(std::string_view signature, std::string_view original) const
{
    ssh_key verify_key = nullptr;
    String sig_str(signature);
    int rc = sshsig_verify(original.data(), original.size(), sig_str.c_str(), "clickhouse", &verify_key);
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

}

#endif
