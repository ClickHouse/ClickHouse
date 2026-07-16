#include <Common/SSHWrapper.h>

#if USE_SSL
#    include <Common/Crypto/OpenSSLInitializer.h>
#endif

#if USE_SSH
#    include <Common/Base64.h>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/ReadHelpers.h>
#    include <base/scope_guard.h>

#    include <array>
#    include <string_view>
#    include <vector>

#    include <openssl/bio.h>
#    include <openssl/objects.h>
#    include <openssl/pem.h>
#    include <openssl/pkcs12.h>
#    include <openssl/x509.h>

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

/// Decode the base64 body between a "-----BEGIN <label>-----" / "-----END <label>-----" PEM pair.
/// Returns false when the markers are absent or the body is not valid base64.
bool decodePEMBody(const String & contents, std::string_view label, String & out)
{
    const String begin_marker = "-----BEGIN " + String(label) + "-----";
    const String end_marker = "-----END " + String(label) + "-----";
    auto begin_pos = contents.find(begin_marker);
    if (begin_pos == String::npos)
        return false;
    begin_pos += begin_marker.size();
    auto end_pos = contents.find(end_marker, begin_pos);
    if (end_pos == String::npos)
        return false;

    String base64_body;
    for (char c : std::string_view(contents).substr(begin_pos, end_pos - begin_pos))
        if (!isspace(static_cast<unsigned char>(c)))
            base64_body += c;

    try
    {
        out = base64Decode(base64_body);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: best-effort detection. A malformed base64 body is reported as "no match" and the
        /// key is handed to libssh, which surfaces the real error at import time.
        return false;
    }
    return true;
}

/// A PKCS#8 PrivateKeyInfo / X.509 SubjectPublicKeyInfo DER blob carries the algorithm as an OID in
/// its AlgorithmIdentifier. id-Ed25519 is 1.3.101.112, DER-encoded as the OBJECT IDENTIFIER
/// 06 03 2B 65 70. The OID sits near the front of the blob (inside the first inner SEQUENCE), so a
/// bounded substring search for that exact byte sequence reliably distinguishes Ed25519 from
/// RSA/EC/DSA without a full ASN.1 parse or a libssh/OpenSSL import.
bool derContainsEd25519OID(std::string_view der)
{
    static constexpr std::array<char, 5> ed25519_oid = {0x06, 0x03, 0x2b, 0x65, 0x70};
    return std::string_view(der).find(std::string_view(ed25519_oid.data(), ed25519_oid.size())) != std::string_view::npos;
}

/// An encrypted PKCS#8 file ("-----BEGIN ENCRYPTED PRIVATE KEY-----") hides its AlgorithmIdentifier
/// OID inside the encrypted body, so a plain byte scan cannot see it. When the passphrase is known,
/// decrypt just the PKCS#8 envelope (PBKDF2 + AES, both FIPS-approved) to obtain the PrivateKeyInfo
/// and read its OID. This never constructs the asymmetric key, so no Ed25519 key material reaches a
/// crypto provider. Returns true only when the decrypted key is the Ed25519 family. A wrong/absent
/// passphrase (or any other failure) returns false; the key then falls through to UNKNOWN, and if it
/// is genuinely Ed25519 the subsequent libssh import fails to decrypt it too, so it never crashes.
bool encryptedPKCS8IsEd25519(const String & contents, const String & passphrase)
{
    std::unique_ptr<BIO, decltype(&BIO_free)> bio(
        BIO_new_mem_buf(contents.data(), static_cast<int>(contents.size())), BIO_free);
    if (!bio)
        return false;

    X509_SIG * p8_encrypted = PEM_read_bio_PKCS8(bio.get(), nullptr, nullptr, nullptr);
    if (!p8_encrypted)
        return false;
    SCOPE_EXIT({ X509_SIG_free(p8_encrypted); });

    PKCS8_PRIV_KEY_INFO * p8_info = PKCS8_decrypt(p8_encrypted, passphrase.c_str(), static_cast<int>(passphrase.size()));
    if (!p8_info)
        return false;
    SCOPE_EXIT({ PKCS8_PRIV_KEY_INFO_free(p8_info); });

    const ASN1_OBJECT * algorithm = nullptr;
    if (!PKCS8_pkey_get0(&algorithm, nullptr, nullptr, nullptr, p8_info) || !algorithm)
        return false;

    return OBJ_obj2nid(algorithm) == NID_ED25519;
}

/// Determine the key type of a private key file WITHOUT importing it into libssh (importing an
/// Ed25519 key under FIPS mode crashes). Three carriers can hold an Ed25519 key:
///   - the OpenSSH container ("openssh-key-v1"): its unencrypted header always exposes the public
///     key type, so this works even for passphrase-protected keys;
///   - an unencrypted PKCS#8 file ("-----BEGIN PRIVATE KEY-----", e.g. `openssl genpkey -algorithm
///     ED25519`): detected via the id-Ed25519 OID in its AlgorithmIdentifier;
///   - an encrypted PKCS#8 file ("-----BEGIN ENCRYPTED PRIVATE KEY-----", e.g. `openssl genpkey
///     -algorithm ED25519 -aes-256-cbc`): its OID is inside the encrypted body, so when the
///     passphrase is known it is decrypted (PBKDF2 + AES, FIPS-approved) just to read the OID.
/// Anything else (legacy PEM RSA/EC/DSA, or an encrypted PKCS#8 with an unknown passphrase) is
/// reported as UNKNOWN, which callers treat as "not Ed25519, safe to import".
enum ssh_keytypes_e detectPrivateKeyType(const String & filename, const String & passphrase)
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

    /// Unencrypted PKCS#8 carrier: "-----BEGIN PRIVATE KEY-----" (the OID is visible in the body).
    String pkcs8_der;
    if (decodePEMBody(contents, "PRIVATE KEY", pkcs8_der) && derContainsEd25519OID(pkcs8_der))
        return SSH_KEYTYPE_ED25519;

    /// Encrypted PKCS#8 carrier: "-----BEGIN ENCRYPTED PRIVATE KEY-----". The OID is hidden inside
    /// the encrypted body, so decrypt with the passphrase to read it (does not construct the key).
    if (contents.find("-----BEGIN ENCRYPTED PRIVATE KEY-----") != String::npos
        && encryptedPKCS8IsEd25519(contents, passphrase))
        return SSH_KEYTYPE_ED25519;

    /// OpenSSH container.
    String blob;
    if (!decodePEMBody(contents, "OPENSSH PRIVATE KEY", blob))
        return SSH_KEYTYPE_UNKNOWN;

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

/// Determine the key type of a public key file without importing it into libssh. Two carriers:
///   - OpenSSH one-line format ("<type> <base64> [comment]"): key type is the first token;
///   - X.509 SubjectPublicKeyInfo PEM ("-----BEGIN PUBLIC KEY-----", e.g. `openssl pkey -pubout`):
///     detected via the id-Ed25519 OID in its AlgorithmIdentifier.
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

    /// SPKI carrier: "-----BEGIN PUBLIC KEY-----".
    String spki_der;
    if (decodePEMBody(contents, "PUBLIC KEY", spki_der) && derContainsEd25519OID(spki_der))
        return SSH_KEYTYPE_ED25519;

    /// OpenSSH one-line public key: key type is the first whitespace-delimited token.
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
    /// The passphrase lets detection decrypt an encrypted PKCS#8 carrier to read its algorithm OID.
    if (OpenSSLInitializer::instance().isFIPSEnabled() && !isKeyTypeUsableInFIPSBuilds(detectPrivateKeyType(filename, passphrase)))
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

void SSHKeyFactory::validatePublicKeyFormat(const String & base64_key, const String & type_name)
{
    /// Format-only validation that does NOT import the key into libssh (importing an Ed25519 key
    /// under FIPS crashes). This keeps a definition's validity independent of the node's FIPS mode:
    /// a key that is FIPS-unusable (Ed25519) but must be preserved/skipped is still rejected here if
    /// it is malformed, matching what makePublicKeyFromBase64 does for FIPS-usable keys.
    /// An SSH public key wire blob starts with a length-prefixed string naming the algorithm.
    String blob;
    try
    {
        blob = base64Decode(base64_key);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: rethrown below as a typed exception; base64Decode's own message is not user-facing here.
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Bad SSH public key of type {}: not valid base64", type_name);
    }

    std::string_view data(blob);
    std::string_view embedded_type;
    if (!readSSHWireString(data, embedded_type))
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Bad SSH public key of type {}: truncated wire format", type_name);

    if (std::string_view(type_name) != embedded_type)
        throw Exception(ErrorCodes::LIBSSH_ERROR,
            "SSH public key type mismatch: declared '{}' but the key encodes '{}'", type_name, String(embedded_type));

    /// SSH certificate carriers (ssh-ed25519-cert-v01, sk-ssh-ed25519-cert-v01) are NOT a flat sequence of
    /// length-prefixed strings: they interleave fixed-width uint64/uint32 fields (serial, cert type) between
    /// the strings (OpenSSH PROTOCOL.certkeys). The generic string-sequence parse below would misread those
    /// integers as string length prefixes, so a genuine certificate would be rejected here while a minimal
    /// bogus blob (type string + one short string) would slip through the lenient "unknown type" branch. That
    /// makes a stored cert's validity depend on the node's FIPS mode. Validate the certificate structure
    /// explicitly. Only the Ed25519 cert families reach this validator (they are the FIPS-unusable set).
    {
        std::string_view t(type_name);
        bool is_ed25519_cert = (t == "ssh-ed25519-cert-v01@openssh.com");
        bool is_sk_ed25519_cert = (t == "sk-ssh-ed25519-cert-v01@openssh.com");
        if (is_ed25519_cert || is_sk_ed25519_cert)
        {
            auto fail = [&]
            {
                throw Exception(ErrorCodes::LIBSSH_ERROR,
                    "Bad SSH public key of type {}: invalid certificate wire format", type_name);
            };
            auto read_string = [&](std::string_view & out)
            {
                if (!readSSHWireString(data, out))
                    fail();
            };
            auto skip_bytes = [&](size_t n)
            {
                if (data.size() < n)
                    fail();
                data.remove_prefix(n);
            };
            /// A nested blob (the signature key and the signature) is itself a sequence of wire strings whose
            /// first string names an algorithm: the signature key is a public-key blob (type + key fields) and
            /// the signature is type + signature bytes. Only checking that these outer fields exist (as this
            /// validator did before) let a certificate with a junk signing key / signature pass while
            /// ssh-keygen -Lf rejects it. Require the nested blob to fully decompose into wire strings with a
            /// non-empty leading type and at least one more non-empty field, so malformed carriers are rejected.
            auto require_nested_blob = [&](std::string_view nested)
            {
                std::string_view nested_type;
                if (!readSSHWireString(nested, nested_type) || nested_type.empty())
                    fail();
                bool saw_nonempty_field = false;
                while (!nested.empty())
                {
                    std::string_view nested_field;
                    if (!readSSHWireString(nested, nested_field))
                        fail();
                    if (!nested_field.empty())
                        saw_nonempty_field = true;
                }
                if (!saw_nonempty_field)
                    fail();
            };
            std::string_view field;
            read_string(field);                 /// nonce
            read_string(field);                 /// public key
            if (field.size() != 32)             /// raw Ed25519 public key is exactly 32 bytes
                fail();
            if (is_sk_ed25519_cert)
                read_string(field);             /// application (security-key variant only)
            skip_bytes(8);                       /// serial (uint64)
            skip_bytes(4);                       /// cert type (uint32)
            read_string(field);                 /// key id
            read_string(field);                 /// valid principals
            skip_bytes(8);                       /// valid after (uint64)
            skip_bytes(8);                       /// valid before (uint64)
            read_string(field);                 /// critical options
            read_string(field);                 /// extensions
            read_string(field);                 /// reserved
            read_string(field);                 /// signature key
            require_nested_blob(field);          /// reject a junk signing key
            read_string(field);                 /// signature
            require_nested_blob(field);          /// reject a junk signature
            if (!data.empty())                   /// no trailing bytes allowed
                fail();
            return;
        }
    }

    /// Validate the full wire structure for the declared type, not only the leading type string. Otherwise a
    /// blob that carries just the length-prefixed type name (e.g. base64("\0\0\0\x0bssh-ed25519")) with no key
    /// body would pass here while makePublicKeyFromBase64 rejects it on a non-FIPS node, making validity depend
    /// on FIPS mode. An SSH public key is the type string followed by a fixed sequence of length-prefixed fields
    /// (RFC 4253/4716, RFC 5656, OpenSSH PROTOCOL.u2f); parse them all and require no trailing bytes.
    std::vector<size_t> field_sizes;
    while (!data.empty())
    {
        std::string_view field;
        if (!readSSHWireString(data, field))
            throw Exception(ErrorCodes::LIBSSH_ERROR,
                "Bad SSH public key of type {}: malformed or truncated wire format", type_name);
        field_sizes.push_back(field.size());
    }

    auto require = [&](bool ok)
    {
        if (!ok)
            throw Exception(ErrorCodes::LIBSSH_ERROR, "Bad SSH public key of type {}: invalid wire format", type_name);
    };

    std::string_view t(type_name);
    /// Ed25519 (and its security-key variant) is the only family that reaches this validator on the
    /// FIPS-preserve/skip paths, but validate every common type so the check is complete and type-agnostic.
    if (t == "ssh-ed25519")
        /// string type, string A (32-byte public key).
        require(field_sizes.size() == 1 && field_sizes[0] == 32);
    else if (t == "sk-ssh-ed25519@openssh.com")
        /// string type, string A (32-byte public key), string application.
        require(field_sizes.size() == 2 && field_sizes[0] == 32);
    else if (t == "ssh-rsa")
        /// string type, mpint e, mpint n.
        require(field_sizes.size() == 2 && field_sizes[0] > 0 && field_sizes[1] > 0);
    else if (t == "ssh-dss")
        /// string type, mpint p, q, g, y.
        require(field_sizes.size() == 4);
    else if (t == "ecdsa-sha2-nistp256" || t == "ecdsa-sha2-nistp384" || t == "ecdsa-sha2-nistp521")
        /// string type, string curve identifier, string Q (EC point).
        require(field_sizes.size() == 2 && field_sizes[0] > 0 && field_sizes[1] > 0);
    else if (t == "sk-ecdsa-sha2-nistp256@openssh.com")
        /// string type, string curve identifier, string Q, string application.
        require(field_sizes.size() == 3 && field_sizes[0] > 0 && field_sizes[1] > 0);
    else
        /// Unknown/other type: require at least one non-empty field beyond the type string.
        require(!field_sizes.empty() && field_sizes[0] > 0);
}

bool SSHKeyFactory::isPrivateKeyFileUsableInFIPSBuilds(const String & filename, const String & passphrase)
{
    return isKeyTypeUsableInFIPSBuilds(detectPrivateKeyType(filename, passphrase));
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
