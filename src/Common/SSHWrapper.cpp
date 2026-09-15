#include <Common/SSHWrapper.h>

# if USE_SSH
#    include <stdexcept>

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
}

SSHKey SSHKeyFactory::makePrivateKeyFromFile(String filename, String passphrase)
{
    ssh_key key;
    if (int rc = ssh_pki_import_privkey_file(filename.c_str(), passphrase.c_str(), nullptr, nullptr, &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't import SSH private key from file");
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicKeyFromFile(String filename)
{
    ssh_key key;
    if (int rc = ssh_pki_import_pubkey_file(filename.c_str(), &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't import SSH public key from file");
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicKeyFromBase64(String base64_key, String type_name)
{
    ssh_key key;
    auto key_type = ssh_key_type_from_name(type_name.c_str());
    if (int rc = ssh_pki_import_pubkey_base64(base64_key.c_str(), key_type, &key); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Bad SSH public key provided");
    return SSHKey(key);
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
