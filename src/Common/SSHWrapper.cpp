#include <Common/SSHWrapper.h>

# if USE_SSH
#    include <stdexcept>

#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#    pragma clang diagnostic ignored "-Wreserved-identifier"

#    include <libssh/libssh.h>

#    pragma clang diagnostic pop

extern "C" {
    struct ssh_signature_struct;
    typedef struct ssh_signature_struct* ssh_signature;
    
    enum ssh_digest_e {
        SSH_DIGEST_AUTO = 0,
        SSH_DIGEST_SHA1 = 1,
        SSH_DIGEST_SHA256,
        SSH_DIGEST_SHA384,
        SSH_DIGEST_SHA512
    };
    
    enum ssh_digest_e ssh_key_type_to_hash(ssh_session session, enum ssh_keytypes_e type);
    
    ssh_signature pki_do_sign(const ssh_key privkey,
                              const unsigned char *input,
                              size_t input_len,
                              enum ssh_digest_e hash_type);
    
    int pki_verify_data_signature(ssh_signature sig,
                                   const ssh_key key,
                                   const unsigned char *input,
                                   size_t input_len);
    
    void ssh_signature_free(ssh_signature sign);
    
    int ssh_pki_export_signature_blob(const ssh_signature sign, ssh_string *sign_blob);
    
    int ssh_pki_import_signature_blob(const ssh_string sig_blob,
                                      const ssh_key pubkey,
                                      ssh_signature *psig);
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LIBSSH_ERROR;
}

namespace
{

class SSHString
{
public:
    explicit SSHString(std::string_view input)
    {
        if (string = ssh_string_new(input.size()); string == nullptr)
            throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't create SSHString");
        if (int rc = ssh_string_fill(string, input.data(), input.size()); rc != SSH_OK)
            throw Exception(ErrorCodes::LIBSSH_ERROR, "Can't create SSHString");
    }

    explicit SSHString(ssh_string other) { string = other; }

    ssh_string get() { return string; }

    String toString()
    {
        return {ssh_string_get_char(string), ssh_string_len(string)};
    }

    ~SSHString()
    {
        ssh_string_free(string);
    }

private:
    ssh_string string;
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
    enum ssh_keytypes_e key_type = ssh_key_type(key);
    enum ssh_digest_e hash_type = ssh_key_type_to_hash(nullptr, key_type);
    
    ssh_signature sig = pki_do_sign(
        key,
        reinterpret_cast<const unsigned char*>(input.data()),
        input.size(),
        hash_type
    );
    
    if (sig == nullptr)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Error signing with ssh key");
    
    ssh_string sig_blob = nullptr;
    int rc = ssh_pki_export_signature_blob(sig, &sig_blob);
    ssh_signature_free(sig);
    
    if (rc != SSH_OK || sig_blob == nullptr)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Error exporting signature blob");
    
    SSHString output_str(sig_blob);
    return output_str.toString();
}

bool SSHKey::verifySignature(std::string_view signature, std::string_view original) const
{
    SSHString sig_str(signature);
    
    ssh_signature sig = nullptr;
    int rc = ssh_pki_import_signature_blob(sig_str.get(), key, &sig);
    
    if (rc != SSH_OK || sig == nullptr)
        return false;
    
    rc = pki_verify_data_signature(
        sig,
        key,
        reinterpret_cast<const unsigned char*>(original.data()),
        original.size()
    );
    
    ssh_signature_free(sig);
    return rc == SSH_OK;
}

bool SSHKey::isPrivate() const
{
    return ssh_key_is_private(key);
}

bool SSHKey::isPublic() const
{
    return ssh_key_is_public(key);
}

namespace
{
    struct CStringDeleter
    {
        void operator()(char * ptr) const { std::free(ptr); }
    };
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
