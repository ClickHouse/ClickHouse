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
    SSHString input_str(input);
    ssh_string output = nullptr;
    if (int rc = pki_sign_string(key, input_str.get(), &output); rc != SSH_OK)
        throw Exception(ErrorCodes::LIBSSH_ERROR, "Error singing with ssh key");
    SSHString output_str(output);
    return output_str.toString();
}

bool SSHKey::verifySignature(std::string_view signature, std::string_view original) const
{
    SSHString sig(signature), orig(original);
    int rc = pki_verify_string(key, sig.get(), orig.get());
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

SSHKey::~SSHKey()
{
    ssh_key_free(key); // it's safe free from libssh
}

}

#endif
