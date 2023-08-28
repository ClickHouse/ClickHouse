#include <Common/SSH/Wrappers.h>
# if USE_SSL
#    include <stdexcept>

#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wreserved-macro-identifier"
#    pragma GCC diagnostic ignored "-Wreserved-identifier"

#    include <libssh/libssh.h>

#    pragma GCC diagnostic pop

namespace
{

class SSHString
{
public:
    explicit SSHString(std::string_view input)
    {
        string = ssh_string_new(input.size());
        ssh_string_fill(string, input.data(), input.size());
    }

    explicit SSHString(ssh_string c_other) { string = c_other; }

    ssh_string get() { return string; }

    String toString()
    {
        return String(ssh_string_get_char(string), ssh_string_len(string));
    }

    ~SSHString()
    {
        ssh_string_free(string);
    }

private:
    ssh_string string;
};

}

namespace ssh
{

SSHKey SSHKeyFactory::makePrivateFromFile(String filename, String passphrase)
{
    ssh_key key;
    int rc = ssh_pki_import_privkey_file(filename.c_str(), passphrase.c_str(), nullptr, nullptr, &key);
    if (rc != SSH_OK)
    {
        throw std::runtime_error("Can't import ssh private key from file");
    }
    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicFromFile(String filename)
{
    ssh_key key;
    int rc = ssh_pki_import_pubkey_file(filename.c_str(), &key);
    if (rc != SSH_OK)
        throw std::runtime_error("Can't import ssh public key from file");

    return SSHKey(key);
}

SSHKey SSHKeyFactory::makePublicFromBase64(String base64_key, String type_name)
{
    ssh_key key;
    auto key_type = ssh_key_type_from_name(type_name.c_str());
    int rc = ssh_pki_import_pubkey_base64(base64_key.c_str(), key_type, &key);
    if (rc != SSH_OK)
        throw std::invalid_argument("Bad ssh public key provided");

    return SSHKey(key);
}

SSHKey::SSHKey(const SSHKey & other)
{
    key = ssh_key_dup(other.key);
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

String SSHKey::signString(std::string_view input) const
{
    SSHString input_str(input);
    ssh_string c_output = nullptr;
    int rc = pki_sign_string(key, input_str.get(), &c_output);
    if (rc != SSH_OK)
        throw std::runtime_error("Error singing with ssh key");

    SSHString output(c_output);
    return output.toString();
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

SSHKey::~SSHKey()
{
    ssh_key_free(key); // it's safe free from libssh
}

}

#endif
