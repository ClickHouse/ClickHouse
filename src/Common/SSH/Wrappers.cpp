#include <Common/SSH/Wrappers.h>
#if USE_SSL
#    include <stdexcept>

#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wreserved-macro-identifier"
#    pragma GCC diagnostic ignored "-Wreserved-identifier"

#    include <libssh/libssh.h>

#    pragma GCC diagnostic pop

namespace
{
class SshString
{
public:
    explicit SshString(std::string_view input)
    {
        string = ssh_string_new(input.size());
        ssh_string_fill(string, input.data(), input.size());
    }

    explicit SshString(ssh_string c_other) { string = c_other; }

    ssh_string get() { return string; }

    String toString() { return String(ssh_string_to_char(string), ssh_string_len(string)); }

    ~SshString() { ssh_string_free(string); }

private:
    ssh_string string;
};

}


namespace ssh
{

SshKey SshKeyFactory::makePrivateFromFile(String filename, String passphrase)
{
    ssh_key key;
    int rc = ssh_pki_import_privkey_file(filename.c_str(), passphrase.c_str(), nullptr, nullptr, &key);
    if (rc != SSH_OK)
    {
        throw std::runtime_error("Can't import ssh private key from file");
    }
    return SshKey(key);
}


SshKey SshKeyFactory::makePublicFromFile(String filename)
{
    ssh_key key;
    int rc = ssh_pki_import_pubkey_file(filename.c_str(), &key);
    if (rc != SSH_OK)
    {
        throw std::runtime_error("Can't import ssh public key from file");
    }
    return SshKey(key);
}


SshKey SshKeyFactory::makePublicFromBase64(String base64_key, String type_name)
{
    ssh_key key;
    auto key_type = ssh_key_type_from_name(type_name.c_str());
    int rc = ssh_pki_import_pubkey_base64(base64_key.c_str(), key_type, &key);
    if (rc != SSH_OK)
    {
        throw std::invalid_argument("Bad ssh public key provided");
    }
    return SshKey(key);
}


SshKey::SshKey(const SshKey & other)
{
    key = ssh_key_dup(other.key);
}


SshKey & SshKey::operator=(const SshKey & other)
{
    ssh_key_free(key);
    key = ssh_key_dup(other.key);
    return *this;
}


SshKey & SshKey::operator=(SshKey && other) noexcept
{
    ssh_key_free(key);
    key = other.key;
    other.key = nullptr;
    return *this;
}


String SshKey::signString(std::string_view input) const
{
    SshString input_str(input);
    ssh_string c_output = nullptr;
    int rc = pki_sign_string(key, input_str.get(), &c_output);
    if (rc != SSH_OK)
    {
        throw std::runtime_error("Error singing with ssh key");
    }
    SshString output(c_output);
    return output.toString();
}


bool SshKey::verifySignature(std::string_view signature, std::string_view original) const
{
    SshString sig(signature), orig(original);
    int rc = pki_verify_string(key, sig.get(), orig.get());
    return rc == SSH_OK;
}


bool SshKey::isPrivate() const
{
    return ssh_key_is_private(key);
}

bool SshKey::isPublic() const
{
    return ssh_key_is_public(key);
}

SshKey::~SshKey()
{
    ssh_key_free(key); // it's safe free from libssh
}

}

#endif
