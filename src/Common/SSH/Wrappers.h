#pragma once
#include <stdexcept>
#include <string_view>
#include <base/types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-macro-identifier"
#pragma GCC diagnostic ignored "-Wreserved-identifier"

#include <libssh/libssh.h>

#pragma GCC diagnostic pop

namespace ssh
{

class SshString
{
public:
    SshString(std::string_view input)
    {
        string_ = ssh_string_new(input.size());
        ssh_string_fill(string_, input.data(), input.size());
    }

    SshString(ssh_string c_other) { string_ = c_other; }

    ssh_string get() { return string_; }

    String toString() { return String(ssh_string_to_char(string_), ssh_string_len(string_)); }

    ~SshString() { ssh_string_free(string_); }

private:
    ssh_string string_;
};

class SshKey
{
public:
    SshKey() = default;

    SshKey(String priv_key_filename, String passphrase)
    {
        int rc = ssh_pki_import_privkey_file(priv_key_filename.c_str(), passphrase.c_str(), nullptr, nullptr, &key_);
        if (rc != SSH_OK)
        {
            throw std::runtime_error("Can't import ssh private key from file");
        }
    }

    SshKey(String pub_key_filename)
    {
        int rc = ssh_pki_import_pubkey_file(pub_key_filename.c_str(), &key_);
        if (rc != SSH_OK)
        {
            throw std::runtime_error("Can't import ssh public key from file");
        }
    }

    SshKey(String base64_key, enum ssh_keytypes_e key_type)
    {
        int rc = ssh_pki_import_pubkey_base64(base64_key.c_str(), key_type, &key_);
        if (rc != SSH_OK)
        {
            throw std::invalid_argument("Bad ssh public key provided");
        }
    }

    bool isEmpty() { return key_ == nullptr; }

    static enum ssh_keytypes_e typeFromName(String name) { return ssh_key_type_from_name(name.c_str()); }

    SshKey(const SshKey & other) { key_ = ssh_key_dup(other.key_); }

    SshKey & operator=(const SshKey & other)
    {
        ssh_key_free(key_);
        key_ = ssh_key_dup(other.key_);
        return *this;
    }

    SshKey(SshKey && other) noexcept
    {
        key_ = other.key_;
        other.key_ = nullptr;
    }

    SshKey & operator=(SshKey && other)
    {
        ssh_key_free(key_);
        key_ = other.key_;
        other.key_ = nullptr;
        return *this;
    }

    String signString(std::string_view input) const
    {
        SshString input_str(input);
        ssh_string c_output = nullptr;
        int rc = pki_sign_string(key_, input_str.get(), &c_output);
        if (rc != SSH_OK)
        {
            throw std::runtime_error("Error singing with ssh key");
        }
        SshString output(c_output);
        return output.toString();
    }

    bool verifySignature(std::string_view signature, std::string_view original) const
    {
        SshString sig(signature), orig(original);
        int rc = pki_verify_string(key_, sig.get(), orig.get());
        if (rc != SSH_OK)
        {
            return false;
        }
        return true;
    }

    bool isPublic() const { return ssh_key_is_public(key_); }

    bool isPrivate() const { return ssh_key_is_private(key_); }

    enum ssh_keytypes_e type() { return ssh_key_type(key_); }

    ssh_key get() { return key_; }

    ~SshKey() { ssh_key_free(key_); } // it's safe free from libssh

private:
    ssh_key key_ = nullptr;
};

}
