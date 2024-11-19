#include <stdexcept>
#include <Access/SSH/SSHPublicKey.h>
#include <Common/Exception.h>
#include <Common/clibssh.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SSH_EXCEPTION;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

}

namespace ssh
{

SSHPublicKey::SSHPublicKey(KeyPtr key_, bool own) : key(key_, own ? &deleter : &disabledDeleter)
{ // disable deleter if class is constructed without ownership
    if (!key)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No ssh_key provided in explicit constructor");
    }
}

SSHPublicKey::~SSHPublicKey() = default;

SSHPublicKey::SSHPublicKey(const SSHPublicKey & other) : key(ssh_key_dup(other.key.get()), &deleter)
{
    if (!key)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to duplicate ssh_key");
    }
}

SSHPublicKey & SSHPublicKey::operator=(const SSHPublicKey & other)
{
    if (this != &other)
    {
        KeyPtr new_key = ssh_key_dup(other.key.get());
        if (!new_key)
        {
            throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to duplicate ssh_key");
        }
        key = UniqueKeyPtr(new_key, deleter); // We don't have access to the pointer from external code, opposed to non owning key object.
                                              // So here we always go for default deleter, regardless of other's
    }
    return *this;
}

SSHPublicKey::SSHPublicKey(SSHPublicKey && other) noexcept = default;

SSHPublicKey & SSHPublicKey::operator=(SSHPublicKey && other) noexcept = default;

bool SSHPublicKey::operator==(const SSHPublicKey & other) const
{
    return isEqual(other);
}

bool SSHPublicKey::isEqual(const SSHPublicKey & other) const
{
    int rc = ssh_key_cmp(key.get(), other.key.get(), SSH_KEY_CMP_PUBLIC);
    return rc == 0;
}

SSHPublicKey SSHPublicKey::createFromBase64(const String & base64, const String & key_type)
{
    KeyPtr key;
    int rc = ssh_pki_import_pubkey_base64(base64.c_str(), ssh_key_type_from_name(key_type.c_str()), &key);
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed importing public key from base64 format.\n\
                Key: {}\n\
                Type: {}",
                base64, key_type
        );
    }
    return SSHPublicKey(key);
}

SSHPublicKey SSHPublicKey::createFromFile(const std::string & filename)
{
    KeyPtr key;
    int rc = ssh_pki_import_pubkey_file(filename.c_str(), &key);
    if (rc != SSH_OK)
    {
        if (rc == SSH_EOF)
        {
            throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Can't import ssh public key from file {} as it doesn't exist or permission denied", filename
                    );
        }
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Can't import ssh public key from file {}", filename);
    }
    return SSHPublicKey(key);
}

SSHPublicKey SSHPublicKey::createNonOwning(KeyPtr key)
{
    return SSHPublicKey(key, false);
}

namespace
{

    struct CStringDeleter
    {
        [[maybe_unused]] void operator()(char * ptr) const { std::free(ptr); }
    };

}

String SSHPublicKey::getBase64Representation() const
{
    char * buf = nullptr;
    int rc = ssh_pki_export_pubkey_base64(key.get(), &buf);

    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to export public key to base64");
    }

    // Create a String from cstring, which makes a copy of the first one and requires freeing memory after it
    std::unique_ptr<char, CStringDeleter> buf_ptr(buf); // This is to safely manage buf memory
    return String(buf_ptr.get());
}

String SSHPublicKey::getType() const
{
    const char * type_c = ssh_key_type_to_char(ssh_key_type(key.get()));
    if (type_c == nullptr)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Key type is unknown or no key contained");
    }
    return String(type_c);
}

std::size_t SSHPublicKey::KeyHasher::operator()(const SSHPublicKey & input_key) const
{
    String combined_string(input_key.getType());
    combined_string += input_key.getBase64Representation();
    return string_hasher(combined_string);
}

void SSHPublicKey::deleter(KeyPtr key)
{
    ssh_key_free(key);
}

}
