#pragma once

#include <Common/Exception.h>

#include <string_view>
#include <base/types.h>

#include "config.h"

#if USE_SSH
using ssh_key = struct ssh_key_struct *;

namespace DB
{

class SSHKey
{
public:
    SSHKey() = default;
    ~SSHKey();

    SSHKey(const SSHKey & other);
    SSHKey(SSHKey && other) noexcept;
    SSHKey & operator=(const SSHKey & other);
    SSHKey & operator=(SSHKey && other) noexcept;

    bool operator==(const SSHKey &) const;
    bool isEqual(const SSHKey & other) const;

    bool isEmpty() { return key == nullptr; }
    String signString(std::string_view input) const;
    bool verifySignature(std::string_view signature, std::string_view original) const;

    bool isPublic() const;
    bool isPrivate() const;

    String getBase64() const;
    String getKeyType() const;

    friend class SSHKeyFactory;
private:
    explicit SSHKey(ssh_key key_) : key(key_) { }
    ssh_key key = nullptr;
};


class SSHKeyFactory
{
public:
    /// The check whether the path is allowed to read for ClickHouse has
    /// (e.g. a file is inside `user_files` directory)
    /// to be done outside of this functions.
    static SSHKey makePrivateKeyFromFile(String filename, String passphrase);
    static SSHKey makePublicKeyFromFile(String filename);
    static SSHKey makePublicKeyFromBase64(String base64_key, String type_name);
};

}

#else
class SSHKey
{
public:
    bool operator==(const SSHKey &) const = default;
    [[ noreturn ]] bool isEmpty() { std::terminate(); }
    [[ noreturn ]] String signString(std::string_view) const { std::terminate(); }
};
#endif
