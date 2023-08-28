#pragma once
#include "config.h"
#if USE_SSL
#    include <string_view>
#    include <base/types.h>

using ssh_key = struct ssh_key_struct *;

namespace ssh
{

class SSHKeyFactory;

class SSHKey
{
public:
    SSHKey() = default;
    ~SSHKey();

    SSHKey(const SSHKey & other);
    SSHKey(SSHKey && other) noexcept
    {
        key = other.key;
        other.key = nullptr;
    }
    SSHKey & operator=(const SSHKey & other);
    SSHKey & operator=(SSHKey && other) noexcept;

    bool isEmpty() { return key == nullptr; }
    String signString(std::string_view input) const;
    bool verifySignature(std::string_view signature, std::string_view original) const;

    bool isPublic() const;
    bool isPrivate() const;

    friend SSHKeyFactory;
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
    static SSHKey makePrivateFromFile(String filename, String passphrase);
    static SSHKey makePublicFromFile(String filename);
    static SSHKey makePublicFromBase64(String base64_key, String type_name);
};

}

#else
namespace ssh
{
class SSHKey
{
public:
    bool isEmpty() { return true; }
};
}
#endif
