#pragma once
#include "config.h"
#if USE_SSL
#    include <string_view>
#    include <base/types.h>

using ssh_key = struct ssh_key_struct *;

namespace ssh
{

class SshKeyFactory;

class SshKey
{
public:
    SshKey() = default;

    bool isEmpty() { return key == nullptr; }

    SshKey(const SshKey & other);

    SshKey & operator=(const SshKey & other);

    SshKey(SshKey && other) noexcept
    {
        key = other.key;
        other.key = nullptr;
    }

    SshKey & operator=(SshKey && other) noexcept;

    String signString(std::string_view input) const;

    bool verifySignature(std::string_view signature, std::string_view original) const;

    bool isPublic() const;

    bool isPrivate() const;

    ~SshKey();

    friend SshKeyFactory;

private:
    explicit SshKey(ssh_key key_) : key(key_) { }
    ssh_key key = nullptr;
};


class SshKeyFactory
{
public:
    static SshKey makePrivateFromFile(String filename, String passphrase);

    static SshKey makePublicFromFile(String filename);

    static SshKey makePublicFromBase64(String base64_key, String type_name);
};


}


#else
namespace ssh
{
class SshKey
{
public:
    bool isEmpty() { return true; }
};
}
#endif
