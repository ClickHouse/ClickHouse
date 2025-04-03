#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <base/types.h>

struct ssh_key_struct;

namespace ssh
{

class SSHPublicKey
{
private:
    class KeyHasher
    {
    public:
        std::size_t operator()(const SSHPublicKey & input_key) const;

    private:
        std::hash<std::string> string_hasher;
    };

public:
    using KeyPtr = ssh_key_struct *;
    using KeySet = std::unordered_set<SSHPublicKey, KeyHasher>;

    SSHPublicKey() = delete;
    ~SSHPublicKey();

    SSHPublicKey(const SSHPublicKey &);
    SSHPublicKey & operator=(const SSHPublicKey &);

    SSHPublicKey(SSHPublicKey &&) noexcept;
    SSHPublicKey & operator=(SSHPublicKey &&) noexcept;

    bool operator==(const SSHPublicKey &) const;

    bool isEqual(const SSHPublicKey & other) const;

    String getBase64Representation() const;

    String getType() const;

    static SSHPublicKey createFromBase64(const String & base64, const String & key_type);

    static SSHPublicKey createFromFile(const String & filename);

    // Creates SSHPublicKey, but without owning the memory of ssh_key.
    // A user must manage it by himself. (This is implemented for compatibility with libssh callbacks)
    static SSHPublicKey createNonOwning(KeyPtr key);

private:
    explicit SSHPublicKey(KeyPtr key, bool own = true);

    static void deleter(KeyPtr key);

    // We may want to not own ssh_key memory, so then we pass this deleter to unique_ptr
    static void disabledDeleter(KeyPtr) { }

    using UniqueKeyPtr = std::unique_ptr<ssh_key_struct, decltype(&deleter)>;
    UniqueKeyPtr key;
};

}
