#pragma once

#include <Core/Types.h>


namespace DB
{
/// Encrypted password for checking when an user logins.
class EncryptedPassword
{
public:
    enum Type
    {
        NONE,
        PLAINTEXT,
        SHA256,
    };

    EncryptedPassword() = default;
    EncryptedPassword(const EncryptedPassword & src) = default;
    EncryptedPassword & operator =(const EncryptedPassword & src) = default;

    EncryptedPassword & clear() { return setPassword(Type::NONE, ""); }
    EncryptedPassword & setPassword(Type type_, const String & password_);
    EncryptedPassword & setHash(Type type_, const String & hash_);
    EncryptedPassword & setHashHex(Type type_, const String & hash_hex_);

    Type getType() const { return type; }
    const String & getHash() const { return hash; }
    String getHashHex() const;

    /// Checks if the provided password is correct. Returns false if not.
    bool isCorrect(const String & password) const;

    /// Checks if the provided password is correct. Throws an exception if not.
    /// `username` is only used for generating an error message if the password is incorrect.
    void checkIsCorrect(const String & password, const String & user_name = String()) const;

    friend bool operator ==(const EncryptedPassword & lhs, const EncryptedPassword & rhs);
    friend bool operator !=(const EncryptedPassword & lhs, const EncryptedPassword & rhs) { return !(lhs == rhs); }

private:
    Type type = Type::NONE;
    String hash;
};
}
