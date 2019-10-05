#pragma once

#include <Core/Types.h>


namespace DB
{
/// Encrypted password for checking when an user logins.
class Authentication
{
public:
    enum Type
    {
        NO_PASSWORD,
        PLAINTEXT_PASSWORD,
        SHA256_PASSWORD,
    };

    Authentication() = default;
    Authentication(const Authentication & src) = default;
    Authentication & operator =(const Authentication & src) = default;

    void setType(Type type_);
    Type getType() const { return type; }

    void setPassword(const String & password);
    void setPasswordHash(const String & hash);
    const String & getPasswordHash() const { return password_hash; }

    /// Checks if the provided password is correct. Returns false if not.
    bool isCorrect(const String & password) const;

    /// Checks if the provided password is correct. Throws an exception if not.
    /// `username` is only used for generating an error message if the password is incorrect.
    void checkIsCorrect(const String & password, const String & user_name = String()) const;

    friend bool operator ==(const Authentication & lhs, const Authentication & rhs);
    friend bool operator !=(const Authentication & lhs, const Authentication & rhs) { return !(lhs == rhs); }

private:
    Type type = Type::NO_PASSWORD;
    String password_hash;
};
}
