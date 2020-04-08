#pragma once

#include <Core/Types.h>


namespace DB
{
/// Authentication type and encrypted password for checking when an user logins.
class Authentication
{
public:
    enum Type
    {
        /// User doesn't have to enter password.
        NO_PASSWORD,

        /// Password is stored as is.
        PLAINTEXT_PASSWORD,

        /// Password is encrypted in SHA256 hash.
        SHA256_PASSWORD,

        /// SHA1(SHA1(password)).
        /// This kind of hash is used by the `mysql_native_password` authentication plugin.
        DOUBLE_SHA1_PASSWORD,
    };

    using Digest = std::vector<UInt8>;

    Authentication(Authentication::Type type = NO_PASSWORD);
    Authentication(const Authentication & src) = default;
    Authentication & operator =(const Authentication & src) = default;
    Authentication(Authentication && src) = default;
    Authentication & operator =(Authentication && src) = default;

    Type getType() const { return type; }

    /// Sets the password and encrypt it using the authentication type set in the constructor.
    void setPassword(const String & password);

    /// Returns the password. Allowed to use only for Type::PLAINTEXT_PASSWORD.
    String getPassword() const;

    /// Sets the password as a string of hexadecimal digits.
    void setPasswordHashHex(const String & hash);
    String getPasswordHashHex() const;

    /// Sets the password in binary form.
    void setPasswordHashBinary(const Digest & hash);
    const Digest & getPasswordHashBinary() const { return password_hash; }

    /// Returns SHA1(SHA1(password)) used by MySQL compatibility server for authentication.
    /// Allowed to use for Type::NO_PASSWORD, Type::PLAINTEXT_PASSWORD, Type::DOUBLE_SHA1_PASSWORD.
    Digest getPasswordDoubleSHA1() const;

    /// Checks if the provided password is correct. Returns false if not.
    bool isCorrectPassword(const String & password) const;

    /// Checks if the provided password is correct. Throws an exception if not.
    /// `user_name` is only used for generating an error message if the password is incorrect.
    void checkPassword(const String & password, const String & user_name = String()) const;

    friend bool operator ==(const Authentication & lhs, const Authentication & rhs);
    friend bool operator !=(const Authentication & lhs, const Authentication & rhs) { return !(lhs == rhs); }

private:
    Type type = Type::NO_PASSWORD;
    Digest password_hash;
};
}
