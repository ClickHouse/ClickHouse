#pragma once

#include <Core/Types.h>


namespace DB
{
/// Encoded password for checking when an user logins.
class EncodedPassword
{
public:
    EncodedPassword();
    EncodedPassword(const EncodedPassword & src);
    EncodedPassword & operator =(const EncodedPassword & src);

    enum class Encoding
    {
        PLAIN_TEXT,
        SHA256,
    };

    void setPassword(const String & password, Encoding encoding_);
    void setNoPassword();
    void setEncodedPassword(const String & encoded_password_, Encoding encoding_);
    void setEncodedPasswordHex(const String & encoded_password_hex_, Encoding encoding_);
    Encoding getEncoding() const { return encoding; }
    const String & getEncodedPassword() const { return encoded_password; }
    String getEncodedPasswordHex() const;

    /// Checks if the provided password is correct. Returns false if not.
    bool isCorrect(const String & password) const;

    /// Checks if the provided password is correct. Throws an exception if not.
    /// `username` is only used for generating an error message if the password is incorrect.
    void checkIsCorrect(const String & password, const String & user_name = String()) const;

    friend bool operator ==(const EncodedPassword & lhs, const EncodedPassword & rhs);
    friend bool operator !=(const EncodedPassword & lhs, const EncodedPassword & rhs) { return !(lhs == rhs); }

private:
    Encoding encoding = Encoding::PLAIN_TEXT;
    String encoded_password;
};
}
