#include <ACL/EncodedPassword.h>
#include <Common/Exception.h>
#include <Core/Defines.h>
#include "config_core.h"
#if USE_SSL
#   include <openssl/sha.h>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int REQUIRED_PASSWORD;
    extern const int WRONG_PASSWORD;
}


namespace
{
    String encodeSHA256(const String & text)
    {
#if USE_SSL
        unsigned char hash[32];
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(text.data()), text.size());
        SHA256_Final(hash, &ctx);
        return String(reinterpret_cast<char *>(hash), 32);
#else
        UNUSED(text);
        throw DB::Exception("SHA256 passwords support is disabled, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
}


EncodedPassword::EncodedPassword() = default;
EncodedPassword::EncodedPassword(const EncodedPassword & src) = default;
EncodedPassword & EncodedPassword::operator =(const EncodedPassword & src) = default;


void EncodedPassword::setPassword(const String & password, Encoding encoding_)
{
    switch (encoding_)
    {
        case Encoding::PLAIN_TEXT: setEncodedPassword(password, encoding_); break;
        case Encoding::SHA256: setEncodedPassword(encodeSHA256(password), encoding_); break;
    }
    __builtin_unreachable();
}


void EncodedPassword::setNoPassword()
{
    setEncodedPassword(String(), Encoding::PLAIN_TEXT); }


void EncodedPassword::setEncodedPassword(const String & encoded_password_, Encoding encoding_)
{
    encoded_password = encoded_password_;
    encoding = encoding_;
}


bool EncodedPassword::isCorrect(const String & password) const
{
    switch (encoding)
    {
        case Encoding::PLAIN_TEXT: return encoded_password.empty() || (encoded_password == password);
        case Encoding::SHA256: return (encoded_password == encodeSHA256(password));
    }
    __builtin_unreachable();
}


void EncodedPassword::checkIsCorrect(const String & password, const String & user_name) const
{
    if (isCorrect(password))
        return;
    auto user_name_with_colon = [&user_name]() { return user_name.empty() ? String() : user_name + ": "; };
    if (password.empty())
        throw Exception(user_name_with_colon() + "Password is required", ErrorCodes::REQUIRED_PASSWORD);
    throw Exception(user_name_with_colon() + "Wrong password", ErrorCodes::WRONG_PASSWORD);
}


bool operator ==(const EncodedPassword & lhs, const EncodedPassword & rhs)
{
    return (lhs.encoding == rhs.encoding) && (lhs.encoded_password == rhs.encoded_password);
}
}

