#include <Access/EncryptedPassword.h>
#include <Common/Exception.h>
#include <Core/Defines.h>
#include <boost/algorithm/hex.hpp>
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


EncryptedPassword & EncryptedPassword::setPassword(Type type_, const String & password_)
{
    switch (type_)
    {
        case NONE: return setHash(NONE, "");
        case PLAINTEXT: return setHash(PLAINTEXT, password_);
        case SHA256: return setHash(SHA256, encodeSHA256(password_));
        default: __builtin_unreachable();
    }
}


EncryptedPassword & EncryptedPassword::setHash(Type type_, const String & hash_)
{
    type = type_;
    hash = hash_;
    return *this;
}


EncryptedPassword & EncryptedPassword::setHashHex(Type type_, const String & hash_hex_)
{
    return setHash(type_, boost::algorithm::unhex(hash_hex_));
}


String EncryptedPassword::getHashHex() const
{
    return boost::algorithm::hex(hash);
}

bool EncryptedPassword::isCorrect(const String & password) const
{
    switch (type)
    {
        case NONE: return true;
        case PLAINTEXT: return password == hash;
        case SHA256: return encodeSHA256(password) == hash;
    }
    __builtin_unreachable();
}


void EncryptedPassword::checkIsCorrect(const String & password, const String & user_name) const
{
    if (isCorrect(password))
        return;
    auto user_name_with_colon = [&user_name]() { return user_name.empty() ? String() : user_name + ": "; };
    if (password.empty() && (type != NONE))
        throw Exception(user_name_with_colon() + "Password is required", ErrorCodes::REQUIRED_PASSWORD);
    throw Exception(user_name_with_colon() + "Wrong password", ErrorCodes::WRONG_PASSWORD);
}


bool operator ==(const EncryptedPassword & lhs, const EncryptedPassword & rhs)
{
    return (lhs.type == rhs.type) && (lhs.hash == rhs.hash);
}
}

