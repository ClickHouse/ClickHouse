#include <Access/Authentication.h>
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
    extern const int LOGICAL_ERROR;
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
        return boost::algorithm::hex(String(reinterpret_cast<char *>(hash), 32));
#else
        UNUSED(text);
        throw DB::Exception("SHA256 passwords support is disabled, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
}


void Authentication::setType(Type type_)
{
    type = type_;
    password_hash.clear();
}

void Authentication::setPassword(const String & password_)
{
    switch (type)
    {
        case NO_PASSWORD:
            throw Exception("Cannot specify password for the 'NO_PASSWORD' authentication type", ErrorCodes::LOGICAL_ERROR);
        case PLAINTEXT_PASSWORD:
            setPasswordHash(password_);
            break;
        case SHA256_PASSWORD:
            setPasswordHash(encodeSHA256(password_));
            break;
        default:
            throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
    }
}


void Authentication::setPasswordHash(const String & hash)
{
    password_hash = hash;
}


bool Authentication::isCorrect(const String & password_) const
{
    switch (type)
    {
        case NO_PASSWORD: return true;
        case PLAINTEXT_PASSWORD: return password_ == password_hash;
        case SHA256_PASSWORD: return encodeSHA256(password_) == password_hash;
        default: throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);

    }
}


void Authentication::checkIsCorrect(const String & password_, const String & user_name) const
{
    if (isCorrect(password_))
        return;
    auto user_name_with_colon = [&user_name]() { return user_name.empty() ? String() : user_name + ": "; };
    if (password_.empty() && (type != NO_PASSWORD))
        throw Exception(user_name_with_colon() + "Password is required", ErrorCodes::REQUIRED_PASSWORD);
    throw Exception(user_name_with_colon() + "Wrong password", ErrorCodes::WRONG_PASSWORD);
}


bool operator ==(const Authentication & lhs, const Authentication & rhs)
{
    return (lhs.type == rhs.type) && (lhs.password_hash == rhs.password_hash);
}
}

