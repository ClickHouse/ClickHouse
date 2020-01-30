#include <Access/Authentication.h>
#include <Common/Exception.h>
#include <common/StringRef.h>
#include <Core/Defines.h>
#include <Poco/SHA1Engine.h>
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
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


namespace
{
    using Digest = Authentication::Digest;

    Digest encodePlainText(const StringRef & text)
    {
        return Digest(text.data, text.data + text.size);
    }

    Digest encodeSHA256(const StringRef & text)
    {
#if USE_SSL
        Digest hash;
        hash.resize(32);
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const UInt8 *>(text.data), text.size);
        SHA256_Final(hash.data(), &ctx);
        return hash;
#else
        UNUSED(text);
        throw DB::Exception("SHA256 passwords support is disabled, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }

    Digest encodeSHA1(const StringRef & text)
    {
        Poco::SHA1Engine engine;
        engine.update(text.data, text.size);
        return engine.digest();
    }

    Digest encodeSHA1(const Digest & text)
    {
        return encodeSHA1(StringRef{reinterpret_cast<const char *>(text.data()), text.size()});
    }

    Digest encodeDoubleSHA1(const StringRef & text)
    {
        return encodeSHA1(encodeSHA1(text));
    }
}


Authentication::Authentication(Authentication::Type type_)
    : type(type_)
{
}


void Authentication::setPassword(const String & password_)
{
    switch (type)
    {
        case NO_PASSWORD:
            throw Exception("Cannot specify password for the 'NO_PASSWORD' authentication type", ErrorCodes::LOGICAL_ERROR);

        case PLAINTEXT_PASSWORD:
            setPasswordHashBinary(encodePlainText(password_));
            return;

        case SHA256_PASSWORD:
            setPasswordHashBinary(encodeSHA256(password_));
            return;

        case DOUBLE_SHA1_PASSWORD:
            setPasswordHashBinary(encodeDoubleSHA1(password_));
            return;
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}


String Authentication::getPassword() const
{
    if (type != PLAINTEXT_PASSWORD)
        throw Exception("Cannot decode the password", ErrorCodes::LOGICAL_ERROR);
    return String(password_hash.data(), password_hash.data() + password_hash.size());
}


void Authentication::setPasswordHashHex(const String & hash)
{
    Digest digest;
    digest.resize(hash.size() / 2);
    boost::algorithm::unhex(hash.begin(), hash.end(), digest.data());
    setPasswordHashBinary(digest);
}


String Authentication::getPasswordHashHex() const
{
    String hex;
    hex.resize(password_hash.size() * 2);
    boost::algorithm::hex(password_hash.begin(), password_hash.end(), hex.data());
    return hex;
}


void Authentication::setPasswordHashBinary(const Digest & hash)
{
    switch (type)
    {
        case NO_PASSWORD:
            throw Exception("Cannot specify password for the 'NO_PASSWORD' authentication type", ErrorCodes::LOGICAL_ERROR);

        case PLAINTEXT_PASSWORD:
        {
            password_hash = hash;
            return;
        }

        case SHA256_PASSWORD:
        {
            if (hash.size() != 32)
                throw Exception(
                    "Password hash for the 'SHA256_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 32 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }

        case DOUBLE_SHA1_PASSWORD:
        {
            if (hash.size() != 20)
                throw Exception(
                    "Password hash for the 'DOUBLE_SHA1_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 20 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}


Digest Authentication::getPasswordDoubleSHA1() const
{
    switch (type)
    {
        case NO_PASSWORD:
        {
            Poco::SHA1Engine engine;
            return engine.digest();
        }

        case PLAINTEXT_PASSWORD:
        {
            Poco::SHA1Engine engine;
            engine.update(getPassword());
            const Digest & first_sha1 = engine.digest();
            engine.update(first_sha1.data(), first_sha1.size());
            return engine.digest();
        }

        case SHA256_PASSWORD:
            throw Exception("Cannot get password double SHA1 for user with 'SHA256_PASSWORD' authentication.", ErrorCodes::BAD_ARGUMENTS);

        case DOUBLE_SHA1_PASSWORD:
            return password_hash;
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}


bool Authentication::isCorrectPassword(const String & password_) const
{
    switch (type)
    {
        case NO_PASSWORD:
            return true;

        case PLAINTEXT_PASSWORD:
        {
            if (password_ == StringRef{reinterpret_cast<const char *>(password_hash.data()), password_hash.size()})
                return true;

            // For compatibility with MySQL clients which support only native authentication plugin, SHA1 can be passed instead of password.
            auto password_sha1 = encodeSHA1(password_hash);
            return password_ == StringRef{reinterpret_cast<const char *>(password_sha1.data()), password_sha1.size()};
        }

        case SHA256_PASSWORD:
            return encodeSHA256(password_) == password_hash;

        case DOUBLE_SHA1_PASSWORD:
        {
            auto first_sha1 = encodeSHA1(password_);

            /// If it was MySQL compatibility server, then first_sha1 already contains double SHA1.
            if (first_sha1 == password_hash)
                return true;

            return encodeSHA1(first_sha1) == password_hash;
        }
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}


void Authentication::checkPassword(const String & password_, const String & user_name) const
{
    if (isCorrectPassword(password_))
        return;
    auto info_about_user_name = [&user_name]() { return user_name.empty() ? String() : " for user " + user_name; };
    if (password_.empty() && (type != NO_PASSWORD))
        throw Exception("Password required" + info_about_user_name(), ErrorCodes::REQUIRED_PASSWORD);
    throw Exception("Wrong password" + info_about_user_name(), ErrorCodes::WRONG_PASSWORD);
}


bool operator ==(const Authentication & lhs, const Authentication & rhs)
{
    return (lhs.type == rhs.type) && (lhs.password_hash == rhs.password_hash);
}
}

