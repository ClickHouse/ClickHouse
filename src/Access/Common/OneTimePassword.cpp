#include <Access/Common/OneTimePassword.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>
#include <Poco/String.h>

#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

static const UInt8 b32_alphabet[] = u8"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

String normalizeOneTimePasswordSecret(const String & secret)
{
    static const UInt8 b32_lower_alphabet[] = u8"abcdefghijklmnopqrstuvwxyz";

    constexpr static UInt8 CHAR_IS_VALID = 1;
    constexpr static UInt8 CHAR_IS_LOWER = 2;
    std::array<UInt8, 128> table = {};
    for (const auto * p = b32_alphabet; *p; p++)
        table[*p] = CHAR_IS_VALID;
    for (const auto * p = b32_lower_alphabet; *p; p++)
        table[*p] = CHAR_IS_LOWER;

    String result = secret;
    size_t i = 0;
    size_t n = 0;
    for (; i < secret.size(); ++i)
    {
        if (secret[i] == ' ' || secret[i] == '=')
            continue;
        size_t idx = static_cast<UInt8>(secret[i]);
        if (idx >= table.size() || table[idx] == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid character in base32 secret: '{}'", secret[i]);
        if (table[idx] == CHAR_IS_VALID)
            result[n] = secret[i];
        if (table[idx] == CHAR_IS_LOWER)
            result[n] = std::toupper(secret[i]);
        ++n;
    }
    result.resize(n);

    if (result.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty secret for one-time password");
    return result;
}

static bool validateBase32Secret(const String & secret)
{
    if (secret.empty())
        return false;

    std::array<UInt8, 128> table = {};
    for (const auto * p = b32_alphabet; *p; p++)
        table[*p] = 1;
    for (const auto c : secret)
    {
        size_t idx = static_cast<UInt8>(c);
        if (idx >= table.size() || table[idx] == 0)
            return false;
    }
    return true;
}

static std::string_view toString(OneTimePasswordConfig::Algorithm algorithm)
{
    switch (algorithm)
    {
        case OneTimePasswordConfig::Algorithm::SHA1: return "SHA1";
        case OneTimePasswordConfig::Algorithm::SHA256: return "SHA256";
        case OneTimePasswordConfig::Algorithm::SHA512: return "SHA512";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown algorithm for one-time password: {}", static_cast<UInt32>(algorithm));
}

static OneTimePasswordConfig::Algorithm hashingAlgorithmFromString(const String & algorithm_name)
{
    for (auto alg : {OneTimePasswordConfig::Algorithm::SHA1, OneTimePasswordConfig::Algorithm::SHA256, OneTimePasswordConfig::Algorithm::SHA512})
    {
        if (Poco::toUpper(algorithm_name) == toString(alg))
            return alg;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown algorithm for one-time password: '{}'", algorithm_name);
}

OneTimePasswordConfig::OneTimePasswordConfig(Int32 num_digits_, Int32 period_, const String & algorithm_name_)
{
    if (num_digits_)
        num_digits = num_digits_;
    if (period_)
        period = period_;
    if (!algorithm_name_.empty())
        algorithm = hashingAlgorithmFromString(algorithm_name_);

    if (num_digits < 4 || 10 < num_digits)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid number of digits for one-time password: {}", num_digits);
    if (period <= 0 || 120 < period)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid period for one-time password: {}", period);
}

std::string_view OneTimePasswordConfig::getAlgorithmName() const { return toString(algorithm); }

String getOneTimePasswordLink(const String & secret, const OneTimePasswordConfig & config)
{
    validateBase32Secret(secret);

    if (config == OneTimePasswordConfig{})
        return fmt::format("otpauth://totp/ClickHouse?issuer=ClickHouse&secret={}", secret);

    return fmt::format("otpauth://totp/ClickHouse?issuer=ClickHouse&secret={}&digits={}&period={}&algorithm={}",
        secret, config.num_digits, config.period, toString(config.algorithm));
}

bool checkOneTimePassword(const String & password, const String & secret, const OneTimePasswordConfig & config)
{
    return password == getOneTimePassword(secret, config);
}

}

#if USE_SSL

#include <cotp.h>

constexpr int TOTP_SHA512 = SHA512;
constexpr int TOTP_SHA256 = SHA256;
constexpr int TOTP_SHA1 = SHA1;
#undef SHA512
#undef SHA256
#undef SHA1

namespace DB
{

String getOneTimePassword(const String & secret, const OneTimePasswordConfig & config)
{
    validateBase32Secret(secret);

    cotp_error_t error;
    int sha_algo = config.algorithm == OneTimePasswordConfig::Algorithm::SHA512 ? TOTP_SHA512
                 : config.algorithm == OneTimePasswordConfig::Algorithm::SHA256 ? TOTP_SHA256
                 : TOTP_SHA1;

    auto result = std::unique_ptr<char>(get_totp(secret.c_str(), config.num_digits, config.period, sha_algo, &error));
    if (result == nullptr || (error != NO_ERROR && error != VALID))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while retrieving one-time password, code: {}",
            static_cast<std::underlying_type_t<cotp_error_t>>(error));
    return String(result.get(), strlen(result.get()));
}

}

#else

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

String getOneTimePassword(const String & secret)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "One-time password support is disabled, because ClickHouse was built without openssl library");
}

}

#endif
