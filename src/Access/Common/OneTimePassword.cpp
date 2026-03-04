#include <string_view>
#include <Access/Common/OneTimePassword.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>
#include <Poco/String.h>

#include "config.h"

#if USE_SSL && USE_LIBCOTP

#include <cotp.h>

constexpr int TOTP_SHA512 = SHA512;
constexpr int TOTP_SHA256 = SHA256;
constexpr int TOTP_SHA1 = SHA1;
#undef SHA512
#undef SHA256
#undef SHA1

#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

/// Checks if the secret contains only valid base32 characters.
/// The secret may contain spaces, which are ignored and lower-case characters, which are converted to upper-case.
String normalizeOneTimePasswordSecret(const String & secret)
{
    static const UInt8 b32_alphabet[] = u8"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
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
            result[n] = static_cast<char>(std::toupper(secret[i]));
        ++n;
    }
    result.resize(n);

    if (result.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty secret for one-time password");
    return result;
}

static std::string_view toString(OneTimePasswordParams::Algorithm algorithm)
{
    switch (algorithm)
    {
        case OneTimePasswordParams::Algorithm::SHA1: return "SHA1";
        case OneTimePasswordParams::Algorithm::SHA256: return "SHA256";
        case OneTimePasswordParams::Algorithm::SHA512: return "SHA512";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown algorithm for one-time password: {}", static_cast<UInt32>(algorithm));
}

static OneTimePasswordParams::Algorithm hashingAlgorithmFromString(const String & algorithm_name)
{
    for (auto alg : {OneTimePasswordParams::Algorithm::SHA1, OneTimePasswordParams::Algorithm::SHA256, OneTimePasswordParams::Algorithm::SHA512})
    {
        if (Poco::toUpper(algorithm_name) == toString(alg))
            return alg;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown algorithm for one-time password: '{}'", algorithm_name);
}

OneTimePasswordParams::OneTimePasswordParams(Int32 num_digits_, Int32 period_, const String & algorithm_name_)
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

std::string_view OneTimePasswordParams::getAlgorithmName() const { return toString(algorithm); }


OneTimePasswordSecret::OneTimePasswordSecret(const String & key_, OneTimePasswordParams params_)
    : key(normalizeOneTimePasswordSecret(key_)), params(params_)
{
}

String getOneTimePasswordSecretLink(const OneTimePasswordSecret & secret)
{
    constexpr auto issuer_name = "ClickHouse";
    if (secret.params == OneTimePasswordParams{})
        return fmt::format("otpauth://totp/ClickHouse?issuer={}&secret={}", issuer_name, secret.key);

    return fmt::format("otpauth://totp/ClickHouse?issuer={}&secret={}&digits={}&period={}&algorithm={}",
        issuer_name, secret.key, secret.params.num_digits, secret.params.period, toString(secret.params.algorithm));
}

struct CStringDeleter { void operator()(char * ptr) const { std::free(ptr); } };

String getOneTimePassword(const String & secret [[ maybe_unused ]], const OneTimePasswordParams & config [[ maybe_unused ]], UInt64 current_time [[ maybe_unused ]])
{
#if USE_SSL && USE_LIBCOTP
    int sha_algo = config.algorithm == OneTimePasswordParams::Algorithm::SHA512 ? TOTP_SHA512
                 : config.algorithm == OneTimePasswordParams::Algorithm::SHA256 ? TOTP_SHA256
                 : TOTP_SHA1;

    cotp_error_t error;
    auto result = std::unique_ptr<char, CStringDeleter>(get_totp_at(secret.c_str(), current_time, config.num_digits, config.period, sha_algo, &error));

    if (result == nullptr || (error != NO_ERROR && error != VALID))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while retrieving one-time password, code: {}",
            static_cast<std::underlying_type_t<cotp_error_t>>(error));
    return String(result.get(), strlen(result.get()));
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "One-time password support is disabled, because ClickHouse was built without openssl library");
#endif
}


bool checkOneTimePassword(std::string_view password, const OneTimePasswordSecret & secret)
{
    if (password.size() != static_cast<size_t>(secret.params.num_digits)
     || !std::all_of(password.begin(), password.end(), ::isdigit))
        return false;

    auto current_time = static_cast<UInt64>(std::time(nullptr));
    for (int delta : {0, -1, 1})
    {
        if (password == getOneTimePassword(secret.key, secret.params, current_time + delta * secret.params.period))
            return true;
    }
    return false;
}

}
