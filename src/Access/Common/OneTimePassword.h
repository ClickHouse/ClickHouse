#pragma once

#include <base/types.h>

namespace DB
{

struct OneTimePasswordConfig
{
    Int32 num_digits = 6;
    Int32 period = 30;

    enum class Algorithm : UInt8
    {
        SHA1,
        SHA256,
        SHA512,
    } algorithm = Algorithm::SHA1;

    explicit OneTimePasswordConfig(Int32 num_digits_ = 0, Int32 period_ = 0, const String & algorithm_name_ = "");

    bool operator==(const OneTimePasswordConfig &) const = default;

    std::string_view getAlgorithmName() const;
};


String getOneTimePasswordLink(const String & secret, const OneTimePasswordConfig & config);

String getOneTimePassword(const String & secret, const OneTimePasswordConfig & config);
bool checkOneTimePassword(const String & password, const String & secret, const OneTimePasswordConfig & config);

/// Checks if the secret contains only valid base32 characters.
/// The secret may contain spaces, which are ignored and lower-case characters, which are converted to upper-case.
String normalizeOneTimePasswordSecret(const String & secret);

}
