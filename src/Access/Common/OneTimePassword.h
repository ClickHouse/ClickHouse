#pragma once

#include <base/types.h>
#include <optional>

namespace DB
{

struct OneTimePasswordParams
{
    Int32 num_digits = 6;
    Int32 period = 30;

    enum class Algorithm : UInt8
    {
        SHA1,
        SHA256,
        SHA512,
    } algorithm = Algorithm::SHA1;

    explicit OneTimePasswordParams(std::optional<Int32> num_digits_ = {}, std::optional<Int32> period_ = {}, std::optional<String> algorithm_name_ = {});

    bool operator==(const OneTimePasswordParams &) const = default;

    std::string_view getAlgorithmName() const;
};

struct OneTimePasswordSecret
{
    String key;
    OneTimePasswordParams params;

    explicit OneTimePasswordSecret(
        const String & key_,
        OneTimePasswordParams params_ = OneTimePasswordParams{});
};

String getOneTimePasswordSecretLink(const OneTimePasswordSecret & secret);

/// Checks the code against the current time step, with a tolerance of one time step in both directions.
bool checkOneTimePassword(std::string_view password, const OneTimePasswordSecret & secret);

/// Same as `checkOneTimePassword`, but also marks the accepted code as used (RFC 6238, Section 5.2):
/// codes for time steps at or before the last accepted one are rejected. Call it once per successful
/// authentication, after all other credentials are verified. The used-codes state is local to the server process.
bool checkAndConsumeOneTimePassword(std::string_view password, const OneTimePasswordSecret & secret);

}
