#pragma once

#include <base/types.h>

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

    explicit OneTimePasswordParams(Int32 num_digits_ = {}, Int32 period_ = {}, const String & algorithm_name_ = {});

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
bool checkOneTimePassword(std::string_view password, const OneTimePasswordSecret & secret);

}
