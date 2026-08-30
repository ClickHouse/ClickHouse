#pragma once
#include "config.h"

#if USE_AVRO

#include <cstdint>
#include <optional>
#include <string>

namespace DataLake::IcebergRestModels
{

struct OAuthTokenResponse
{
    std::string access_token;
    std::optional<int64_t> expires_in;
    std::string token_type;
};

OAuthTokenResponse parseOAuthTokenResponse(const std::string & json, bool require_bearer_type = false);

}

#endif
