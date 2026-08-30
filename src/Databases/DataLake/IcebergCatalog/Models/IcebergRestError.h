#pragma once
#include "config.h"

#if USE_AVRO

#include <optional>
#include <string>

namespace DataLake::IcebergRestModels
{

struct ErrorResponse
{
    std::string message;
    std::string type;
    int code = 0;
};

std::optional<ErrorResponse> tryParseErrorResponse(const std::string & json);
std::string serializeErrorResponse(const ErrorResponse & error);

}

#endif
