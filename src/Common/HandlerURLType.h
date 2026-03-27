#pragma once

#include <cstdint>
#include <string>


namespace DB
{

/// URL matching type for SQL-defined HTTP handlers.
enum class HandlerURLType : uint8_t
{
    Exact,
    Prefix,
    Regexp,
};

/// Convert enum to string for serialization/formatting.
const char * handlerURLTypeToString(HandlerURLType type);

/// Parse string to enum; throws on unknown value.
HandlerURLType parseHandlerURLType(const std::string & str);

}
