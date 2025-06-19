#pragma once
#include <Core/Types.h>


/// SQL intercept enum. Used in SQLInterceptMode::type.
enum class SQLInterceptMode : uint8_t
{
    LOG,  /// Query will be executed but log a warnning message.
    THROW, /// Query will throw an exception.
};
