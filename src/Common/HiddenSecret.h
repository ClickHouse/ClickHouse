#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace DB
{

class Field;

/// How a hidden secret is shown, bare and as a SQL string literal.
inline constexpr std::string_view HIDDEN_SECRET = "[HIDDEN]";
inline constexpr std::string_view HIDDEN_SECRET_LITERAL = "'[HIDDEN]'";

/// The masker of an engine setting whose whole value is secret, see `SecretArgumentsSpec::secret_settings`.
inline std::optional<std::string> hideSecretValue(const Field &)
{
    return std::string(HIDDEN_SECRET_LITERAL);
}

}
