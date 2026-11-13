#pragma once

#include <optional>

namespace DB
{

/// A function that captures the setting `cast_fixed_string_to_string_strip_trailing_zeros` when it is created, so its
/// result depends on the value of the setting in the scope where it was built, such as a subquery with its own `SETTINGS`.
/// The serialization of a query plan transfers that value, because the receiver rebuilds the function with its own context.
class IFunctionWithFixedStringToStringMode
{
public:
    virtual ~IFunctionWithFixedStringToStringMode() = default;

    /// Whether the function removes the trailing zero bytes of a `FixedString` converted to `String`,
    /// or nullopt if the result of the function does not depend on it.
    virtual std::optional<bool> getFixedStringToStringStripTrailingZeros() const = 0;
};

}
