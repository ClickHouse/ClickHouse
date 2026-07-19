#pragma once

#include <memory>
#include <optional>

namespace DB
{

class IColumn;

/// Reads a non-negative integer "limit" argument (a limit, count, or K) of any integer width
/// (`(U)Int8/16/32/64/128/256`) from a column, one value per row, and returns it as a `size_t`.
///
/// A negative value throws `BAD_ARGUMENTS`. A value too large for `size_t` becomes `SIZE_MAX`: for a
/// limit this is harmless because the caller clamps it with `min` against the array size. It is
/// therefore meant for "limit" arguments (how many elements to take or sort), not for "size"
/// arguments that allocate output (such as `arrayResize` or `arrayWithConstant`), which must reject
/// an over-large value rather than turning it into `SIZE_MAX`.
///
/// Build the implementation matching a column with `createArrayLimitGetter`.
class ArrayLimitGetter
{
public:
    virtual ~ArrayLimitGetter() = default;

    virtual size_t get(size_t row) const = 0;

    /// If the limit is the same for every row (a constant argument), returns it; otherwise nullopt.
    /// Lets a caller reserve more tightly for a constant argument.
    virtual std::optional<size_t> tryGetConstant() const { return {}; }
};

/// Builds an `ArrayLimitGetter` for `column`, which must be an integer column (possibly constant).
/// `function_name` is only used to build the exception message for a negative value.
std::unique_ptr<ArrayLimitGetter> createArrayLimitGetter(const IColumn & column, const char * function_name);

}
