#pragma once

namespace DB
{

/// Runs `f` and reports whether it completed: false if it threw, and the exception is swallowed.
///
/// For `noexcept` code whose contract on failure is "do nothing and return false", such as adding
/// an attribute to an OpenTelemetry span from a destructor or an exception handler. Keeps the
/// `try`/`catch` out of the calling code while leaving the boundary visible at the call site.
/// Not for hiding failures whose outcome matters to the caller: those must propagate.
template <typename F>
bool tryOrFalse(F && f) noexcept
{
    try
    {
        f();
    }
    catch (...) // Ok: the caller's contract is to return false on failure
    {
        return false;
    }
    return true;
}

}
