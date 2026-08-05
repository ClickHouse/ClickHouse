#include <DataTypes/Serializations/PrefixReadCancellationChecker.h>

#include <Common/CurrentThread.h>

namespace DB
{

void PrefixReadCancellationChecker::throwIfCancelled()
{
    try
    {
        CurrentThread::checkIfNotCancelled();
    }
    catch (const Exception & e)
    {
        /// Copy-constructs, so `code()` and the message survive; only the dynamic type changes.
        throw PrefixReadCancelledException(e);
    }
    /// A cause that is not a `DB::Exception` propagates untouched: it cannot be copied into one
    /// without losing information, so the readers fall back to `isRetryableException` for it.
}

bool isPrefixReadCancelled(std::exception_ptr exception_ptr)
{
    if (!exception_ptr)
        return false;

    try
    {
        std::rethrow_exception(exception_ptr);
    }
    catch (const PrefixReadCancelledException &)
    {
        return true;
    }
    catch (...) /// NOLINT(bugprone-empty-catch)
    {
        /// Ok: the rethrow is only a type test, so any other type means this was not a cancellation.
    }

    return false;
}

}
