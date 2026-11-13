#pragma once

#include <Common/Exception.h>

namespace DB
{

/// Thrown by the cancellation predicate of a background merge or mutation.
class BackgroundTaskCancellationException final : public Exception
{
public:
    using Exception::Exception;

    BackgroundTaskCancellationException * clone() const override
    {
        return new BackgroundTaskCancellationException(*this);
    }

    void rethrow() const override
    {
        throw *this; /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
    }
};

}
