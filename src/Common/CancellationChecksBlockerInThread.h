#pragma once

#include <boost/noncopyable.hpp>

namespace DB
{

/// Blocks cancellation checks while alive.
struct CancellationChecksBlockerInThread : public boost::noncopyable
{
    CancellationChecksBlockerInThread();
    ~CancellationChecksBlockerInThread();

    static bool isBlocked();
};

}
