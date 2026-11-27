#pragma once

#include <Poco/Message.h>
#include <base/types.h>

namespace ProfileEvents
{
    /// Increment a counter for log messages.
    void incrementForLogMessage(Poco::Message::Priority priority);

    /// Increment time consumed by logging.
    void incrementLoggerElapsedNanoseconds(UInt64 ns);
}
