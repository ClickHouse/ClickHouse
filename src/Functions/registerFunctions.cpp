#include "config.h"

#include <Functions/FunctionFactory.h>
#include <Common/MemoryTrackerBlockerInThread.h>


namespace DB
{

void registerFunctions();
void registerFunctions()
{
    /// Registration runs once and allocates while building static function metadata (for example
    /// HMAC enumerates the OpenSSL digests through a C callback). It must not be subject to the
    /// memory tracker's limit or fault injection, so block tracking for its duration.
    MemoryTrackerBlockerInThread blocker;

    auto & factory = FunctionFactory::instance();

    for (const auto & [_, reg] : FunctionRegisterMap::instance())
        reg(factory);
}

}
