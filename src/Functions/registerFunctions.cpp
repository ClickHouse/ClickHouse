#include "config.h"

#include <Functions/FunctionFactory.h>
#include <Common/LockMemoryExceptionInThread.h>


namespace DB
{

void registerFunctions();
void registerFunctions()
{
    /// Registration runs once and allocates while building static function metadata (for example
    /// HMAC enumerates the OpenSSL digests through a C callback). It must not be subject to the
    /// memory tracker's limit or fault injection.
    LockMemoryExceptionInThread lock_memory_exception(VariableContext::Global);

    auto & factory = FunctionFactory::instance();

    for (const auto & [_, reg] : FunctionRegisterMap::instance())
        reg(factory);
}

}
