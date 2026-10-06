#include <Processors/Executors/Runtime/Wyrd/Facade/Log.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/Syscall.h>
#else
#    include <base/getThreadId.h>

#    include <fmt/format.h>

#    include <cstdio>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

void log(std::string message, std::source_location location)
{
    trace(std::move(message), location);
}

#else

void log(std::string message, std::source_location location)
{
    fmt::print(stderr, "Wyrd thread {}: {} at {}:{}\n", getThreadId(), message, location.file_name(), location.line());
}

#endif

}
