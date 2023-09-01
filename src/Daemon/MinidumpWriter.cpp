#include "MinidumpWriter.h"
#include <atomic>
#include <filesystem>
#include <Poco/Util/LayeredConfiguration.h>

#if USE_BREAKPAD && !defined(CLICKHOUSE_PROGRAM_STANDALONE_BUILD)

namespace ClickhouseBreakpad
{
// Create the exception handler object
void createExceptionHandler(const std::string & dump_path, bool minidump, bool generate_coredump);

// Handle the crash signals, should be called during CH crash signal handler and before calling default
// signal handler.
// The signal flow looks like this:
//   ClickHouse signal
//   handler
//        |
//        V
//   eh.handleSignal -------------------------| (clones a new process which
//        |                                   |  shares an address space with
//   (wait for cloned                         |  the crashed process. This
//     process)                               |  allows us to ptrace the crashed
//        |                                   |  process)
//        V                                   V
//    Clickhouse continue            ThreadEntry (static function to bounce
//    signal handler                          |      back into the object)
//                                            |
//                                            V
//                                          DoDump  (writes minidump)
//                                            |
//                                            V
//                                         sys_exit
void handler(int sig, siginfo_t * info, void * context);
}

namespace MinidumpWriter
{

void initialize([[maybe_unused]] Poco::Util::LayeredConfiguration & config)
{
    static std::atomic_bool initialized = false;
    if (initialized.load())
        return;
    auto minidump = config.getBool("minidump", true);
    auto generate_coredump = config.getBool("minidump_generate_core", true);
    // By default, dump to log folder
    std::string dump_path = std::filesystem::path(config.getString("logger.log", "/tmp/log.log")).parent_path();
    ClickhouseBreakpad::createExceptionHandler(dump_path, minidump, generate_coredump);
    initialized.store(true);
}

void signalHandler(int sig, siginfo_t * info, void * context)
{
    ClickhouseBreakpad::handler(sig, info, context);
}

}
#else
namespace MinidumpWriter
{
void initialize(Poco::Util::LayeredConfiguration &)
{
}
void signalHandler(int, siginfo_t *, void *)
{
}
}
#endif
