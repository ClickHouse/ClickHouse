
#include "MinidumpWriter.h"
#include <fmt/ostream.h>
#include <sys/wait.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <filesystem>
#include <memory>



namespace MinidumpWriter
{
#if USE_BREAKPAD

static bool minidump = false;
static bool generate_coredump = false;
static std::shared_ptr<google_breakpad::MinidumpDescriptor> descriptor;
static std::shared_ptr<google_breakpad::ExceptionHandler> eh;

static bool callback(const google_breakpad::MinidumpDescriptor & desc, void *, bool succeeded)
{
    fmt::print(stderr, "Minidump dump path: {}\n", desc.path());
    int status;
    if (generate_coredump)
    {
        auto coredump_path = std::filesystem::path(desc.path()).replace_extension("core");
        status = Minidump2Core::generate(desc.path(), coredump_path.c_str());
        if (status == 0)
        {
            fmt::print(stderr, "Coredump dump path: {}\n", coredump_path.string());
            succeeded = true;
        }
        else
            fmt::print(stderr, "Cannot generate coredump\n");
    }
    return succeeded;
}

#endif

bool useMinidump()
{
#if USE_BREAKPAD
    return minidump;
#else
    return false;
#endif
}

void initialize([[maybe_unused]] Poco::Util::LayeredConfiguration & config)
{
#if USE_BREAKPAD
    minidump = config.getBool("minidump", true);
    generate_coredump = config.getBool("minidump_generate_core", true);
    if (minidump)
    {
        std::string dump_path = std::filesystem::path(config.getString("logger.log", "/tmp/log.log")).parent_path();
        descriptor = std::make_shared<google_breakpad::MinidumpDescriptor>(dump_path);
    }
#endif
}

void installMinidumpHandler(int)
{
#if USE_BREAKPAD
    if (minidump && !eh)
        eh = std::make_shared<google_breakpad::ExceptionHandler>(*descriptor, nullptr, callback, nullptr, true, -1);
#endif
}

}
