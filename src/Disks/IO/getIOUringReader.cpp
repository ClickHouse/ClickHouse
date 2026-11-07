#include <Disks/IO/getIOUringReader.h>

#if USE_LIBURING

#include <Interpreters/Context.h>
#include <Common/ErrorCodes.h>
#include <Common/SeccompFilter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

std::unique_ptr<IOUringReader> createIOUringReader()
{
    return std::make_unique<IOUringReader>(512);
}

IOUringReader & getIOUringReaderOrThrow(ContextPtr context)
{
    if (isIOUringRefusedBySeccomp())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Cannot use the io_uring read method: the seccomp policy of the server refuses the io_uring system calls. "
            "Set the `seccomp_allow_io_uring` server setting to `true` and restart the server, or use another read method");

    auto & reader = context->getIOUringReader();
    if (!reader.isSupported())
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "io_uring is not supported by this system");
    }
    return reader;
}

IOUringReader & getIOUringReaderOrThrow()
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");
    return getIOUringReaderOrThrow(context);
}

}
#endif
