#include <base/types.h>

#include <Formats/NativeReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/registerFormats.h>

#include <IO/ReadBufferFromMemory.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

using namespace DB;

static bool initialized = false;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (initialized)
        return 0;
    initialized = true;

    static SharedContextHolder shared_context = Context::createShared();
    static ContextMutablePtr context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerAggregateFunctions();
    registerFormats();

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        /// Need at least one byte to choose the decode mode.
        if (size < 1)
            return 0;

        /// Use the first byte to toggle decode_types_in_binary_format:
        ///   0 = text type names (classic protocol)
        ///   1 = binary-encoded type names (DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION+)
        bool use_binary_type_encoding = (data[0] & 1) != 0;

        /// Pick a server_revision that enables/disables binary type encoding.
        /// DBMS_TCP_PROTOCOL_VERSION is a recent revision that enables custom serialization.
        /// DBMS_MIN_REVISION_WITH_CLIENT_INFO is an older revision that predates it.
        UInt64 server_revision = use_binary_type_encoding
            ? DBMS_TCP_PROTOCOL_VERSION
            : DBMS_MIN_REVISION_WITH_CLIENT_INFO;

        FormatSettings format_settings;
        format_settings.native.decode_types_in_binary_format = use_binary_type_encoding;

        DB::ReadBufferFromMemory in(data + 1, size - 1);
        NativeReader reader(in, server_revision, std::make_optional(format_settings));

        Block block;
        while ((block = reader.read()))
            ;
    }
    catch (...)
    {
    }

    return 0;
}
