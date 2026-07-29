#include <base/types.h>

#include <Formats/NativeReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/registerFormats.h>

#include <IO/ReadBufferFromMemory.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

using namespace DB;

static bool initialized = false;

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

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

        /// `NativeReader` has two independent switches, so use one selector bit for each.
        ///
        /// Bit 0 toggles the type encoding (a format setting):
        ///   0 = text type names (classic protocol)
        ///   1 = binary-encoded type names (`BinaryTypeIndex`)
        bool use_binary_type_encoding = (data[0] & 1) != 0;

        /// Bit 1 toggles the protocol revision, which gates the `has_custom` /
        /// `SerializationInfo` payload independently of the type encoding:
        ///   0 = a recent revision (>= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION)
        ///   1 = a legacy revision that predates custom serialization
        /// Both type encodings are exercised with both revisions this way.
        bool use_legacy_revision = (data[0] & 2) != 0;

        UInt64 server_revision = use_legacy_revision
            ? DBMS_MIN_REVISION_WITH_CLIENT_INFO
            : DBMS_TCP_PROTOCOL_VERSION;

        FormatSettings format_settings;
        format_settings.native.decode_types_in_binary_format = use_binary_type_encoding;

        DB::ReadBufferFromMemory in(data + 1, size - 1);
        NativeReader reader(in, server_revision, std::make_optional(format_settings));

        /// An empty `Block` is not an end-of-stream marker: a well-formed block with
        /// zero columns and zero rows also reads as empty. Consume the whole input
        /// instead, so that blocks following an empty one are parsed as well.
        while (!in.eof())
        {
            const auto position_before = in.count();
            reader.read();
            /// Defensive stop: `read` always consumes at least the block header,
            /// but never loop forever if it somehow does not advance.
            if (in.count() == position_before)
                break;
        }
    }
    catch (...)
    {
        /// Ok: malformed input is expected to throw.
    }

    return 0;
}
