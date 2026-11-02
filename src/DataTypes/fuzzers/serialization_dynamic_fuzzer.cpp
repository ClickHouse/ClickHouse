#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDynamic.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnDynamic.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

#include <Parsers/SecretArguments.h>

/// No engine is registered, so the secrets of their arguments are shown.
[[maybe_unused]] static const bool secret_arguments_finder_installed
    = (DB::setSecretArgumentsFinder(&DB::NoSecretArgumentsFinder::instance()), true);

using namespace DB;

ContextMutablePtr context;
extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    /// Dynamic type can hold AggregateFunction values, so register them.
    registerAggregateFunctions();

    return 0;
}

/// Auxiliary header at the start of the fuzz input:
///   [0]    max_types_selector: chooses max_dynamic_types (1, 4, 16, 255)
///   [1]    flags: bit 3 = decode_types_in_binary_format, bit 4 = limit_binary_type_complexity
///   [2..9] rows: uint64_t LE, capped at 65536
///
/// SerializationDynamic binary layout (simplified):
///   Prefix stream: [structure_version: VarUInt][num_dynamic_types: VarUInt]
///                  [type_name_0: String] ... [type_name_N: String]
///   Data streams:  each named dynamic type gets its own substream
///   Shared data:   overflow values go into a shared binary blob
///
/// The structure_version controls which serialization path is taken
/// (FLATTENED vs standard), making this fuzzer exercise both branches.
struct AuxiliaryRandomData
{
    uint8_t max_types_selector;
    uint8_t flags;
    uint64_t rows;
};

static size_t selectMaxTypes(uint8_t selector)
{
    switch (selector % 4)
    {
        case 0:  return 1;
        case 1:  return 4;
        case 2:  return 16;
        case 3:
        default: return 255;
    }
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        if (size < sizeof(AuxiliaryRandomData))
            return 0;

        const auto * aux = reinterpret_cast<const AuxiliaryRandomData *>(data);
        const size_t rows = static_cast<size_t>(aux->rows % 65536);
        const bool decode_types_in_binary_format = (aux->flags & 8) != 0;
        /// Both limits are reachable through the Native format: `input_format_binary_max_type_complexity` is 1000
        /// by default and 0 means unlimited. The `FormatSettings` default is 0, so pick between them explicitly.
        const bool limit_binary_type_complexity = (aux->flags & 16) != 0;
        const size_t max_dynamic_types = selectMaxTypes(aux->max_types_selector);

        auto dynamic_type = std::make_shared<DataTypeDynamic>(max_dynamic_types);
        auto serialization = dynamic_type->getDefaultSerialization();

        size -= sizeof(AuxiliaryRandomData);
        data += sizeof(AuxiliaryRandomData);

        DB::ReadBufferFromMemory in(data, size);

        FormatSettings format_settings;
        format_settings.binary.max_binary_array_size = 100;
        format_settings.binary.max_binary_string_size = 100;
        format_settings.native.decode_types_in_binary_format = decode_types_in_binary_format;
        format_settings.binary.max_binary_type_complexity = limit_binary_type_complexity ? 1000 : 0;

        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &in; };
        settings.position_independent_encoding = false;
        /// Fuzz input is untrusted: outside the Native format, inconsistent data is a LOGICAL_ERROR by design.
        settings.native_format = true;
        settings.format_settings = &format_settings;

        ISerialization::DeserializeBinaryBulkStatePtr state;
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

        MutableColumnPtr column = dynamic_type->createColumn();
        serialization->deserializeBinaryBulkWithMultipleStreams(*column, rows, settings, state, nullptr);
    }
    catch (...)
    {
    }

    return 0;
}
