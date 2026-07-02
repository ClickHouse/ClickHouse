#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDynamic.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnDynamic.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

using namespace DB;

ContextMutablePtr context;
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
///   [1]    flags: bit 0 = native_format, bit 1 = use_specialized_prefixes
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
        const bool use_native_format = (aux->flags & 1) != 0;
        const bool use_specialized_prefixes = (aux->flags & 2) != 0;
        const size_t max_dynamic_types = selectMaxTypes(aux->max_types_selector);

        auto dynamic_type = std::make_shared<DataTypeDynamic>(max_dynamic_types);
        auto serialization = dynamic_type->getDefaultSerialization();

        size -= sizeof(AuxiliaryRandomData);
        data += sizeof(AuxiliaryRandomData);

        DB::ReadBufferFromMemory in(data, size);

        FormatSettings format_settings;
        format_settings.binary.max_binary_array_size = 100;
        format_settings.binary.max_binary_string_size = 100;

        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &in; };
        settings.position_independent_encoding = false;
        settings.native_format = use_native_format;
        settings.format_settings = &format_settings;
        settings.use_specialized_prefixes_and_suffixes_substreams = use_specialized_prefixes;

        ISerialization::DeserializeBinaryBulkStatePtr state;
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

        ColumnPtr column = dynamic_type->createColumn();
        serialization->deserializeBinaryBulkWithMultipleStreams(column, 0, rows, settings, state, nullptr);
    }
    catch (...)
    {
    }

    return 0;
}
