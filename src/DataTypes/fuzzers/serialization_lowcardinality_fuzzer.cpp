#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/IColumn.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Interpreters/Context.h>

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

    return 0;
}

/// Auxiliary header bytes at the start of the fuzz input:
///   [0]    select inner type: 0 = LowCardinality(String), 1 = LowCardinality(Nullable(UInt64))
///   [1]    native_format flag (0 or 1)
///   [2..9] number of rows to read (uint64_t little-endian, capped at 65536)
struct AuxiliaryRandomData
{
    uint8_t type_selector;
    uint8_t native_format;
    uint64_t rows;
};

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
        size_t rows = aux->rows % 65536;
        bool use_native_format = (aux->native_format & 1) != 0;

        DataTypePtr inner_type;
        if (aux->type_selector % 2 == 0)
            inner_type = std::make_shared<DataTypeString>();
        else
            inner_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());

        auto lc_type = std::make_shared<DataTypeLowCardinality>(inner_type);
        auto serialization = lc_type->getDefaultSerialization();

        size -= sizeof(AuxiliaryRandomData);
        data += sizeof(AuxiliaryRandomData) / sizeof(uint8_t);

        DB::ReadBufferFromMemory in(data, size);

        FormatSettings format_settings;
        format_settings.binary.max_binary_array_size = 100;
        format_settings.binary.max_binary_string_size = 100;

        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &in; };
        settings.position_independent_encoding = false;
        settings.native_format = use_native_format;
        settings.format_settings = &format_settings;

        ISerialization::DeserializeBinaryBulkStatePtr state;

        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);

        ColumnPtr column = lc_type->createColumn();
        serialization->deserializeBinaryBulkWithMultipleStreams(column, 0, rows, settings, state, nullptr);
    }
    catch (...)
    {
    }

    return 0;
}
