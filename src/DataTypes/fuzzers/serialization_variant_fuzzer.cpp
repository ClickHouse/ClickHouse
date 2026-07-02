#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>

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

/// Auxiliary header at the start of the fuzz input:
///   [0]    variant_selector: chooses which Variant type to deserialize into
///   [1]    flags: bit 0 = native_format, bit 1 = use_specialized_prefixes
///   [2..9] rows: uint64_t LE, capped at 65536
///
/// Variant types exercised:
///   0: Variant(UInt64, String)                              — 2 variants, basic
///   1: Variant(UInt64, String, Int32, Float64)              — 4 variants
///   2: Variant(UInt64, String, Int32, Float64, Date, Nullable(UInt32)) — 6 variants
///   3: Variant(Array(UInt64), String, Tuple(UInt64, String)) — nested variants
///
/// The BASIC vs COMPACT discriminator serialization mode is encoded in the
/// stream data itself (first byte of the VariantDiscriminators prefix), so
/// both modes are naturally exercised by the fuzzer corpus.
struct AuxiliaryRandomData
{
    uint8_t variant_selector;
    uint8_t flags;
    uint64_t rows;
};

static DataTypePtr makeVariantType(uint8_t selector)
{
    switch (selector % 4)
    {
        case 0:
            return std::make_shared<DataTypeVariant>(DataTypes{
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeString>(),
            });
        case 1:
            return std::make_shared<DataTypeVariant>(DataTypes{
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeFloat64>(),
            });
        case 2:
            return std::make_shared<DataTypeVariant>(DataTypes{
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeDate>(),
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()),
            });
        case 3:
        default:
            return std::make_shared<DataTypeVariant>(DataTypes{
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()),
                std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeTuple>(DataTypes{
                    std::make_shared<DataTypeUInt64>(),
                    std::make_shared<DataTypeString>(),
                }),
            });
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

        DataTypePtr variant_type = makeVariantType(aux->variant_selector);
        auto serialization = variant_type->getDefaultSerialization();

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

        ColumnPtr column = variant_type->createColumn();
        serialization->deserializeBinaryBulkWithMultipleStreams(column, 0, rows, settings, state, nullptr);
    }
    catch (...)
    {
    }

    return 0;
}
