#include <Processors/Formats/fuzzers/AvroFuzzerCommon.h>

#include <base/unit.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

#include <Parsers/SecretArguments.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>

#include <Compiler.hh>

namespace DB::ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

/// No engine is registered, so the secrets of their arguments are shown.
[[maybe_unused]] static const bool secret_arguments_finder_installed
    = (DB::setSecretArgumentsFinder(&DB::NoSecretArgumentsFinder::instance()), true);

namespace DB::AvroFuzzer
{

ContextMutablePtr getContext()
{
    static SharedContextHolder shared_context = Context::createShared();
    static ContextMutablePtr context = Context::createGlobal(shared_context.get());
    return context;
}

void initialize()
{
    static bool initialized = false;
    if (initialized)
        return;
    initialized = true;

    getContext()->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerAggregateFunctions();
    registerFormats();
}

void resetMemoryLimits()
{
    total_memory_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1_GiB);
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.setHardLimit(1_GiB);
}

Variation::Variation(UInt8 byte)
    : allow_missing_fields(byte & 1)
    , null_as_default(byte & 2)
    , type_mode((byte >> 2) & 3)
    , only_even_columns(byte & 16)
    , only_count(byte & 32)
{
    static constexpr UInt64 block_sizes[] = {1, 13, 1000, 65409};
    max_block_size = block_sizes[(byte >> 6) & 3];
}

FormatSettings makeFormatSettings(const Variation & variation)
{
    FormatSettings format_settings = getFormatSettings(getContext());
    format_settings.avro.allow_missing_fields = variation.allow_missing_fields;
    format_settings.null_as_default = variation.null_as_default;
    return format_settings;
}

const DataTypes & scalarTargetTypes()
{
    static const DataTypes types = []
    {
        DataTypes result;
        for (const char * name :
             {"Int8", "UInt8", "Int16", "UInt32", "Int64", "UInt64", "Int128", "UInt256", "Float32", "Float64", "Bool", "String",
              "FixedString(4)", "FixedString(16)", "UUID", "Date", "Date32", "DateTime", "DateTime64(3)", "DateTime64(9)",
              "Decimal32(2)", "Decimal64(4)", "Decimal128(10)", "Decimal256(20)", "Enum8('a' = 1, 'b' = 2)",
              "Enum16('x' = 1000, 'y' = 2)", "IPv4", "IPv6", "Variant(String, Int64)", "LowCardinality(String)"})
            result.push_back(DataTypeFactory::instance().get(name));
        return result;
    }();
    return types;
}

DataTypePtr transformType(const DataTypePtr & type, UInt8 type_mode, size_t salt, size_t depth)
{
    if (type_mode == 0 || depth > 8)
        return type;

    const WhichDataType which(type);

    if (which.isArray())
        return std::make_shared<DataTypeArray>(
            transformType(assert_cast<const DataTypeArray &>(*type).getNestedType(), type_mode, salt + 1, depth + 1));

    if (which.isMap())
    {
        const auto & map_type = assert_cast<const DataTypeMap &>(*type);
        return std::make_shared<DataTypeMap>(map_type.getKeyType(), transformType(map_type.getValueType(), type_mode, salt + 1, depth + 1));
    }

    if (which.isTuple())
    {
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
        DataTypes elements;
        for (size_t i = 0; i != tuple_type.getElements().size(); ++i)
            elements.push_back(transformType(tuple_type.getElement(i), type_mode, salt + i + 1, depth + 1));
        return std::make_shared<DataTypeTuple>(elements, tuple_type.getElementNames());
    }

    if (which.isNullable())
    {
        auto nested = transformType(removeNullable(type), type_mode, salt, depth + 1);
        return nested->canBeInsideNullable() ? makeNullable(nested) : nested;
    }

    DataTypePtr result = type;
    if (type_mode == 2)
        result = scalarTargetTypes()[salt % scalarTargetTypes().size()];
    else if (type_mode == 3)
        result = scalarTargetTypes()[(salt * 7 + 3) % scalarTargetTypes().size()];

    if (type_mode != 2 && result->canBeInsideNullable())
        result = makeNullable(result);
    return result;
}

Block makeHeader(const NamesAndTypesList & structure, const Variation & variation)
{
    Block header;
    size_t index = 0;
    for (const auto & [name, type] : structure)
    {
        if (!variation.only_even_columns || index % 2 == 0)
        {
            auto column_type = transformType(type, variation.type_mode, index);
            header.insert(ColumnWithTypeAndName(column_type->createColumn(), column_type, name));
        }
        ++index;
    }
    return header;
}

#if USE_AVRO_RUST
NamesAndTypesList inferStructure(const AvroRust::Schema & schema, const FormatSettings & format_settings)
{
    const AvroRust::Node root = schema.root();
    if (root.kind() != AVRO_KIND_RECORD)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Root schema must be a record");

    NamesAndTypesList names_and_types;
    for (UInt32 i = 0; i < root.childCount(); ++i)
        names_and_types.emplace_back(
            String(root.fieldName(i)), AvroRust::inferType(root.child(i), format_settings.schema_inference_allow_nullable_tuple_type));
    return names_and_types;
}
#else
NamesAndTypesList inferStructure(const avro::ValidSchema & schema, const FormatSettings & format_settings)
{
    const avro::NodePtr root_node = schema.root();
    if (root_node->type() != avro::Type::AVRO_RECORD)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Root schema must be a record");

    NamesAndTypesList names_and_types;
    for (int i = 0; i != static_cast<int>(root_node->leaves()); ++i)
        names_and_types.emplace_back(
            root_node->nameAt(i),
            AvroSchemaReader::avroNodeToDataType(root_node->leafAt(i), format_settings.schema_inference_allow_nullable_tuple_type));
    return names_and_types;
}

avro::ValidSchema compileSchema(const std::string & text)
{
    return avro::compileJsonSchemaFromString(text, MAX_AVRO_SCHEMA_DEPTH);
}

std::string printSchema(const avro::ValidSchema & schema, bool pretty)
{
    return schema.toJson(pretty);
}
#endif

void readAll(InputFormatPtr input_format, bool only_count)
{
    if (only_count)
        input_format->needOnlyCount();

    QueryPipeline pipeline(Pipe(std::move(input_format)));
    PullingPipelineExecutor executor(pipeline);
    /// A row of `null` fields takes no bytes, so a block of a few bytes can declare millions of rows, and with
    /// `max_block_size` 1 each of them is a chunk. The pipeline costs the same per chunk, which only measures the executor.
    static constexpr size_t MAX_CHUNKS_TO_PULL = 1 << 16;
    size_t chunks = 0;
    Block block;
    while (chunks < MAX_CHUNKS_TO_PULL && executor.pull(block))
        ++chunks;
}

}
