#pragma once

/// Shared plumbing of the `avro_*_fuzzer` targets: one-time initialization, memory limits and
/// the derivation of target headers from an Avro schema.
///
/// The fuzzers cover the Avro read paths: with `USE_AVRO_RUST` the Rust decoder (`AvroRust::Schema`, `AvroRust::Plan`,
/// the OCF reader through `AvroRowInputFormat`), otherwise the C++ ones over `contrib/avro` as ClickHouse uses it
/// (`AvroRowInputFormat`, `AvroConfluentRowInputFormat`, `AvroSchemaReader`, `AvroDeserializer`). The Iceberg and Paimon
/// readers (`AvroForIcebergDeserializer`) follow the same switch.
/// They do not touch the network.

#include "config.h"

#include <base/types.h>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>

#include <DataTypes/IDataType.h>

#include <Formats/FormatSettings.h>

#include <Interpreters/Context_fwd.h>

#include <Processors/Formats/IInputFormat.h>

#include <Schema.hh>
#include <ValidSchema.hh>

#if USE_AVRO_RUST
#include <Processors/Formats/Impl/Avro/AvroRustSchema.h>
#endif

#include <string>

namespace DB::AvroFuzzer
{

ContextMutablePtr getContext();

/// Call from `LLVMFuzzerInitialize`.
void initialize();

/// Call at the start of every `LLVMFuzzerTestOneInput`. A runaway allocation then throws
/// `MEMORY_LIMIT_EXCEEDED` (which the fuzzer catches) instead of being killed by libFuzzer's RSS limit.
void resetMemoryLimits();

/// The variation byte. Every bit selects an independent switch so that the corpus can mutate them separately.
///
///   bit 0:    `input_format_avro_allow_missing_fields`
///   bit 1:    `input_format_null_as_default`
///   bits 2-3: how the header is derived from the inferred structure (see `transformType`)
///   bit 4:    keep only the even columns, so that the other fields go through `createSkipFn`
///   bit 5:    `needOnlyCount`, the `SELECT count()` path through `AvroRowInputFormat::countRows`
///   bits 6-7: `max_block_size`
struct Variation
{
    bool allow_missing_fields = false;
    bool null_as_default = false;
    UInt8 type_mode = 0;
    bool only_even_columns = false;
    bool only_count = false;
    UInt64 max_block_size = 1;

    explicit Variation(UInt8 byte);
};

/// The format settings of the global context with the switches of `variation` applied.
FormatSettings makeFormatSettings(const Variation & variation);

/// The scalar target types the Avro deserializer has to be able to convert to or reject.
const DataTypes & scalarTargetTypes();

/// Maps the type inferred from the schema (`type_mode` 0) to a type the table could declare instead:
///   1: `Nullable` wrapped around everything that may be nullable,
///   2: scalars replaced by a rotating choice from `scalarTargetTypes`,
///   3: the same with another rotation, and `Nullable` where allowed.
/// Arrays, maps, tuples and `Nullable` are rebuilt around the transformed element types.
DataTypePtr transformType(const DataTypePtr & type, UInt8 type_mode, size_t salt, size_t depth = 0);

/// The header `AvroRowInputFormat` and friends are asked to produce: the structure inferred from the schema,
/// passed through `transformType`, optionally restricted to the even columns.
Block makeHeader(const NamesAndTypesList & structure, const Variation & variation);

#if USE_AVRO_RUST
/// The loop of `readSchema` over the Rust schema (`AvroRust::inferType` per field), as `AvroSchemaReader` does it for
/// the schema of a Confluent message.
NamesAndTypesList inferStructure(const AvroRust::Schema & schema, const FormatSettings & format_settings);
#else
/// The loop of `AvroSchemaReader::readSchema` that follows the read of the schema itself: the
/// registry-less equivalent for the schema of a Confluent message or of a file that is already parsed.
NamesAndTypesList inferStructure(const avro::ValidSchema & schema, const FormatSettings & format_settings);

/// The schema compiler as the server calls it for every schema it reads (`MAX_AVRO_SCHEMA_DEPTH` nesting limit).
avro::ValidSchema compileSchema(const std::string & text);

/// The text of a schema as `ValidSchema` prints it.
std::string printSchema(const avro::ValidSchema & schema, bool pretty);
#endif

/// Reads `input_format` through the same pipeline as `format_fuzzer`, to the end or up to a limit of chunks.
void readAll(InputFormatPtr input_format, bool only_count);

}
