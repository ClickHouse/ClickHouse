#include <cstddef>
#include <cstdint>

#include <Processors/Formats/fuzzers/AvroFuzzerCommon.h>

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

using namespace DB;

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

/** Fuzzes the parse of an Avro schema and everything ClickHouse builds from it before it reads a single datum.
  *
  * The input is the text of a JSON schema, exactly what ends up in the `avro.schema` metadata key of a data file
  * or in the `schema` field of a Schema Registry reply. With `USE_AVRO_RUST` it is compiled by the production
  * `AvroRust::Schema` with `MAX_AVRO_SCHEMA_DEPTH` (see `fetchSchema` in `AvroConfluentSchemaRegistry.cpp`) and
  * the compiled schema is walked by:
  *   - the canonical form,
  *   - `AvroRust::inferType` (schema inference), with and without nullable tuples,
  *   - the `AvroRust::Plan` constructor for four headers derived from the inferred structure (see
  *     `AvroFuzzer::makeHeader`) and for the count-only plan.
  *
  * Without `USE_AVRO_RUST` the schema is compiled by the legacy compiler (`AvroFuzzer::compileSchema`) and walked by
  * `ValidSchema` printing, `AvroSchemaReader::avroNodeToDataType`, and the `AvroDeserializer` constructors of the
  * Iceberg readers and of `AvroRowInputFormat`.
  *
  * No datum is decoded here: `avro_ocf_fuzzer`, `avro_confluent_fuzzer` and `avro_iceberg_manifest_fuzzer` do that.
  *
  * Compile the code as follows:
  *   mkdir build_asan_fuzz
  *   cd build_asan_fuzz
  *   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
  *   ninja avro_schema_fuzzer
  *
  * The fuzzer can be run as follows:
  *   ../../../build_asan_fuzz/src/Processors/Formats/fuzzers/avro_schema_fuzzer corpus -jobs=64 -rss_limit_mb=8192 \
  *       -dict=../../../tests/fuzz/avro.dict
  *
  * The seed corpus is `tests/fuzz/avro_schema_fuzzer.in`.
  */
extern "C" int LLVMFuzzerInitialize(const int *, char ***)
{
    AvroFuzzer::initialize();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        AvroFuzzer::resetMemoryLimits();

        const std::string schema_text(reinterpret_cast<const char *>(data), size);

#if USE_AVRO_RUST
        /// The production schema compiler, as `fetchSchema` and the OCF reader use it.
        const AvroRust::Schema schema(schema_text, MAX_AVRO_SCHEMA_DEPTH);

        (void) schema.canonical();

        const AvroRust::Node root_node = schema.root();

        /// Inference of any node, as `AvroForIcebergDeserializer` does for the root of a manifest.
        try
        {
            AvroRust::inferType(root_node, /* allow_nullable_tuple_type = */ false);
            AvroRust::inferType(root_node, /* allow_nullable_tuple_type = */ true);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Not every schema is representable. The plan is still built for those that are.
        }

        /// The four variations cover: the identity header, `Nullable` everywhere with a column subset (skip functions),
        /// rotating scalar targets with missing fields allowed, and another rotation with `null_as_default`.
        for (UInt8 variation_byte : {UInt8(0), UInt8(4 | 16), UInt8(1 | 8), UInt8(2 | 1 | 12)})
        {
            const AvroFuzzer::Variation variation(variation_byte);
            const FormatSettings variation_settings = AvroFuzzer::makeFormatSettings(variation);
            try
            {
                const NamesAndTypesList structure = AvroFuzzer::inferStructure(schema, variation_settings);
                const Block header = AvroFuzzer::makeHeader(structure, variation);
                if (header.columns() == 0)
                    continue;
                AvroRust::Plan plan(header, schema, variation.allow_missing_fields, variation.null_as_default, variation_settings);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
            }
        }

        /// The count-only plan (empty header, all fields skipped) that `needOnlyCount` builds.
        try
        {
            const FormatSettings count_settings = AvroFuzzer::makeFormatSettings(AvroFuzzer::Variation(0));
            AvroRust::Plan count_plan(Block{}, schema, /* allow_missing_fields = */ true, count_settings.null_as_default, count_settings);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
#else
        const avro::ValidSchema schema = AvroFuzzer::compileSchema(schema_text);

        (void) AvroFuzzer::printSchema(schema, true);
        (void) AvroFuzzer::printSchema(schema, false);

        const avro::NodePtr root_node = schema.root();

        /// Inference of any node, as `AvroForIcebergDeserializer` does for the root of a manifest.
        DataTypePtr root_type;
        try
        {
            AvroSchemaReader::avroNodeToDataType(root_node, /* allow_nullable_tuple_type = */ false);
            root_type = AvroSchemaReader::avroNodeToDataType(root_node, /* allow_nullable_tuple_type = */ true);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Not every schema is representable. The plan is still built for those that are.
        }

        const FormatSettings format_settings = AvroFuzzer::makeFormatSettings(AvroFuzzer::Variation(0));

        /// `AvroDeserializer` keeps a reference to the settings, so they must outlive it.
        if (root_type)
        {
            try
            {
                AvroDeserializer iceberg_style_deserializer(root_type, root_node->name().fullname(), schema, true, true, format_settings);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
            }
        }

        /// The four variations cover: the identity header, `Nullable` everywhere with a column subset (skip functions),
        /// rotating scalar targets with missing fields allowed, and another rotation with `null_as_default`.
        for (UInt8 variation_byte : {UInt8(0), UInt8(4 | 16), UInt8(1 | 8), UInt8(2 | 1 | 12)})
        {
            const AvroFuzzer::Variation variation(variation_byte);
            const FormatSettings variation_settings = AvroFuzzer::makeFormatSettings(variation);
            try
            {
                const NamesAndTypesList structure = AvroFuzzer::inferStructure(schema, variation_settings);
                const Block header = AvroFuzzer::makeHeader(structure, variation);
                if (header.columns() == 0)
                    continue;
                AvroDeserializer deserializer(
                    header, schema, variation.allow_missing_fields, variation.null_as_default, variation_settings);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
            }
        }
#endif
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: malformed input is expected to throw.
    }

    return 0;
}
