#include <cstddef>
#include <cstdint>

#include <Processors/Formats/fuzzers/AvroFuzzerCommon.h>

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

#include <IO/ReadBufferFromMemory.h>

#include <Interpreters/Context.h>

using namespace DB;

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

/** Fuzzes the `Avro` input format on the bytes of an Object Container File.
  *
  * The input is the variation byte (see `AvroFuzzer::Variation`) followed by the bytes of the file. The
  * structure is inferred from the file with `AvroSchemaReader`, as `DESCRIBE`, `file(...)` and `s3(...)`
  * do, and the file is read to the end through `Context::getInputFormat("Avro", ...)`, as `format_fuzzer`
  * does. The variation byte changes:
  *   - `input_format_avro_allow_missing_fields` and `input_format_null_as_default`,
  *   - the target types of the header (inferred, `Nullable`, rotating scalar targets),
  *   - whether half of the columns are not requested (the skip functions),
  *   - `needOnlyCount` (`AvroRowInputFormat::countRows`),
  *   - `max_block_size`.
  *
  * Files whose schema cannot be inferred are not read: they cannot be reached with an inferred structure.
  * `avro_schema_fuzzer` still covers the schemas that inference rejects.
  *
  * Compile the code as follows:
  *   mkdir build_asan_fuzz
  *   cd build_asan_fuzz
  *   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
  *   ninja avro_ocf_fuzzer
  *
  * The fuzzer can be run as follows:
  *   ../../../build_asan_fuzz/src/Processors/Formats/fuzzers/avro_ocf_fuzzer corpus -jobs=64 -rss_limit_mb=8192 \
  *       -dict=../../../tests/fuzz/avro.dict
  *
  * The seed corpus is `tests/fuzz/avro_ocf_fuzzer.in`.
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

        /// One byte of variation and at least the magic of a file.
        if (size < 1 + 4)
            return 0;

        const AvroFuzzer::Variation variation(data[0]);
        const FormatSettings format_settings = AvroFuzzer::makeFormatSettings(variation);

        NamesAndTypesList structure;
        {
            ReadBufferFromMemory schema_in(data + 1, size - 1);
            AvroSchemaReader schema_reader(schema_in, /* confluent_ = */ false, format_settings);
            structure = schema_reader.readSchema();
        }

        const Block header = AvroFuzzer::makeHeader(structure, variation);

        /// The table of a query always has a column; an empty record is only inferred as no columns at all.
        if (header.columns() == 0)
            return 0;

        ReadBufferFromMemory in(data + 1, size - 1);
        InputFormatPtr input_format = AvroFuzzer::getContext()->getInputFormat("Avro", in, header, variation.max_block_size, format_settings);
        AvroFuzzer::readAll(std::move(input_format), variation.only_count);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: malformed input is expected to throw.
    }

    return 0;
}
