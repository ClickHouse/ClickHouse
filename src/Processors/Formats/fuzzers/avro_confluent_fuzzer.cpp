#include <Common/VectorWithMemoryTracking.h>
#include <cstddef>
#include <cstdint>

#include <Columns/IColumn.h>
#include <Processors/Formats/fuzzers/AvroFuzzerCommon.h>

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

#include <Common/Exception.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#if !USE_AVRO_RUST
#include <Decoder.hh>
#endif

#include <algorithm>
#include <exception>
#include <optional>
#include <span>
#include <string>
#include <vector>

using namespace DB;

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

#if USE_AVRO_RUST
/// Messages decoded per input, so that a long input of tiny messages cannot dominate the run time.
constexpr size_t MAX_MESSAGES = 4096;

/// Messages per input (the first ones) whose datum is also decoded as growing prefixes, see `probeFragmentedDatum`.
constexpr size_t MAX_PROBED_MESSAGES = 256;

/// The most prefix lengths `prefixSchedule` returns: 8 short ones, 12 powers of two, 3 around the end of the datum and
/// the whole window.
constexpr size_t MAX_PREFIXES = 24;
#endif

/// The same checks as `readConfluentSchemaId` in `AvroRowInputFormat.cpp`, which is `static` there.
uint32_t readConfluentSchemaId(ReadBuffer & in)
{
    uint8_t magic = 0;
    uint32_t schema_id = 0;

    try
    {
        readBinaryBigEndian(magic, in);
        readBinaryBigEndian(schema_id, in);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing AvroConfluent magic byte or schema identifier.");
        throw;
    }

    if (magic != 0x00)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid magic byte before AvroConfluent schema identifier. Must be zero byte, found {} instead", int(magic));

    return schema_id;
}

#if USE_AVRO_RUST
/// The prefix lengths of a window of `window_size` bytes at which `decodeDatum` is asked for the datum, in increasing
/// order, at most `MAX_PREFIXES` of them whatever the size of the window:
///  - 0..7: the split of the first bytes (a varint of the first field, the length of a string, a block count);
///  - 16, 32, ... (at most 12 powers of two): the split in the middle of strings, arrays and maps, with a number of
///    attempts that is logarithmic in the size of the window;
///  - `datum_size - 1`, `datum_size`, `datum_size + 1` when `datum_size` (the bytes the datum occupies, known from
///    the decode of the whole window) is known: the last incomplete prefix, the first complete one and one byte of
///    the next frame;
///  - the whole window.
VectorWithMemoryTracking<size_t> prefixSchedule(size_t window_size, std::optional<size_t> datum_size)
{
    VectorWithMemoryTracking<size_t> lengths;
    lengths.reserve(MAX_PREFIXES);

    for (size_t length = 0; length < 8; ++length)
        lengths.push_back(length);

    size_t length = 16;
    for (size_t powers = 0; powers < 12 && length <= window_size; ++powers, length *= 2)
        lengths.push_back(length);

    if (datum_size)
    {
        if (*datum_size > 0)
            lengths.push_back(*datum_size - 1);
        lengths.push_back(*datum_size);
        lengths.push_back(*datum_size + 1);
    }

    lengths.push_back(window_size);

    std::sort(lengths.begin(), lengths.end());
    lengths.erase(std::unique(lengths.begin(), lengths.end()), lengths.end());
    lengths.erase(std::lower_bound(lengths.begin(), lengths.end(), window_size + 1), lengths.end());
    return lengths;
}

VectorWithMemoryTracking<size_t> columnSizes(const MutableColumns & columns)
{
    VectorWithMemoryTracking<size_t> sizes;
    sizes.reserve(columns.size());
    for (const auto & column : columns)
        sizes.push_back(column->size());
    return sizes;
}

/// A violation of the contract of `Plan::decodeDatum` must not be an exception (the fuzzer swallows those, as it does
/// for malformed input): the process aborts so that the input is reported as a crash.
void requireInvariant(bool condition, const char * description)
{
    if (!condition)
        abortOnFailedAssertion(std::string("avro_confluent_fuzzer: ") + description);
}

/// The incomplete-input contract of `Plan::decodeDatum` over a `window` that starts at one datum and may continue
/// with the next frames. `oracle_consumed` is what `Plan::decode(window, 1, ...)` returned (the bytes of the datum),
/// or `std::nullopt` when it threw. `oracle_missing` is its `missing`.
///
/// The datum is first decoded from the prefixes of `prefixSchedule`, with `more_input = true`, and the probing stops
/// at the first prefix that completes it. The executor is deterministic and reads the bytes of a datum in order, so:
///  - an incomplete prefix leaves every destination column at its size, reports `complete == false`, `consumed == 0`
///    and no committed row, whether it returns or throws;
///  - a prefix shorter than the datum is never complete and, if the datum is valid, never an error (it is reported as
///    incomplete);
///  - a prefix that is not shorter than the datum is complete (the datum may end before the end of the window: the
///    rest is the next frame, which is not consumed), with the same `consumed` and `missing` as `decode`, and one row
///    added to every column;
///  - if `decode` failed, no prefix completes, because it would be a valid datum for `decode` as well.
/// Then the datum is decoded once more at the whole window with `more_input = false`, where the incomplete input is an
/// error: it must behave as `decode` (the same `consumed` and `missing`, or an error).
///
/// Extra decodes per message: at most `MAX_PREFIXES` with `more_input = true` plus one with `more_input = false`, each
/// over a prefix of the window; a decode stops at the end of the datum, so each costs at most about that of `decode`.
void probeFragmentedDatum(
    AvroRust::Plan & plan,
    std::span<const UInt8> window,
    std::optional<size_t> oracle_consumed,
    const VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> & oracle_missing,
    MutableColumns & columns,
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> & missing)
{
    for (const size_t length : prefixSchedule(window.size(), oracle_consumed))
    {
        const VectorWithMemoryTracking<size_t> sizes = columnSizes(columns);
        AvroRust::Plan::DatumResult result;
        try
        {
            result = plan.decodeDatum(window.first(length), true, columns, missing);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                throw;
            requireInvariant(columnSizes(columns) == sizes, "a decode error of a prefix changed a destination column");
            requireInvariant(!oracle_consumed, "a prefix of a datum that decodes was rejected with a decode error");
            return;
        }

        if (!result.complete)
        {
            requireInvariant(columnSizes(columns) == sizes, "an incomplete prefix changed a destination column");
            requireInvariant(result.consumed == 0, "an incomplete prefix reported consumed bytes");
            requireInvariant(plan.committedRows() == 0, "an incomplete prefix committed a row");
            requireInvariant(!oracle_consumed || length < *oracle_consumed, "a prefix that contains the datum is incomplete");
            continue;
        }

        requireInvariant(oracle_consumed.has_value(), "a prefix completed a datum that the whole window rejects");
        requireInvariant(length >= *oracle_consumed, "a prefix shorter than the datum is complete");
        requireInvariant(result.consumed == *oracle_consumed, "a prefix completed the datum with other consumed bytes than the whole window");
        requireInvariant(plan.committedRows() == 1, "a complete prefix did not commit exactly one row");
        requireInvariant(missing == oracle_missing, "a prefix completed the datum with another `missing` than the whole window");
        const VectorWithMemoryTracking<size_t> sizes_after = columnSizes(columns);
        for (size_t i = 0; i < sizes.size(); ++i)
            requireInvariant(sizes_after[i] == sizes[i] + 1, "a complete prefix did not add one row to every column");
        break;
    }

    const VectorWithMemoryTracking<size_t> sizes = columnSizes(columns);
    try
    {
        const auto result = plan.decodeDatum(window, false, columns, missing);
        requireInvariant(result.complete, "a decode without more input reported an incomplete datum instead of an error");
        requireInvariant(oracle_consumed.has_value(), "a decode without more input completed a datum that `decode` rejects");
        requireInvariant(result.consumed == *oracle_consumed, "a decode without more input consumed other bytes than `decode`");
        requireInvariant(missing == oracle_missing, "a decode without more input produced another `missing` than `decode`");
        const VectorWithMemoryTracking<size_t> sizes_after = columnSizes(columns);
        for (size_t i = 0; i < sizes.size(); ++i)
            requireInvariant(sizes_after[i] == sizes[i] + 1, "a decode without more input did not add one row to every column");
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            throw;
        requireInvariant(columnSizes(columns) == sizes, "a decode error without more input changed a destination column");
        requireInvariant(!oracle_consumed, "a decode without more input rejected a datum that `decode` accepts");
    }
}
#endif

}

/** Fuzzes the decoding of Confluent-framed Avro messages (the `AvroConfluent` format, i.e. Kafka).
  *
  * `AvroConfluentRowInputFormat` cannot be driven without a registry: it fetches the schema of every unseen
  * schema id through `ConfluentSchemaRegistry::getSchema`, whose cache is private and filled only by an HTTP
  * request to `format_avro_schema_registry_url`, and the format has no other constructor argument that
  * injects a schema. The fuzzers do not use the network, so this target runs the same steps by hand instead:
  * the registry is replaced by "every schema id maps to the schema of the input".
  *
  * With `USE_AVRO_RUST` the schema is compiled by the production `AvroRust::Schema`, one `AvroRust::Plan` is built
  * for the header and every message is decoded by `Plan::decode` with `max_rows = 1` over the unread bytes of the
  * buffer, as `AvroConfluentRowInputFormat::readRow` does. The framing check is a copy and the split-datum
  * retry of `readRow` is not replayed (the input is one contiguous window); the rest is the production code. At most
  * `MAX_MESSAGES` messages are decoded per input.
  *
  * What that retry relies on, the incomplete-input contract of `Plan::decodeDatum`, is fuzzed instead: for the first
  * `MAX_PROBED_MESSAGES` messages the datum is also decoded from a bounded schedule of growing prefixes of the window
  * (see `probeFragmentedDatum`) into separate destination columns. An incomplete prefix must leave the columns
  * unchanged, the first prefix that contains the datum must complete it with the result of `Plan::decode`, and the
  * decode of the whole window without more input must agree with `Plan::decode` too.
  *
  * Without `USE_AVRO_RUST` the schema is compiled with `AvroFuzzer::compileSchema` as `fetchSchema` does, and the loop
  * of `AvroConfluentRowInputFormat::readRow` (framing check, `AvroDeserializer::deserializeRow`,
  * `Decoder::drain`) is repeated verbatim over an `AvroInputStreamReadBufferAdapter` on a `ReadBuffer`.
  *
  * Input layout: variation byte (see `AvroFuzzer::Variation`), a little-endian `UInt16` length of the
  * schema, the text of the schema, and the rest is the stream of messages (`0x00`, big-endian `UInt32` schema id,
  * the binary encoding of one datum), so both the schema and the datums are fuzzed.
  *
  * Compile the code as follows:
  *   mkdir build_asan_fuzz
  *   cd build_asan_fuzz
  *   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
  *   ninja avro_confluent_fuzzer
  *
  * The fuzzer can be run as follows:
  *   ../../../build_asan_fuzz/src/Processors/Formats/fuzzers/avro_confluent_fuzzer corpus -jobs=64 -rss_limit_mb=8192 \
  *       -dict=../../../tests/fuzz/avro.dict
  *
  * The seed corpus is `tests/fuzz/avro_confluent_fuzzer.in`.
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

        /// Variation byte and the length of the schema.
        if (size < 3)
            return 0;

        const AvroFuzzer::Variation variation(data[0]);
        const size_t schema_size = std::min<size_t>(size_t(data[1]) | (size_t(data[2]) << 8), size - 3);
        const std::string schema_text(reinterpret_cast<const char *>(data + 3), schema_size);

        const FormatSettings format_settings = AvroFuzzer::makeFormatSettings(variation);

#if USE_AVRO_RUST
        const AvroRust::Schema schema(schema_text, MAX_AVRO_SCHEMA_DEPTH);

        const NamesAndTypesList structure = AvroFuzzer::inferStructure(schema, format_settings);
        const Block header = AvroFuzzer::makeHeader(structure, variation);

        /// The table of a query always has a column; an empty record is only inferred as no columns at all.
        if (header.columns() == 0)
            return 0;

        ReadBufferFromMemory in(data + 3 + schema_size, size - 3 - schema_size);

        /// What `AvroConfluentRowInputFormat::getOrCreateDeserializer` and `readRow` do: one plan per schema id
        /// (the registry maps every id to the schema of the input), one `decode` call per message over the
        /// unread bytes of the buffer.
        AvroRust::Plan plan(header, schema, format_settings.avro.allow_missing_fields, format_settings.null_as_default, format_settings);

        MutableColumns columns = header.cloneEmptyColumns();
        VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> missing;
        /// The destination of the decodes of `probeFragmentedDatum`: the real columns keep only what `decode` produced.
        MutableColumns probe_columns = header.cloneEmptyColumns();
        VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> probe_missing;
        size_t rows = 0;
        size_t messages = 0;
        while (!in.eof() && in.available() != 0 && messages++ < MAX_MESSAGES)
        {
            readConfluentSchemaId(in);
            const std::span<const UInt8> window(reinterpret_cast<const UInt8 *>(in.position()), in.available());
            plan.beginBlock();

            /// The decode of the whole window is the oracle of the decodes of the prefixes: its result (or its error)
            /// is kept until they have run, and then acts as it did before.
            std::optional<size_t> consumed;
            std::exception_ptr decode_error;
            try
            {
                consumed = plan.decode(window, 1, columns, missing);
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    throw;
                decode_error = std::current_exception();
            }

            if (messages <= MAX_PROBED_MESSAGES)
            {
                if (probe_columns.front()->size() >= variation.max_block_size)
                    probe_columns = header.cloneEmptyColumns();
                probeFragmentedDatum(plan, window, consumed, missing, probe_columns, probe_missing);
            }

            if (decode_error)
                std::rethrow_exception(decode_error);
            in.position() += *consumed;

            /// Like a block of `max_block_size` rows, then a fresh one.
            if (++rows >= variation.max_block_size)
            {
                columns = header.cloneEmptyColumns();
                rows = 0;
            }
        }
#else
        const avro::ValidSchema schema = AvroFuzzer::compileSchema(schema_text);

        const NamesAndTypesList structure = AvroFuzzer::inferStructure(schema, format_settings);
        const Block header = AvroFuzzer::makeHeader(structure, variation);

        /// The table of a query always has a column; an empty record is only inferred as no columns at all.
        if (header.columns() == 0)
            return 0;

        ReadBufferFromMemory in(data + 3 + schema_size, size - 3 - schema_size);

        /// What `AvroConfluentRowInputFormat` does in `readPrefix`, `getOrCreateDeserializer` and `readRow`.
        auto input_stream = std::make_unique<AvroInputStreamReadBufferAdapter>(in);
        avro::DecoderPtr decoder = avro::binaryDecoder();
        decoder->init(*input_stream);

        AvroDeserializer deserializer(header, schema, format_settings.avro.allow_missing_fields, format_settings.null_as_default, format_settings);

        MutableColumns columns = header.cloneEmptyColumns();
        RowReadExtension ext;
        size_t rows = 0;
        while (!in.eof() && in.available() != 0)
        {
            readConfluentSchemaId(in);
            deserializer.deserializeRow(columns, *decoder, ext);
            decoder->drain();

            /// Like a block of `max_block_size` rows, then a fresh one.
            if (++rows >= variation.max_block_size)
            {
                columns = header.cloneEmptyColumns();
                rows = 0;
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
