#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>

#if USE_AVRO_RUST

#include <base/unaligned.h>
#include <Columns/IColumn.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/ParseError.h>
#include <IO/ReadHelpers.h>
#include <IO/WithFileSize.h>
#include <Processors/Formats/Impl/AvroConfluentSchemaRegistry.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <algorithm>
#include <cstring>
#include <limits>
#include <span>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int AVRO_EXCEPTION;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_WRITE_TO_SOCKET;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_END_OF_FILE;
}

namespace
{

/// Attribute introspection retains the Iceberg ids that the C++ schema compiler discarded.
NameSet columnsAbsentFromFile(const Block & header, const ColumnMapper & mapper, const AvroRust::Schema & schema)
{
    auto root = schema.root();
    if (root.kind() != AVRO_KIND_RECORD || !root.childCount())
        return {};
    for (UInt32 i = 0; i < root.childCount(); ++i)
    {
        auto id = root.attr("field-id", i);
        if (!id || id->kind != AVRO_ATTR_INT)
            return {};
    }

    UnorderedSetWithMemoryTracking<Int64> file_ids;
    for (UInt32 i = 0; i < schema.nodeCount(); ++i)
    {
        auto node = schema.node(i);
        for (std::string_view key : {"field-id", "element-id", "key-id", "value-id"})
        {
            if (auto value = node.attr(key); value && value->kind == AVRO_ATTR_INT)
                file_ids.insert(value->int_value);
        }
        if (node.kind() == AVRO_KIND_RECORD)
        {
            for (UInt32 field = 0; field < node.childCount(); ++field)
                if (auto value = node.attr("field-id", field); value && value->kind == AVRO_ATTR_INT)
                    file_ids.insert(value->int_value);
        }
    }
    NameSet result;
    const auto & encoding = mapper.getStorageColumnEncoding();
    for (const auto & column : header)
    {
        auto id = encoding.find(column.name);
        if (id != encoding.end() && !file_ids.contains(id->second))
            result.insert(column.name);
    }
    return result;
}

/// Iceberg row lineage columns are stored only by a rewritten data file: a file without them reads them as NULL.
void addRowLineageColumnsAbsentFromFile(NameSet & absent, const Block & header, const ColumnMapper & mapper, const AvroRust::Schema & schema)
{
    const auto root = schema.root();
    for (const auto * name : {"_row_id", "_last_updated_sequence_number"})
    {
        if (!header.has(name) || mapper.getStorageColumnEncoding().contains(name))
            continue;
        bool in_file = false;
        for (UInt32 i = 0; i < root.childCount() && !in_file; ++i)
            in_file = root.fieldName(i) == name;
        if (!in_file)
            absent.insert(name);
    }
}

/// The same set as `isConnectionError` in `IRowInputFormat.cpp`, which is private to that file.
bool isConnectionError(int code)
{
    return code == ErrorCodes::SOCKET_TIMEOUT || code == ErrorCodes::NETWORK_ERROR || code == ErrorCodes::CANNOT_READ_FROM_SOCKET
        || code == ErrorCodes::CANNOT_WRITE_TO_SOCKET || code == ErrorCodes::UNEXPECTED_END_OF_FILE;
}

size_t columnsByteSize(const MutableColumns & columns)
{
    size_t result = 0;
    for (const auto & column : columns)
        result += column->byteSize();
    return result;
}

/// The largest datum of one AvroConfluent message that is accepted, whether the whole datum is inside one buffer of the
/// input or is split between buffers. A message that is not complete yet is buffered in memory (`carry`, together with
/// the bytes read ahead of it), so the limit cannot be lifted. It is internal on purpose: no format setting applies
/// (`format_binary_max_string_size` limits one `RowBinary` string, not a message of another format).
constexpr size_t MAX_CONFLUENT_DATUM_BYTES = 1ULL << 30;

[[noreturn]] void throwConfluentDatumTooLarge()
{
    throw Exception(
        ErrorCodes::INCORRECT_DATA,
        "An AvroConfluent message is larger than {} bytes. Most likely, data is corrupted or format mismatch occurred",
        MAX_CONFLUENT_DATUM_BYTES);
}

}

AvroRowInputFormat::AvroRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_)
    , params(params_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
{
}

void AvroRowInputFormat::readPrefix()
{
    file_reader_ptr = std::make_unique<AvroRust::OcfReader>(*in);
    NameSet absent;
    if (column_mapper)
    {
        absent = columnsAbsentFromFile(output.getHeader(), *column_mapper, file_reader_ptr->schema());
        addRowLineageColumnsAbsentFromFile(absent, output.getHeader(), *column_mapper, file_reader_ptr->schema());
    }
    if (need_only_count)
    {
        /// Preserve schema/header validation even though the count program has no output sinks.
        AvroRust::Plan validation(
            output.getHeader(), file_reader_ptr->schema(), format_settings.avro.allow_missing_fields,
            format_settings.null_as_default, format_settings, absent);
        /// The all-skip plan is used when compressed blocks still require payload validation.
        deserializer_ptr = std::make_unique<AvroRust::Plan>(
            Block{}, file_reader_ptr->schema(), true, format_settings.null_as_default, format_settings, absent);
    }
    else
    {
        deserializer_ptr = std::make_unique<AvroRust::Plan>(
            output.getHeader(), file_reader_ptr->schema(), format_settings.avro.allow_missing_fields,
            format_settings.null_as_default, format_settings, absent);
    }
}

void AvroRowInputFormat::resetParser()
{
    IInputFormat::resetParser();
    deserializer_ptr.reset();
    file_reader_ptr.reset();
    total_rows = 0;
    approx_bytes_read_for_chunk = 0;
    block_missing_values.clear();
    got_connection_exception = false;
}

Chunk AvroRowInputFormat::read()
{
    if (got_connection_exception)
        return {};

    if (!file_reader_ptr)
    {
        try
        {
            readPrefix();
        }
        catch (Exception & e)
        {
            /// The decoder over `contrib/avro` throws `avro::Exception`, which is not a `DB::Exception`, so `IRowInputFormat`
            /// never annotates what the library finds wrong with the header. The messages must stay the same: a reader of
            /// `system.kafka_consumers` or of a log compares them.
            if (e.code() != ErrorCodes::AVRO_EXCEPTION)
                e.addMessage("(while reading header)");
            throw;
        }
    }

    const size_t first_row = total_rows;
    const size_t start_offset = getDataOffsetMaybeCompressed(getReadBuffer());
    auto columns = need_only_count ? MutableColumns{} : output.getHeader().cloneEmptyColumns();
    block_missing_values.clear();
    size_t rows = 0;
    size_t bytes = 0;
    const bool count_bytes = params.min_block_size_bytes || params.max_block_size_bytes;
    /// `byteSize` of the columns after the last batch; `bytes` is the growth since the start of the chunk.
    size_t columns_bytes = count_bytes ? columnsByteSize(columns) : 0;
    /// The largest bytes-per-row average of any batch of this chunk, to size the next batch so that it does not step over `max_block_size_bytes`.
    UInt64 peak_row_bytes = 0;
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    auto below_min = [&]
    {
        return (!params.min_block_size_rows && !params.min_block_size_bytes)
            || rows < params.min_block_size_rows || bytes < params.min_block_size_bytes;
    };
    auto below_max = [&]
    {
        return (!params.max_block_size_rows || rows < params.max_block_size_rows)
            && (!params.max_block_size_bytes || bytes < params.max_block_size_bytes);
    };
    /// How many rows may be decoded in one call without stepping over a byte threshold by more than the estimate's error.
    /// Byte thresholds are not row-by-row decisions: the rows are measured per batch and the next batch is sized from them.
    auto rows_within_byte_thresholds = [&]() -> UInt64
    {
        UInt64 limit = std::numeric_limits<UInt64>::max();
        if (params.max_block_size_bytes)
        {
            /// Nothing is known about the size of a row yet: measure one.
            if (!rows)
                return 1;
            limit = std::max<UInt64>(1, (params.max_block_size_bytes - bytes) / std::max<UInt64>(1, peak_row_bytes));
        }
        if (params.min_block_size_bytes && rows && bytes < params.min_block_size_bytes)
        {
            const UInt64 average_row_bytes = std::max<UInt64>(1, bytes / rows);
            limit = std::min<UInt64>(limit, (params.min_block_size_bytes - bytes + average_row_bytes - 1) / average_row_bytes);
        }
        return limit;
    };

    try
    {
        while (rows == 0 || (below_min() && below_max()))
        {
            CurrentThread::checkIfNotCancelled();
            if (params.max_block_wait_ms && rows)
            {
                auto elapsed = watch.elapsedMilliseconds();
                if (elapsed >= params.max_block_wait_ms
                    || !getReadBuffer().poll((params.max_block_wait_ms - elapsed) * 1000))
                    break;
            }

            if (!file_reader_ptr->rowsLeft())
            {
                if (!file_reader_ptr->nextBlock())
                    break;
                if (!file_reader_ptr->rowsLeft())
                    continue;
            }

            UInt64 batch_rows = std::min<UInt64>(file_reader_ptr->rowsLeft(), CANCELLATION_CHECK_PERIOD_ROWS);
            if (params.max_block_size_rows)
                batch_rows = std::min<UInt64>(batch_rows, params.max_block_size_rows - rows);
            if (params.min_block_size_rows && rows < params.min_block_size_rows)
                batch_rows = std::min<UInt64>(batch_rows, params.min_block_size_rows - rows);
            if (params.max_block_wait_ms)
            {
                /// The only case where a row, not a batch, is the unit: the block must end by the clock after any row.
                batch_rows = 1;
            }
            else if (count_bytes)
                batch_rows = std::min(batch_rows, rows_within_byte_thresholds());
            try
            {
                if (!need_only_count || !file_reader_ptr->canCountRows())
                {
                    file_reader_ptr->decodeRows(*deserializer_ptr, batch_rows, columns, missing_scratch);
                    /// A column that the plan never leaves without a value has no flags at all.
                    for (size_t column = 0; column < missing_scratch.size(); ++column)
                        for (size_t row = 0; row < missing_scratch[column].size(); ++row)
                            if (missing_scratch[column][row])
                                block_missing_values.setBit(column, rows + row);
                }
                else
                    file_reader_ptr->discardRows(batch_rows);
            }
            catch (Exception & e)
            {
                if (isParseError(e.code()))
                {
                    if (params.allow_errors_num != 0 || params.allow_errors_ratio != 0)
                        e.addMessage("(Input format doesn't allow to skip errors)");
                    e.addMessage("(at row " + std::to_string(total_rows + file_reader_ptr->committedRows(*deserializer_ptr) + 1) + ")\n");
                }
                else if (params.connection_handling && isConnectionError(e.code()))
                {
                    /// The input broke while a later window of the batch was being read: the rows that the earlier windows
                    /// decoded are in the columns, committed, and are kept like the rows of the batches before.
                    const UInt64 committed = file_reader_ptr->committedRows(*deserializer_ptr);
                    for (size_t column = 0; column < missing_scratch.size(); ++column)
                        for (size_t row = 0; row < std::min<size_t>(committed, missing_scratch[column].size()); ++row)
                            if (missing_scratch[column][row])
                                block_missing_values.setBit(column, rows + row);
                    rows += committed;
                    total_rows += committed;
                }
                throw;
            }

            if (count_bytes)
            {
                const size_t columns_bytes_now = columnsByteSize(columns);
                const UInt64 batch_bytes = columns_bytes_now > columns_bytes ? columns_bytes_now - columns_bytes : 0;
                columns_bytes = columns_bytes_now;
                bytes += batch_bytes;
                peak_row_bytes = std::max<UInt64>(peak_row_bytes, (batch_bytes + batch_rows - 1) / batch_rows);
            }
            rows += batch_rows;
            total_rows += batch_rows;
            if (params.max_block_wait_ms && watch.elapsedMilliseconds() >= params.max_block_wait_ms)
                break;
        }
    }
    catch (Exception & e)
    {
        /// As `IRowInputFormat::read`: keep what was committed and end the input. A connection error comes from reading
        /// `in` (`nextBlock`, `poll`, or the next window of a batch), and the columns then hold committed rows only.
        if (!params.connection_handling || !isConnectionError(e.code()))
            throw;
        got_connection_exception = true;
    }

    approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(getReadBuffer()) - start_offset;
    if (!rows)
        return {};
    if (need_only_count)
    {
        auto chunk = getChunkForCount(rows);
        chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(first_row));
        return chunk;
    }
    for (auto & column : columns)
        column->finalize();
    Chunk chunk(std::move(columns), rows);
    chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(first_row));
    return chunk;
}

AvroConfluentRowInputFormat::AvroConfluentRowInputFormat(
    SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(std::move(header_), in_, params_)
    , schema_registry(getConfluentSchemaRegistry(format_settings_))
    , format_settings(format_settings_)
{
}

void AvroConfluentRowInputFormat::readPrefix()
{
}

/// A message can be cut by the end of the buffer of `in`, and `in` is refilled in chunks of whatever size its source
/// delivers, down to one byte. The executor keeps no state inside a row, so an incomplete datum can only be decoded
/// again from its start.
///
/// Fast path (zero copy): the whole datum is inside the current buffer of `in`. It is decoded there and `in` is
/// advanced by exactly the bytes of the datum.
///
/// Slow path: the buffer ends inside the datum. The bytes go into `carry` (once each; `in` is advanced past them)
/// and the datum is decoded again from its start, under a work budget, and once more when `in` has no more bytes.
///
/// Budget. Let `u_i` be the unread bytes presented to the i-th decode attempt on this datum (the direct attempt on the
/// buffer of `in` is the first) and `S_i = u_1 + ... + u_i` the cumulative bytes presented. After a failed attempt
/// the next one waits until, with `K = 4` (`work_budget_factor`),
///     S_{i-1} + u_i <= K * u_i,  i.e.  u_i >= ceil(S_{i-1} / (K - 1)),   and also  u_i > u_{i-1}.
/// So every budgeted attempt ends with `S_i <= K * u_i`. A refill that grows the datum by a few percent is decoded
/// at once, while the first refills are cheap relative to what was already scanned: with `u_1 = 1000` and refills
/// of 100 bytes, attempts happen at 1000, 1100, 1200 and 1300 bytes, and only then must the size grow by `K/(K-1)`
/// per attempt, which is geometric. Under one-byte fragmentation the attempts are then spaced geometrically.
/// The exception is the final attempt when `in` has no more bytes (`more_input == false`): it is always made, once,
/// and throws on an incomplete datum, and it scans at most `u_n` more bytes.
///
/// The work is linear in the bytes received, whatever the chunk sizes:
///  * An attempt costs O(its bytes) (a failing attempt has fewer than `d` bytes, the succeeding one stops at the end
///    of the datum of `d` bytes). All attempts but the final EOF one obey `S_i <= K * u_i`, and `u_i` is at most the
///    bytes accumulated `A`, so the scan is `<= K * A`; with the final attempt, `<= (K + 1) * A`, at most 5 times
///    the bytes accumulated. The attempt that stops at `max_carry` is the last one (it throws or succeeds), so
///    capping the wanted size at `max_carry` keeps the same bound.
///  * Each byte is copied into `carry` once, and moved inside `carry` at most once: when a row needs more input for the
///    first time, the consumed front of `carry` is dropped by moving the rest, which at that moment is only the prefix
///    of that row's datum (`carry_begin` does not change again until the row ends). `carry` grows geometrically.
///  * Memory is capped before it grows: `carry` never holds more than `max_carry` bytes (`carry.size()` and its
///    capacity), a datum that needs more fails, and the largest request is `retry_at <= max_carry`. The sums and the
///    growth below are saturating or checked, so `2 * size` and `S_i` cannot overflow.
///
/// `max_carry` is `MAX_CONFLUENT_DATUM_BYTES`, a constant that no setting changes. A datum larger than it is rejected
/// with `INCORRECT_DATA` in both paths: a contiguous datum is checked by the bytes it consumed, a split one by the
/// bytes `carry` would have to hold. So the outcome does not depend on how the input is cut into buffers.
///
/// Over-read: bytes are taken whole chunk by chunk (never past `max_carry`) until the wanted size is reached, so the
/// last chunk can bring bytes of the next message(s). `in` has been advanced past them and is never rewound, so
/// `carry` owns them: they are the oldest unread bytes of the stream, and the next `readRow` parses its schema id and
/// its datum from them before it looks at `in`. Only when `carry` is used up does a row go back to the fast path.
bool AvroConfluentRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    /// `in` is not consulted while retained bytes last.
    if (carry_begin == carry.size() && in->eof())
        return false;
    auto & plan = getOrCreateDeserializer(readSchemaId());
    auto & missing = missing_values;
    constexpr size_t max_carry = MAX_CONFLUENT_DATUM_BYTES;

    /// True: the datum is decoded in place in the buffer of `in`. False: from the unread part of `carry`.
    bool direct = carry_begin == carry.size();
    if (direct)
    {
        carry.clear();
        carry_begin = 0;
    }
    /// False once `in` has no more bytes: an incomplete datum is then an error, thrown by `decodeDatum`.
    bool more_input = true;
    /// Cumulative bytes presented to `decodeDatum` for this datum by the failed attempts, saturating.
    size_t scanned = 0;
    /// How many times the bytes of the previous attempts can be presented again: see the proof above.
    constexpr size_t work_budget_factor = 4;

    /// Appends `n` bytes of `data`, a part of the buffer of `in`, to `carry` and advances `in` past them.
    /// `carry.size() + n <= max_carry` always holds.
    auto take = [&](const UInt8 * data, size_t n)
    {
        const size_t required = carry.size() + n;
        if (required > max_carry)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AvroConfluent carry of {} bytes exceeds the limit of {}", required, max_carry);
        if (required > carry.capacity())
        {
            const size_t capacity = carry.capacity();
            const size_t doubled = capacity > max_carry / 2 ? max_carry : 2 * capacity;
            carry.reserve_exact(std::max(required, doubled));
        }
        carry.insert(data, data + n);
        in->position() += n;
    };

    while (true)
    {
        const std::span<const UInt8> window = direct
            ? std::span<const UInt8>(reinterpret_cast<const UInt8 *>(in->position()), in->available())
            : std::span<const UInt8>(carry.data() + carry_begin, carry.size() - carry_begin);
        const auto result = plan.decodeDatum(window, more_input, columns, missing);
        if (result.complete)
        {
            if (result.consumed > window.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "An AvroConfluent datum of {} bytes was decoded from {} bytes", result.consumed, window.size());
            /// The same limit applies when the datum is contiguous (`window` is the whole buffer of `in`) and when it is not.
            if (result.consumed > max_carry)
                throwConfluentDatumTooLarge();
            if (direct)
                in->position() += result.consumed;
            else
                carry_begin += result.consumed;
            break;
        }

        /// The datum extends beyond the `unread` bytes seen so far. Without `more_input`, `decodeDatum` throws instead.
        if (!more_input)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "An AvroConfluent datum is incomplete after the end of the input but no decode error was reported");
        const size_t unread = window.size();
        if (unread >= max_carry)
            throwConfluentDatumTooLarge();

        if (direct)
        {
            /// The whole buffer of `in`, which failed above, so `unread` bytes of datum and nothing after them.
            take(window.data(), unread);
            direct = false;
        }
        else if (carry_begin)
        {
            std::memmove(carry.data(), carry.data() + carry_begin, unread);
            carry.resize_assume_reserved(unread);
            carry_begin = 0;
        }

        /// The next attempt waits for the bytes the budget allows: more than this one, and at least
        /// `scanned / (work_budget_factor - 1)` rounded up, so that `scanned + size <= work_budget_factor * size`.
        scanned = scanned > std::numeric_limits<size_t>::max() - unread ? std::numeric_limits<size_t>::max() : scanned + unread;
        const size_t budget_size = scanned / (work_budget_factor - 1) + (scanned % (work_budget_factor - 1) ? 1 : 0);
        const size_t retry_at = std::min(max_carry, std::max(unread + 1, budget_size)); /// `unread < max_carry`
        while (carry.size() < retry_at)
        {
            if (in->eof())
            {
                more_input = false; /// The final attempt is made whatever the budget.
                break;
            }
            take(reinterpret_cast<const UInt8 *>(in->position()), std::min(in->available(), max_carry - carry.size()));
        }
    }

    ext.read_columns.assign(columns.size(), true);
    for (size_t column = 0; column < missing.size(); ++column)
        if (!missing[column].empty())
            ext.read_columns[column] = !missing[column][0];
    return true;
}

/// The magic byte and the big-endian schema id. The retained bytes come first, then `in`.
AvroConfluentRowInputFormat::SchemaId AvroConfluentRowInputFormat::readSchemaId()
{
    constexpr size_t header_size = 1 + sizeof(SchemaId);
    char header[header_size];
    const size_t retained = std::min(header_size, carry.size() - carry_begin);
    std::copy_n(reinterpret_cast<const char *>(carry.data() + carry_begin), retained, header);
    carry_begin += retained;
    const size_t got = retained + (retained < header_size ? in->read(header + retained, header_size - retained) : 0);
    if (got < header_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing AvroConfluent magic byte or schema identifier.");
    const UInt8 magic = static_cast<UInt8>(header[0]);
    if (magic)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Invalid magic byte before AvroConfluent schema identifier. Must be zero byte, found {} instead", int(magic));
    return unalignedLoadBigEndian<SchemaId>(header + 1);
}

void AvroConfluentRowInputFormat::dropCarry()
{
    carry.clear();
    carry_begin = 0;
}

void AvroConfluentRowInputFormat::resetParser()
{
    dropCarry();
    IRowInputFormat::resetParser();
}

void AvroConfluentRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    dropCarry();
    IRowInputFormat::setReadBuffer(in_);
}

void AvroConfluentRowInputFormat::syncAfterError()
{
    /// A message cannot be found again after an error: what is buffered is skipped, the retained bytes too.
    dropCarry();
    in->tryIgnore(in->available());
}

AvroRust::Plan & AvroConfluentRowInputFormat::getOrCreateDeserializer(SchemaId schema_id)
{
    auto it = deserializer_cache.find(schema_id);
    if (it == deserializer_cache.end())
    {
        auto schema = schema_registry->getSchema(
            schema_id, format_settings.avro.schema_registry_timeouts, format_settings.avro.schema_registry_retry);
        auto plan = std::make_unique<AvroRust::Plan>(output.getHeader(), *schema,
            format_settings.avro.allow_missing_fields, format_settings.null_as_default, format_settings);
        it = deserializer_cache.emplace(schema_id, std::move(plan)).first;
    }
    return *it->second;
}

}

#endif
