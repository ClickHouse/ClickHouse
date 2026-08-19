#include <Processors/Formats/Impl/VortexBlockInputFormat.h>

#if USE_VORTEX

#include <Core/Defines.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Set.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <shared_mutex>

#include <vortex_ffi.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

static constexpr auto VORTEX_MAGIC_BYTES = "VTXF";

/// The context of the read callback passed to the Rust vortex library. The callback runs on the
/// threads that run the IO queue of the runtime, from several of them at once when the reader was
/// opened with `io_concurrency > 1` (only done for thread-safe files, see `makeReaderOptions`).
struct VortexReadContext
{
    arrow::io::RandomAccessFile * file = nullptr;
    std::atomic<size_t> bytes_read{0};

    /// The first exception thrown by the callback. The library reports the failed read as an error
    /// of the scan, and the exception is rethrown in place of that error.
    std::mutex exception_mutex;
    std::exception_ptr exception;

    void setException(std::exception_ptr e)
    {
        std::lock_guard lock(exception_mutex);
        if (!exception)
            exception = std::move(e);
    }

    std::exception_ptr getException()
    {
        std::lock_guard lock(exception_mutex);
        return exception;
    }
};

extern "C" int32_t vortexFFIReadCallback(void * context, uint64_t offset, uint64_t length, uint8_t * out);
extern "C" int32_t vortexFFIReadCallback(void * context, uint64_t offset, uint64_t length, uint8_t * out)
{
    auto * ctx = static_cast<VortexReadContext *>(context);
    try
    {
        auto result = ctx->file->ReadAt(static_cast<int64_t>(offset), static_cast<int64_t>(length), out);
        if (!result.ok())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Vortex file: {}", result.status().ToString());
        if (*result != static_cast<int64_t>(length))
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Unexpected end of Vortex file: read {} bytes instead of {} at offset {}",
                *result, length, offset);
        ctx->bytes_read.fetch_add(length, std::memory_order_relaxed);
        return 0;
    }
    catch (...)
    {
        ctx->setException(std::current_exception());
        return 1;
    }
}

/// Takes the error message of a failed FFI call and frees it.
static String takeVortexError(char * error)
{
    String message = error ? String(error) : "unknown error";
    if (error)
        vortex_ffi_free_string(error);
    return message;
}

/// The exception for an error reported by the library: the exception thrown by the IO callback if
/// there was one (the library reports a failed read only as a generic error, and the callback's
/// exception is the real cause and carries the right error code), or an exception with the error
/// message returned by the library otherwise.
static std::exception_ptr
makeVortexException(const String & message, const std::exception_ptr & callback_exception, int code = ErrorCodes::INCORRECT_DATA)
{
    if (callback_exception)
        return callback_exception;
    return std::make_exception_ptr(Exception(code, "Error while reading Vortex file: {}", message));
}

/// Throws the exception for a failed FFI call, see `makeVortexException`. Frees the message.
[[noreturn]] static void
throwVortexError(char * error, const std::exception_ptr & callback_exception, int code = ErrorCodes::INCORRECT_DATA)
{
    std::rethrow_exception(makeVortexException(takeVortexError(error), callback_exception, code));
}

static void throwFromArrowStatusIfFailed(const arrow::Status & status)
{
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Error while reading Vortex file: {}", status.ToString());
}

namespace
{

/// Filter pushdown: the WHERE condition (`KeyCondition` built from the filter DAG) is translated
/// into a Vortex filter expression, which may reduce the rows decoded by selective queries.
/// Whole segments are not yet pruned by statistics.
///
/// The translation is best-effort: any part of the condition that cannot be translated exactly is
/// *weakened* so that the pushed filter always keeps a superset of the rows the query filter
/// keeps (ClickHouse re-applies the full WHERE on the returned rows). Weakening must respect
/// polarity: under an even number of NOTs a conjunct of an AND may be dropped, under an odd
/// number a disjunct of an OR may be dropped, and nothing else.

struct VortexExpressionDeleter
{
    void operator()(VortexFFIExpression * expression) const { vortex_ffi_expr_free(expression); }
};
using VortexExpressionPtr = std::unique_ptr<VortexFFIExpression, VortexExpressionDeleter>;

String getColumnNameFromKeyCondition(const KeyCondition & key_condition, size_t index)
{
    for (const auto & [name, i] : key_condition.getKeyColumns())
        if (i == index)
            return name;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get column with index {} from KeyCondition", index);
}

/// Returns true if the ClickHouse column type and the Vortex column type describe the same
/// values, so a comparison pushed into the scan behaves exactly like the one evaluated by
/// ClickHouse. Counter-example: `UInt64` in the header and `I64` in the file interpret the same
/// bits differently, so a pushed comparison could drop different rows.
bool typesMatchForFilterPushdown(const DataTypePtr & header_type, const arrow::DataType & arrow_type)
{
    WhichDataType which(removeNullable(recursiveRemoveLowCardinality(header_type)));
    switch (arrow_type.id())
    {
        case arrow::Type::INT8: return which.isInt8();
        case arrow::Type::INT16: return which.isInt16();
        case arrow::Type::INT32: return which.isInt32();
        case arrow::Type::INT64: return which.isInt64();
        case arrow::Type::UINT8: return which.isUInt8();
        case arrow::Type::UINT16: return which.isUInt16();
        case arrow::Type::UINT32: return which.isUInt32();
        case arrow::Type::UINT64: return which.isUInt64();
        case arrow::Type::FLOAT: return which.isFloat32();
        case arrow::Type::DOUBLE: return which.isFloat64();
        /// FixedString is excluded: its comparison semantics (zero padding) differ from Binary.
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::STRING_VIEW:
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::BINARY_VIEW:
            return which.isString();
        default:
            return false;
    }
}

/// Builds a Vortex literal with the exact type of the file column. Returns nullptr if the value
/// cannot be represented exactly in that type (the atom is then not pushed down).
VortexExpressionPtr makeVortexLiteral(const arrow::DataType & arrow_type, const Field & field)
{
    auto make_int = [&](VortexFFIPType ptype) -> VortexExpressionPtr
    {
        if (field.getType() == Field::Types::Int64)
            return VortexExpressionPtr(vortex_ffi_expr_literal_int(ptype, field.safeGet<Int64>()));
        if (field.getType() == Field::Types::UInt64)
        {
            UInt64 value = field.safeGet<UInt64>();
            if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return nullptr;
            return VortexExpressionPtr(vortex_ffi_expr_literal_int(ptype, static_cast<Int64>(value)));
        }
        return nullptr;
    };

    auto make_uint = [&](VortexFFIPType ptype) -> VortexExpressionPtr
    {
        if (field.getType() == Field::Types::UInt64)
            return VortexExpressionPtr(vortex_ffi_expr_literal_uint(ptype, field.safeGet<UInt64>()));
        if (field.getType() == Field::Types::Int64)
        {
            Int64 value = field.safeGet<Int64>();
            if (value < 0)
                return nullptr;
            return VortexExpressionPtr(vortex_ffi_expr_literal_uint(ptype, static_cast<UInt64>(value)));
        }
        return nullptr;
    };

    auto make_float = [&](VortexFFIPType ptype) -> VortexExpressionPtr
    {
        if (field.getType() != Field::Types::Float64)
            return nullptr;
        Float64 value = field.safeGet<Float64>();
        if (!std::isfinite(value))
            return nullptr;
        return VortexExpressionPtr(vortex_ffi_expr_literal_float(ptype, value));
    };

    auto make_string = [&](bool is_utf8) -> VortexExpressionPtr
    {
        if (field.getType() != Field::Types::String)
            return nullptr;
        const auto & value = field.safeGet<String>();
        return VortexExpressionPtr(
            vortex_ffi_expr_literal_string(reinterpret_cast<const uint8_t *>(value.data()), value.size(), is_utf8));
    };

    switch (arrow_type.id())
    {
        case arrow::Type::INT8: return make_int(VortexFFIPType::I8);
        case arrow::Type::INT16: return make_int(VortexFFIPType::I16);
        case arrow::Type::INT32: return make_int(VortexFFIPType::I32);
        case arrow::Type::INT64: return make_int(VortexFFIPType::I64);
        case arrow::Type::UINT8: return make_uint(VortexFFIPType::U8);
        case arrow::Type::UINT16: return make_uint(VortexFFIPType::U16);
        case arrow::Type::UINT32: return make_uint(VortexFFIPType::U32);
        case arrow::Type::UINT64: return make_uint(VortexFFIPType::U64);
        case arrow::Type::FLOAT: return make_float(VortexFFIPType::F32);
        case arrow::Type::DOUBLE: return make_float(VortexFFIPType::F64);
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::STRING_VIEW:
            return make_string(/* is_utf8 */ true);
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::BINARY_VIEW:
            return make_string(/* is_utf8 */ false);
        default:
            return nullptr;
    }
}

/// An `IN` set larger than this is not pushed down (it would become a long chain of ORs that is
/// re-evaluated per statistics zone).
constexpr size_t max_pushed_down_set_size = 64;

/// Translates one atom (comparison, IN, IS NULL) of the KeyCondition. Returns nullptr when the
/// atom cannot be translated exactly.
VortexExpressionPtr buildVortexAtomExpression(
    const KeyCondition::RPNElement & element,
    const KeyCondition & key_condition,
    const Block & header,
    const arrow::Schema & schema,
    bool positive)
{
    using RPNElement = KeyCondition::RPNElement;

    /// A condition on a function of a column (e.g. `toDate(t) = ...`) is not translatable.
    if (!element.monotonic_functions_chain.empty())
        return nullptr;

    /// A relaxed atom matches a superset of the original condition (e.g. a prefix range built
    /// from LIKE). That is fine to push down as-is, but under an odd number of NOTs the
    /// negation would drop rows the query keeps.
    if (element.relaxed && !positive)
        return nullptr;

    const bool is_set = element.function == RPNElement::FUNCTION_IN_SET || element.function == RPNElement::FUNCTION_NOT_IN_SET;
    if (is_set
        && (!element.set_index || element.set_index->getOrderedSet().size() != 1
            || element.set_index->hasMonotonicFunctionsChain()))
        return nullptr;

    String column_name = getColumnNameFromKeyCondition(key_condition, element.getKeyColumn());
    auto arrow_field = schema.GetFieldByName(column_name);
    const auto * header_column = header.findByName(column_name);
    if (!arrow_field || !header_column)
        return nullptr;

    if (!typesMatchForFilterPushdown(header_column->type, *arrow_field->type()))
        return nullptr;

    /// If the file column is nullable but the query reads it as non-nullable, ClickHouse
    /// replaces nulls with default values (or fails), while the pushed filter would evaluate
    /// the atom on null and drop the row. Push only when both sides see the same values.
    const bool header_nullable = header_column->type->isNullable() || header_column->type->isLowCardinalityNullable();
    if (arrow_field->nullable() && !header_nullable)
        return nullptr;

    VortexExpressionPtr column(vortex_ffi_expr_column(column_name.c_str()));
    if (!column)
        return nullptr;

    switch (element.function)
    {
        case RPNElement::FUNCTION_IS_NULL:
            return VortexExpressionPtr(vortex_ffi_expr_is_null(column.get()));
        case RPNElement::FUNCTION_IS_NOT_NULL:
        {
            VortexExpressionPtr null_check(vortex_ffi_expr_is_null(column.get()));
            return VortexExpressionPtr(vortex_ffi_expr_not(null_check.get()));
        }
        case RPNElement::FUNCTION_IN_RANGE:
        case RPNElement::FUNCTION_NOT_IN_RANGE:
        {
            const Range & range = element.range;
            const bool has_left = !range.left.isNegativeInfinity();
            const bool has_right = !range.right.isPositiveInfinity();
            if (!has_left && !has_right)
                return nullptr;

            VortexExpressionPtr result;
            if (has_left && has_right && range.left_included && range.right_included && range.left == range.right)
            {
                auto literal = makeVortexLiteral(*arrow_field->type(), range.left);
                if (!literal)
                    return nullptr;
                result = VortexExpressionPtr(vortex_ffi_expr_compare(VortexFFIComparison::Eq, column.get(), literal.get()));
            }
            else
            {
                VortexExpressionPtr left_bound;
                if (has_left)
                {
                    auto literal = makeVortexLiteral(*arrow_field->type(), range.left);
                    if (!literal)
                        return nullptr;
                    left_bound = VortexExpressionPtr(vortex_ffi_expr_compare(
                        range.left_included ? VortexFFIComparison::Gte : VortexFFIComparison::Gt, column.get(), literal.get()));
                }
                VortexExpressionPtr right_bound;
                if (has_right)
                {
                    auto literal = makeVortexLiteral(*arrow_field->type(), range.right);
                    if (!literal)
                        return nullptr;
                    right_bound = VortexExpressionPtr(vortex_ffi_expr_compare(
                        range.right_included ? VortexFFIComparison::Lte : VortexFFIComparison::Lt, column.get(), literal.get()));
                }
                if (left_bound && right_bound)
                    result = VortexExpressionPtr(vortex_ffi_expr_and(left_bound.get(), right_bound.get()));
                else
                    result = left_bound ? std::move(left_bound) : std::move(right_bound);
            }
            if (!result)
                return nullptr;
            if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
                return VortexExpressionPtr(vortex_ffi_expr_not(result.get()));
            return result;
        }
        case RPNElement::FUNCTION_IN_SET:
        case RPNElement::FUNCTION_NOT_IN_SET:
        {
            const auto & set_column = element.set_index->getOrderedSet()[0];
            if (set_column->empty() || set_column->size() > max_pushed_down_set_size)
                return nullptr;

            VortexExpressionPtr result;
            for (size_t i = 0; i < set_column->size(); ++i)
            {
                auto literal = makeVortexLiteral(*arrow_field->type(), (*set_column)[i]);
                if (!literal)
                    return nullptr;
                VortexExpressionPtr equals(vortex_ffi_expr_compare(VortexFFIComparison::Eq, column.get(), literal.get()));
                if (!equals)
                    return nullptr;
                if (result)
                    result = VortexExpressionPtr(vortex_ffi_expr_or(result.get(), equals.get()));
                else
                    result = std::move(equals);
            }
            if (!result)
                return nullptr;
            if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
                return VortexExpressionPtr(vortex_ffi_expr_not(result.get()));
            return result;
        }
        default:
            return nullptr;
    }
}

/// Consumes the expression on top of `rpn_stack` and returns its translation, or nullptr if that
/// part of the condition is not pushed down. `positive` tells whether the expression is under an
/// even number of NOTs; it decides in which direction the condition may be weakened.
VortexExpressionPtr buildVortexFilterImpl(
    KeyCondition::RPN & rpn_stack,
    const KeyCondition & key_condition,
    const Block & header,
    const arrow::Schema & schema,
    bool positive)
{
    using RPNElement = KeyCondition::RPNElement;

    if (rpn_stack.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty RPN stack while building a Vortex filter expression");

    const RPNElement element = rpn_stack.back();
    rpn_stack.pop_back();

    switch (element.function)
    {
        case RPNElement::FUNCTION_IN_RANGE:
        case RPNElement::FUNCTION_NOT_IN_RANGE:
        case RPNElement::FUNCTION_IN_SET:
        case RPNElement::FUNCTION_NOT_IN_SET:
        case RPNElement::FUNCTION_IS_NULL:
        case RPNElement::FUNCTION_IS_NOT_NULL:
            return buildVortexAtomExpression(element, key_condition, header, schema, positive);
        case RPNElement::FUNCTION_NOT:
        {
            auto child = buildVortexFilterImpl(rpn_stack, key_condition, header, schema, !positive);
            if (!child)
                return nullptr;
            return VortexExpressionPtr(vortex_ffi_expr_not(child.get()));
        }
        case RPNElement::FUNCTION_AND:
        {
            auto rhs = buildVortexFilterImpl(rpn_stack, key_condition, header, schema, positive);
            auto lhs = buildVortexFilterImpl(rpn_stack, key_condition, header, schema, positive);
            if (lhs && rhs)
                return VortexExpressionPtr(vortex_ffi_expr_and(lhs.get(), rhs.get()));
            /// Dropping a conjunct only widens the filter, which is allowed in positive polarity.
            if (positive)
                return lhs ? std::move(lhs) : std::move(rhs);
            return nullptr;
        }
        case RPNElement::FUNCTION_OR:
        {
            auto rhs = buildVortexFilterImpl(rpn_stack, key_condition, header, schema, positive);
            auto lhs = buildVortexFilterImpl(rpn_stack, key_condition, header, schema, positive);
            if (lhs && rhs)
                return VortexExpressionPtr(vortex_ffi_expr_or(lhs.get(), rhs.get()));
            /// NOT (a OR b) implies NOT a, so under a NOT a disjunct may be dropped.
            if (!positive)
                return lhs ? std::move(lhs) : std::move(rhs);
            return nullptr;
        }
        case RPNElement::ALWAYS_FALSE:
            return VortexExpressionPtr(vortex_ffi_expr_literal_bool(false));
        case RPNElement::ALWAYS_TRUE:
        /// KeyCondition relaxes what it cannot analyze towards "always true", so ALWAYS_TRUE is
        /// not necessarily exact and cannot be negated; treat it as not translated.
        case RPNElement::FUNCTION_UNKNOWN:
        case RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE:
        case RPNElement::FUNCTION_POINT_IN_POLYGON:
            return nullptr;
    }
    return nullptr;
}

/// Translates the KeyCondition into a Vortex filter expression, or returns nullptr when nothing
/// useful can be pushed down.
VortexExpressionPtr buildVortexFilterExpression(const KeyCondition & key_condition, const Block & header, const arrow::Schema & schema)
{
    auto rpn_stack = key_condition.getRPN();
    if (rpn_stack.empty())
        return nullptr;

    auto result = buildVortexFilterImpl(rpn_stack, key_condition, header, schema, /* positive */ true);
    chassert(rpn_stack.empty());
    return result;
}

}

/// The I/O options of a reader: how many reads may be in flight and how the reads of nearby
/// segments are merged.
///
/// A read occupies the thread that runs it (the callback is synchronous), so more concurrent reads
/// than threads driving the I/O queue have nobody to run them.
/// `RandomAccessFileFromRandomAccessReadBuffer` (`readBigAt`) and a file copied into memory are
/// thread-safe; the seek+read wrapper is not and gets one read at a time.
///
/// Coalescing merges the reads of segments that are close in the file into one callback: fewer
/// requests to remote storage and fewer syscalls locally, at the price of reading the gaps. The
/// distances and sizes are the ones Vortex uses for its own file and object store sources. A file
/// copied into memory needs none.
static VortexFFIReaderOptions makeReaderOptions(const arrow::io::RandomAccessFile & file, size_t io_threads, bool is_remote_fs)
{
    VortexFFIReaderOptions options{};
    const bool in_memory = dynamic_cast<const arrow::io::BufferReader *>(&file) != nullptr;
    const bool thread_safe = in_memory || dynamic_cast<const RandomAccessFileFromRandomAccessReadBuffer *>(&file) != nullptr;
    options.io_concurrency = thread_safe ? static_cast<uint32_t>(std::clamp<size_t>(io_threads, 1, 1024)) : 1;
    if (!in_memory)
    {
        options.coalesce_distance = 1 << 20;
        options.coalesce_max_size = is_remote_fs ? (16 << 20) : (4 << 20);
    }
    return options;
}

/// Opens a Vortex file over a ClickHouse read buffer and reads its Arrow schema. `io_threads` is
/// the number of threads that will run the reads (see `makeReaderOptions`).
static VortexFFIReader * openVortexReader(
    const VortexFFIRuntime * runtime,
    ReadBuffer & in,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped,
    std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    VortexReadContext & read_context,
    std::shared_ptr<arrow::Schema> & file_schema,
    size_t io_threads,
    bool is_remote_fs)
{
    /// Avoid read-ahead buffering on the ClickHouse side: the Vortex library coalesces its reads
    /// by itself, so buffering would only fetch extra bytes (noticeable on remote storage).
    arrow_file = asArrowFile(in, format_settings, is_stopped, "Vortex", VORTEX_MAGIC_BYTES, /* avoid_buffering */ true);
    if (is_stopped)
        return nullptr;

    auto file_size = arrow_file->GetSize();
    throwFromArrowStatusIfFailed(file_size.status());

    read_context.file = arrow_file.get();

    const VortexFFIReaderOptions options = makeReaderOptions(*arrow_file, io_threads, is_remote_fs);
    char * error = nullptr;
    auto * reader
        = vortex_ffi_reader_open(runtime, &read_context, vortexFFIReadCallback, static_cast<uint64_t>(*file_size), &options, &error);
    if (!reader)
        throwVortexError(error, read_context.getException());

    ArrowSchema c_schema{};
    if (vortex_ffi_reader_schema(reader, &c_schema, &error) != 0)
    {
        vortex_ffi_reader_free(reader);
        throwVortexError(error, read_context.getException());
    }

    auto schema = arrow::ImportSchema(&c_schema);
    if (!schema.ok())
    {
        vortex_ffi_reader_free(reader);
        throwFromArrowStatusIfFailed(schema.status());
    }
    file_schema = *schema;

    return reader;
}

/// The C callbacks of the runtime and of the scan consumer. They must not let an exception escape
/// into the library, so everything they do is `noexcept`.

extern "C" void vortexFFINotifyCallback(void * context, VortexFFIQueue queue);
extern "C" void vortexFFINotifyCallback(void * context, VortexFFIQueue queue)
{
    static_cast<VortexBlockInputFormat *>(context)->onNotify(queue);
}

extern "C" int32_t vortexFFIChunkCallback(void * context, ::ArrowArray * array, uint64_t split_index);
extern "C" int32_t vortexFFIChunkCallback(void * context, ::ArrowArray * array, uint64_t split_index)
{
    return static_cast<VortexBlockInputFormat *>(context)->onChunk(array, split_index);
}

extern "C" void vortexFFIFinishCallback(void * context, const char * error);
extern "C" void vortexFFIFinishCallback(void * context, const char * error)
{
    static_cast<VortexBlockInputFormat *>(context)->onScanFinish(error);
}

VortexBlockInputFormat::VortexBlockInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    const FormatSettings & format_settings_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    FormatFilterInfoPtr format_filter_info_,
    bool is_remote_fs_)
    : IInputFormat(header_, &in_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
    , parser_shared_resources(std::move(parser_shared_resources_))
    , format_filter_info(std::move(format_filter_info_))
    , is_remote_fs(is_remote_fs_)
{
}

VortexBlockInputFormat::~VortexBlockInputFormat()
{
    closeReader();
}

bool VortexBlockInputFormat::hasSeparateIORunner() const
{
    return parser_shared_resources && !parser_shared_resources->io_runner.isDisabled();
}

ThreadPoolCallbackRunnerFast & VortexBlockInputFormat::runnerFor(VortexFFIQueue queue) const
{
    chassert(parser_shared_resources);
    /// The reads go to the download pool when there is one, as in the Parquet reader; otherwise the
    /// two queues share the parsing pool (or run inline in the manual mode).
    if (queue == VortexFFIQueue::Io && hasSeparateIORunner())
        return parser_shared_resources->io_runner;
    return parser_shared_resources->parsing_runner;
}

VortexFFIQueue VortexBlockInputFormat::driverQueue(VortexFFIQueue queue) const
{
    /// Without a download pool one kind of driver runs both queues, so that the parsing pool is not
    /// oversubscribed by two sets of drivers.
    return hasSeparateIORunner() ? queue : VortexFFIQueue::Cpu;
}

size_t VortexBlockInputFormat::maxDrivers(VortexFFIQueue queue) const
{
    if (!parser_shared_resources)
        return 0;
    queue = driverQueue(queue);
    const auto & runner = runnerFor(queue);
    if (runner.getMode() != ThreadPoolCallbackRunnerFast::Mode::ThreadPool)
        return 0;
    /// The share of this reader shrinks with the number of files read in parallel and grows back as
    /// they finish, so it is recomputed on every notification.
    size_t threads = queue == VortexFFIQueue::Io ? parser_shared_resources->getIOThreadsPerReader()
                                                 : parser_shared_resources->getParsingThreadsPerReader();
    return std::max<size_t>(threads, 1);
}

void VortexBlockInputFormat::onNotify(VortexFFIQueue queue) noexcept
{
    if (closing.load(std::memory_order_acquire) || is_stopped)
        return;

    queue = driverQueue(queue);
    const size_t max_drivers = maxDrivers(queue);
    if (max_drivers == 0)
        return; /// The manual mode: `read` runs the tasks itself.

    /// The task was queued before this call, and a driver that stops decrements the counter before
    /// it looks at the queue (see `driveQueue`); both sequentially consistent, so that either this
    /// call sees the driver gone or the driver sees the task, and no task is left with nobody to
    /// run it.
    size_t running = running_drivers[static_cast<size_t>(queue)].load(std::memory_order_seq_cst);
    while (running < max_drivers)
    {
        if (!running_drivers[static_cast<size_t>(queue)].compare_exchange_weak(running, running + 1, std::memory_order_relaxed))
            continue;

        try
        {
            runnerFor(queue)([this, queue, shutdown = tasks_shutdown] { driveQueue(queue, shutdown); });
        }
        catch (...)
        {
            /// The runner does not keep a task it failed to schedule, so the driver will not run.
            running_drivers[static_cast<size_t>(queue)].fetch_sub(1, std::memory_order_relaxed);
            /// The scan is not cancelled from here: `vortex_ffi_scan_cancel` calls `onNotify` back
            /// on the calling thread (a cancelled task is queued once more to be dropped), and a
            /// second failure would then take `scan_mutex` recursively. `read` rethrows the error
            /// right away, and the scan is cancelled when the reader is closed.
            setBackgroundException(std::current_exception(), /* cancel_scan */ false);
        }
        return;
    }
}

void VortexBlockInputFormat::driveQueue(VortexFFIQueue queue, std::shared_ptr<ShutdownHelper> shutdown_) noexcept
{
    /// The reader may already be closed (and destroyed) if the task waited in the pool for long.
    std::shared_lock shutdown_lock(*shutdown_, std::try_to_lock);
    if (!shutdown_lock.owns_lock())
    {
        /// Not decrementing `running_drivers`: nobody looks at it after the shutdown.
        return;
    }

    /// A driver of the CPU queue also runs the I/O queue when the two share a runner (see
    /// `driverQueue`), so that the reads of a file are not stalled by a reader without I/O drivers.
    const bool drive_io = queue == VortexFFIQueue::Io || !hasSeparateIORunner();
    const bool drive_cpu = queue == VortexFFIQueue::Cpu;

    /// Runs the tasks in batches, so that the counter is up to date often enough for another
    /// notification to start a second driver while this one is busy.
    while (true)
    {
        char * error = nullptr;
        int64_t tasks = 0;
        if (drive_cpu)
            tasks = vortex_ffi_runtime_run(runtime, VortexFFIQueue::Cpu, /* max_tasks */ 16, &error);
        if (tasks >= 0 && drive_io)
        {
            int64_t io_tasks = vortex_ffi_runtime_run(runtime, VortexFFIQueue::Io, /* max_tasks */ 16, &error);
            tasks = io_tasks < 0 ? io_tasks : tasks + io_tasks;
        }
        if (tasks < 0)
        {
            setBackgroundException(makeVortexException(takeVortexError(error), read_context->getException()));
            break;
        }
        if (tasks == 0)
            break;
    }

    /// A task could have been queued between the last run and the decrement, and its notification
    /// would then have found the driver still running. Sequentially consistent, together with the
    /// load in `onNotify`, so that this driver sees such a task or the notification sees the
    /// decrement (with weaker orders both could see the old values, on AArch64 for real).
    running_drivers[static_cast<size_t>(queue)].fetch_sub(1, std::memory_order_seq_cst);

    if ((drive_cpu && vortex_ffi_runtime_pending(runtime, VortexFFIQueue::Cpu) > 0)
        || (drive_io && vortex_ffi_runtime_pending(runtime, VortexFFIQueue::Io) > 0))
        onNotify(queue);

    {
        std::lock_guard lock(delivery_mutex);
    }
    delivery_cv.notify_all();
}

int32_t VortexBlockInputFormat::onChunk(::ArrowArray * array, UInt64 split_index) noexcept
{
    try
    {
        DeliveredChunk item;
        item.empty = array == nullptr;
        item.holds_permit = array != nullptr;

        if (array)
        {
            /// The array is owned by this callback: importing it passes the ownership to Arrow.
            auto batch = arrow::ImportRecordBatch(array, scan_schema);
            throwFromArrowStatusIfFailed(batch.status());

            ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(**batch);

            auto table = arrow::Table::FromRecordBatches({*batch});
            throwFromArrowStatusIfFailed(table.status());

            std::unique_ptr<ArrowColumnToCHColumn> converter;
            {
                std::lock_guard lock(converters_mutex);
                if (!converters.empty())
                {
                    converter = std::move(converters.back());
                    converters.pop_back();
                }
            }
            if (!converter)
                converter = createConverter();

            item.missing_values = BlockMissingValues(getPort().getHeader().columns());
            /// If defaults_for_omitted_fields is true, calculate the default values from default
            /// expression for omitted fields. Otherwise fill the missing columns with zero values.
            BlockMissingValues * missing_values_ptr = format_settings.defaults_for_omitted_fields ? &item.missing_values : nullptr;
            item.chunk = converter->arrowTableToCHChunk(*table, (*table)->num_rows(), nullptr, missing_values_ptr);

            std::lock_guard lock(converters_mutex);
            converters.push_back(std::move(converter));
        }

        {
            std::lock_guard lock(delivery_mutex);
            if (!delivered.emplace(split_index, std::move(item)).second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Vortex scan delivered split {} twice", split_index);
        }
        delivery_cv.notify_all();
        return 0;
    }
    catch (...)
    {
        setBackgroundException(std::current_exception());
        return 1;
    }
}

void VortexBlockInputFormat::onScanFinish(const char * error) noexcept
{
    {
        std::lock_guard lock(delivery_mutex);
        if (error && !background_exception)
            background_exception = makeVortexException(error, read_context->getException());
        scan_finished = true;
    }
    delivery_cv.notify_all();
}

void VortexBlockInputFormat::setBackgroundException(std::exception_ptr exception, bool cancel_scan) noexcept
{
    {
        std::lock_guard lock(delivery_mutex);
        if (!background_exception)
            background_exception = std::move(exception);
    }
    delivery_cv.notify_all();
    /// The rest of the scan is not needed anymore; `read` rethrows the error.
    if (cancel_scan)
        cancelScan();
}

void VortexBlockInputFormat::cancelScan() noexcept
{
    std::lock_guard lock(scan_mutex);
    if (scan)
        vortex_ffi_scan_cancel(scan);
}

void VortexBlockInputFormat::onCancel() noexcept
{
    is_stopped = 1;
    cancelScan();
    {
        std::lock_guard lock(delivery_mutex);
    }
    delivery_cv.notify_all();
}

void VortexBlockInputFormat::stopTasks()
{
    closing.store(true, std::memory_order_release);
    cancelScan();
    /// Waits for the drivers that are running (they stop as soon as the queues are empty, which
    /// the cancellation makes them be); the ones still queued in the pool find the shutdown when
    /// they start and return without touching the reader.
    if (tasks_shutdown)
        tasks_shutdown->shutdown();
}

void VortexBlockInputFormat::closeReader()
{
    stopTasks();
    {
        std::lock_guard lock(scan_mutex);
        if (scan)
        {
            vortex_ffi_scan_free(scan);
            scan = nullptr;
        }
        scan_schema.reset();
    }
    {
        std::lock_guard lock(delivery_mutex);
        delivered.clear();
        next_split_index = 0;
        scan_finished = false;
        background_exception = nullptr;
    }
    {
        std::lock_guard lock(converters_mutex);
        converters.clear();
    }
    for (auto & running : running_drivers)
        running.store(0, std::memory_order_relaxed);
    /// `tasks_shutdown` is not reset here: `onNotify` may be copying it right now, and it is
    /// replaced by `prepareReader` before the next runtime exists, when no notification can arrive.
    if (reader)
    {
        vortex_ffi_reader_free(reader);
        reader = nullptr;
    }
    if (runtime)
    {
        vortex_ffi_runtime_free(runtime);
        runtime = nullptr;
    }
    file_schema.reset();
    arrow_file.reset();
    read_context.reset();
    closing.store(false, std::memory_order_release);
}

std::unique_ptr<ArrowColumnToCHColumn> VortexBlockInputFormat::createConverter() const
{
    return std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Vortex",
        format_settings,
        std::nullopt,
        std::nullopt,
        /* allow_missing_columns */ true,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.allow_geoparquet_parser);
}

void VortexBlockInputFormat::prepareReader()
{
    if (parser_shared_resources)
    {
        parser_shared_resources->initOnce(
            [&]
            {
                /// `max_parsing_threads = 1` means "no thread pool at all", the same convention as in
                /// `ParquetV3BlockInputFormat`: the tasks then run inside `read`.
                if (parser_shared_resources->max_parsing_threads <= 1)
                    parser_shared_resources->parsing_runner.initManual();
                else
                    parser_shared_resources->parsing_runner.initThreadPool(
                        getFormatParsingThreadPool().get(),
                        parser_shared_resources->max_parsing_threads,
                        ThreadName::VORTEX_DECODER,
                        CurrentThread::getGroup());

                /// Reads go to the download pool, so that they do not occupy the decoding threads.
                if (parser_shared_resources->max_parsing_threads > 1 && parser_shared_resources->max_io_threads > 0)
                    parser_shared_resources->io_runner.initThreadPool(
                        getFormatParsingThreadPool().get(),
                        parser_shared_resources->max_io_threads,
                        ThreadName::VORTEX_READER,
                        CurrentThread::getGroup());
            });
    }

    tasks_shutdown = std::make_shared<ShutdownHelper>();
    /// A runtime that reports its runnable tasks to this reader. It must be created before anything
    /// that spawns tasks on it, and freed after everything that may spawn (see `closeReader`).
    runtime = vortex_ffi_runtime_new(this, parser_shared_resources ? vortexFFINotifyCallback : nullptr);

    const size_t cpu_threads = std::max<size_t>(maxDrivers(VortexFFIQueue::Cpu), 1);
    const size_t io_threads = std::max<size_t>(maxDrivers(VortexFFIQueue::Io), 1);

    read_context = std::make_unique<VortexReadContext>();
    reader = openVortexReader(runtime, *in, format_settings, is_stopped, arrow_file, *read_context, file_schema, io_threads, is_remote_fs);
    if (!reader)
        return;

    if (need_only_count)
        return;

    /// Read only the columns requested in the header and present in the file. The requested
    /// columns that are not present in the file are filled with default values. If none of the
    /// requested columns is present (or no columns are requested at all), no scan is created,
    /// and only the number of rows is used.
    ///
    /// A header column can also address a subcolumn of a top-level field: a `Nested`/struct
    /// subcolumn is requested as `name.sub`, and `ArrowColumnToCHColumn` can extract it only if
    /// the whole top-level field `name` was scanned. Keep `Nested::extractTableName` of every
    /// header column for that reason, the same way the `ArrowIPC` reader does — otherwise such a
    /// column would be silently filled with default values.
    std::vector<std::string> column_names;
    std::vector<const char *> column_name_pointers;
    std::unordered_set<std::string> added_column_names;
    auto add_column_name = [&](const std::string & name)
    {
        if (file_schema->GetFieldByName(name) && added_column_names.emplace(name).second)
            column_names.push_back(name);
    };
    for (const auto & column : getPort().getHeader())
    {
        add_column_name(column.name);
        add_column_name(Nested::extractTableName(column.name));
    }
    for (const auto & name : column_names)
        column_name_pointers.push_back(name.c_str());

    if (column_names.empty())
    {
        pending_rows_without_columns = vortex_ffi_reader_row_count(reader);
        return;
    }

    /// Push the translatable parts of the WHERE condition down into the scan, which may reduce
    /// the rows decoded by selective queries. Whole segments are not yet pruned by statistics.
    /// The translation is best-effort, and ClickHouse re-applies the full filter on the returned
    /// rows anyway.
    VortexExpressionPtr filter;
    if (format_settings.vortex.filter_push_down && format_filter_info && format_filter_info->hasFilter())
    {
        format_filter_info->initKeyConditionOnce(getPort().getHeader());
        if (format_filter_info->key_condition)
            filter = buildVortexFilterExpression(*format_filter_info->key_condition, getPort().getHeader(), *file_schema);
    }

    VortexFFIScanOptions options{};
    options.columns = column_name_pointers.data();
    options.num_columns = column_name_pointers.size();
    options.filter = filter.get();
    /// Chunks in flight: being read, decoded, or waiting in the delivery queue. Two per decoding
    /// thread, so that a thread has a chunk to decode while the I/O of the next one is in flight.
    /// Capped because the decoded chunks of a wide projection are large; this bounds the memory the
    /// scan holds, together with how fast `read` returns the capacity.
    options.in_flight = static_cast<uint32_t>(std::clamp<size_t>(2 * cpu_threads, 4, 64));

    VortexFFIScanConsumer consumer{};
    consumer.context = this;
    consumer.on_chunk = vortexFFIChunkCallback;
    consumer.on_finish = vortexFFIFinishCallback;

    /// The scan starts producing as soon as it is created - a chunk may reach `onChunk` before
    /// `vortex_ffi_scan_create` even returns - so the schema those chunks are imported with has to
    /// be in place beforehand. It is the projection of the file schema, which is exactly what the
    /// library builds for the same list of columns (checked below).
    arrow::FieldVector scan_fields;
    scan_fields.reserve(column_names.size());
    for (const auto & name : column_names)
        scan_fields.push_back(file_schema->GetFieldByName(name));
    scan_schema = arrow::schema(std::move(scan_fields));

    char * error = nullptr;
    auto * new_scan = vortex_ffi_scan_create(reader, &options, &consumer, &error);
    if (!new_scan)
    {
        /// Nothing was delivered: the scan is spawned last, after everything that can fail. This
        /// only makes sure that no driver of this reader is running before the exception propagates
        /// (the reader is closed right after, when `ISource` handles the exception, see
        /// `IInputFormat::onFinish`).
        stopTasks();
        throwVortexError(error, read_context->getException());
    }

    /// The schema the library reports must match the one the chunks are imported with; a mismatch
    /// would surface as a confusing import error on every chunk.
    ArrowSchema c_schema{};
    if (vortex_ffi_scan_schema(new_scan, &c_schema, &error) == 0)
    {
        auto library_schema = arrow::ImportSchema(&c_schema);
        chassert(library_schema.ok() && (*library_schema)->Equals(*scan_schema, /* check_metadata */ false));
    }
    else if (error)
    {
        vortex_ffi_free_string(error);
    }

    std::lock_guard scan_lock(scan_mutex);
    scan = new_scan;
}

Chunk VortexBlockInputFormat::readWithoutColumns()
{
    if (!pending_rows_without_columns)
        return {};

    size_t num_rows = std::min<UInt64>(pending_rows_without_columns, DEFAULT_BLOCK_SIZE);
    pending_rows_without_columns -= num_rows;

    auto batch = arrow::RecordBatch::Make(arrow::schema(arrow::FieldVector{}), num_rows, arrow::ArrayVector{});
    auto table = arrow::Table::FromRecordBatches({batch});
    throwFromArrowStatusIfFailed(table.status());

    std::unique_ptr<ArrowColumnToCHColumn> converter;
    {
        std::lock_guard lock(converters_mutex);
        if (!converters.empty())
        {
            converter = std::move(converters.back());
            converters.pop_back();
        }
    }
    if (!converter)
        converter = createConverter();

    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    Chunk chunk = converter->arrowTableToCHChunk(*table, num_rows, nullptr, block_missing_values_ptr);

    std::lock_guard lock(converters_mutex);
    converters.push_back(std::move(converter));
    return chunk;
}

Chunk VortexBlockInputFormat::read()
{
    if (!reader && !count_returned)
        prepareReader();

    if (is_stopped)
        return {};

    if (need_only_count)
    {
        if (count_returned)
            return {};
        count_returned = true;
        return getChunkForCount(vortex_ffi_reader_row_count(reader));
    }

    block_missing_values.clear();

    if (!scan)
        return readWithoutColumns();

    /// Without a thread pool the tasks run inside this call, on this thread.
    const bool manual
        = !parser_shared_resources || parser_shared_resources->parsing_runner.getMode() != ThreadPoolCallbackRunnerFast::Mode::ThreadPool;

    /// Consecutive waits that found the scan making no progress, see the end of the loop.
    size_t idle_timeouts = 0;

    std::unique_lock lock(delivery_mutex);
    while (true)
    {
        if (background_exception)
            std::rethrow_exception(background_exception);
        if (is_stopped)
            return {};

        if (!delivered.empty())
        {
            auto it = delivered.begin();
            if (!format_settings.vortex.preserve_order || it->first == next_split_index)
            {
                const UInt64 split_index = it->first;
                DeliveredChunk item = std::move(it->second);
                delivered.erase(it);
                next_split_index = std::max(next_split_index, split_index) + 1;
                lock.unlock();

                if (item.holds_permit)
                {
                    /// The scan may read one split further ahead now.
                    std::lock_guard scan_lock(scan_mutex);
                    if (scan)
                        vortex_ffi_scan_release(scan, 1);
                }

                if (item.empty)
                {
                    /// A split that matched no rows: it only moves the file order forward.
                    lock.lock();
                    continue;
                }

                block_missing_values = std::move(item.missing_values);
                /// The bytes read since the previous chunk was returned are attributed to this one.
                size_t bytes_read = read_context->bytes_read.load(std::memory_order_relaxed);
                approx_bytes_read_for_chunk = bytes_read - previous_approx_bytes_read;
                previous_approx_bytes_read = bytes_read;
                return std::move(item.chunk);
            }

            /// The next chunk in file order is still being produced. Chunks may be delivered out of
            /// order, so this only means it is not here yet, unless the scan is over.
            if (scan_finished)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Vortex reader lost split {} of the scan (the next delivered one is {})",
                    next_split_index,
                    it->first);
        }
        else if (scan_finished)
        {
            return {};
        }

        if (manual)
        {
            /// No thread pool: run the tasks of the scan on this thread.
            lock.unlock();
            char * error = nullptr;
            int64_t cpu_tasks = vortex_ffi_runtime_run(runtime, VortexFFIQueue::Cpu, /* max_tasks */ 8, &error);
            int64_t io_tasks = cpu_tasks < 0 ? 0 : vortex_ffi_runtime_run(runtime, VortexFFIQueue::Io, /* max_tasks */ 8, &error);
            if (cpu_tasks < 0 || io_tasks < 0)
                throwVortexError(error, read_context->getException());
            lock.lock();
            if (cpu_tasks == 0 && io_tasks == 0)
            {
                /// Nothing ran and nothing can run: no other thread will make this call progress.
                if (!scan_finished && !background_exception && !is_stopped)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in the Vortex reader (single-threaded)");
            }
            continue;
        }

        /// Wait for the tasks running in the pool. Re-check periodically as a safety net: the
        /// protocol between the drivers and the notifications (see `driveQueue`) is meant to leave
        /// no runnable task without a driver, and a bug in it should cost a stall, not a hang.
        if (delivery_cv.wait_for(lock, std::chrono::seconds(1)) != std::cv_status::timeout)
            continue;

        const bool idle = running_drivers[static_cast<size_t>(VortexFFIQueue::Cpu)].load(std::memory_order_relaxed) == 0
            && running_drivers[static_cast<size_t>(VortexFFIQueue::Io)].load(std::memory_order_relaxed) == 0;
        if (!idle || scan_finished || background_exception)
        {
            idle_timeouts = 0;
            continue;
        }

        lock.unlock();
        const bool has_tasks
            = vortex_ffi_runtime_pending(runtime, VortexFFIQueue::Cpu) > 0 || vortex_ffi_runtime_pending(runtime, VortexFFIQueue::Io) > 0;
        if (has_tasks)
        {
            /// Tasks are waiting and nothing is running them, which the protocol above should make
            /// impossible: schedule the drivers again rather than hang.
            onNotify(VortexFFIQueue::Cpu);
            onNotify(VortexFFIQueue::Io);
            idle_timeouts = 0;
        }
        else
        {
            /// Nothing is running, nothing is queued, and the scan has neither delivered what this
            /// call needs nor finished: it would wait forever. Reported after a grace period, so
            /// that a task that was about to be queued is not mistaken for a deadlock.
            ++idle_timeouts;
        }
        lock.lock();

        const bool nothing_deliverable
            = delivered.empty() || (format_settings.vortex.preserve_order && delivered.begin()->first != next_split_index);
        if (idle_timeouts >= 3 && nothing_deliverable && !scan_finished && !background_exception && !is_stopped)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in the Vortex reader (thread pool)");
    }
}

void VortexBlockInputFormat::resetParser()
{
    /// The scan tasks read through the buffer that `IInputFormat::resetParser` is about to drain.
    closeReader();
    IInputFormat::resetParser();

    pending_rows_without_columns = 0;
    count_returned = false;
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;
    previous_approx_bytes_read = 0;
}

void VortexBlockInputFormat::resetReadBuffer()
{
    /// The scan tasks read through the buffer that is about to be released.
    closeReader();
    IInputFormat::resetReadBuffer();
}

const BlockMissingValues * VortexBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}

VortexSchemaReader::VortexSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

VortexSchemaReader::~VortexSchemaReader()
{
    if (reader)
        vortex_ffi_reader_free(reader);
    if (runtime)
        vortex_ffi_runtime_free(runtime);
}

void VortexSchemaReader::initializeIfNeeded()
{
    if (reader)
        return;

    /// No notification callback: the calling thread runs the tasks of the footer read itself.
    if (!runtime)
        runtime = vortex_ffi_runtime_new(nullptr, nullptr);
    read_context = std::make_unique<VortexReadContext>();
    reader = openVortexReader(
        runtime,
        in,
        format_settings,
        is_stopped,
        arrow_file,
        *read_context,
        file_schema,
        /* io_threads */ 1,
        /* is_remote_fs */ false);
}

NamesAndTypesList VortexSchemaReader::readSchema()
{
    initializeIfNeeded();

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *file_schema,
        nullptr,
        "Vortex",
        format_settings,
        /* skip_columns_with_unsupported_types */ false,
        /* allow_arrow_null_type */ true,
        format_settings.schema_inference_make_columns_nullable != 0,
        /* case_insensitive_matching */ false,
        format_settings.parquet.allow_geoparquet_parser);
    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> VortexSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return vortex_ffi_reader_row_count(reader);
}

void registerInputFormatVortex(FormatFactory & factory);
void registerInputFormatVortex(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "Vortex",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & /* read_settings */,
           bool is_remote_fs,
           FormatParserSharedResourcesPtr parser_shared_resources,
           FormatFilterInfoPtr format_filter_info) -> InputFormatPtr
        {
            return std::make_shared<VortexBlockInputFormat>(
                buf,
                std::make_shared<const Block>(sample),
                settings,
                std::move(parser_shared_resources),
                std::move(format_filter_info),
                is_remote_fs);
        });
    factory.markFormatSupportsSubsetOfColumns("Vortex");

    factory.setDocumentation("Vortex", Documentation{.description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Vortex](https://vortex.dev/) is an extensible columnar file format for compressed Apache Arrow-compatible data,
designed for fast scans and random access. ClickHouse supports reading and writing Vortex files.

## Data types matching {#data-types-matching-vortex}

The table below shows the Vortex data types and the corresponding ClickHouse [data types](/reference/data-types/index)
in `INSERT` and `SELECT` queries.

| Vortex data type (`INSERT`)         | ClickHouse data type                                          | Vortex data type (`SELECT`) |
|-------------------------------------|---------------------------------------------------------------|-----------------------------|
| `Bool`                              | [Bool](/reference/data-types/boolean)                         | `Bool`                      |
| `I8`, `U8`                          | [Int8/UInt8](/reference/data-types/int-uint)                  | `I8`, `U8`                  |
| `I16`, `U16`                        | [Int16/UInt16](/reference/data-types/int-uint)                | `I16`, `U16`                |
| `I32`, `U32`                        | [Int32/UInt32](/reference/data-types/int-uint)                | `I32`, `U32`                |
| `I64`, `U64`                        | [Int64/UInt64](/reference/data-types/int-uint)                | `I64`, `U64`                |
| `F32`                               | [Float32](/reference/data-types/float)                        | `F32`                       |
| `F64`                               | [Float64](/reference/data-types/float)                        | `F64`                       |
| `Utf8`, `Binary`                    | [String](/reference/data-types/string)                        | `Binary`                    |
| `Binary`                            | [FixedString](/reference/data-types/fixedstring)              | `Binary`                    |
| `Decimal`                           | [Decimal](/reference/data-types/decimal)                      | `Decimal`                   |
| `vortex.date`                       | [Date32](/reference/data-types/date32)                        | `vortex.date`               |
| `vortex.timestamp`                  | [DateTime](/reference/data-types/datetime)/[DateTime64](/reference/data-types/datetime64) | `vortex.timestamp` |
| `vortex.time`                       | [Time64](/reference/data-types/time64)                        | `vortex.time`               |
| `List`                              | [Array](/reference/data-types/array)                          | `List`                      |
| `Struct`                            | [Tuple](/reference/data-types/tuple)                          | `Struct`                    |
| `Null`                              | [Nullable(Nothing)](/reference/data-types/special-data-types/nothing) | `Null`             |

Other types are not supported. In particular, [Map](/reference/data-types/map),
[Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint), [IPv6](/reference/data-types/ipv6)
and [Interval](/reference/data-types/special-data-types/interval) columns cannot be written to Vortex files.
[String](/reference/data-types/string) columns are written as `Binary` because ClickHouse strings are
arbitrary byte sequences, while Vortex requires `Utf8` values to be valid UTF-8. Vortex has no
fixed-size binary type, so [FixedString](/reference/data-types/fixedstring) is also written as `Binary`;
schema inference reads it back as [String](/reference/data-types/string).
[LowCardinality](/reference/data-types/lowcardinality) columns are written as their underlying type
(Vortex chooses dictionary and other encodings adaptively by itself).
[DateTime](/reference/data-types/datetime) columns are written as `vortex.timestamp` with second precision,
so they are read back as [DateTime64](/reference/data-types/datetime64) with scale 0.
[IPv4](/reference/data-types/ipv4) columns are written as `U32` because Vortex has no type for IP addresses,
so schema inference reads them back as [UInt32](/reference/data-types/int-uint). Specify the type explicitly
to read such a column back as `IPv4`: `SELECT * FROM file('data.vortex', Vortex, 'ip IPv4')`.

The data types of ClickHouse table columns do not have to match the corresponding Vortex data fields.
When inserting data, ClickHouse interprets data types according to the table above and then
[casts](/reference/functions/regular-functions/type-conversion-functions#CAST) the data to the data type set for the
ClickHouse table column.

## Example usage {#example-usage}

You can select data from a Vortex file:

```sql
SELECT * FROM file('data.vortex');
```

And write data to a Vortex file:

```sql
SELECT * FROM numbers(3) INTO OUTFILE 'numbers.vortex' FORMAT Vortex;
```

## Format settings {#format-settings}

| Setting                                  | Description                                                          | Default |
|------------------------------------------|----------------------------------------------------------------------|---------|
| `input_format_vortex_filter_push_down`   | Push translatable parts of the `WHERE` condition down into the scan, which may reduce the rows decoded by selective queries. ClickHouse reapplies the full filter after the scan. Whole segments are not yet pruned by statistics. Pushdown currently supports top-level integer, floating-point, and string/binary columns. | `1` |
| `input_format_vortex_preserve_order`     | Return the rows in file order. By default the row splits of a file are decoded in parallel and returned as soon as they are ready, so the row order is not guaranteed; with the setting a slow split holds back the ones after it. | `0` |
| `max_parsing_threads`                    | The number of threads that decode Vortex files (shared by the files read in parallel by one query). `1` disables the thread pool: the file is then read inside `read`, on the thread of the query pipeline. | number of cores |
| `max_download_threads`                   | The number of threads that read the file. `0` makes the reads share the decoding threads. | `4` |

As in other columnar formats, only the columns used by the query are read from the file, and
columns missing in the file are filled with default values.

## Performance {#performance}

A file is read in parallel. The scan splits it into row ranges (aligned to the chunk boundaries of
the requested columns, at most 100 000 rows each) that are read, filtered and decoded concurrently:
the decoding runs on up to `max_parsing_threads` threads of the same pool the `Parquet` reader uses,
the reads on up to `max_download_threads` threads, and the conversion of a decoded chunk to
ClickHouse columns happens on the thread that decoded it. Chunks are returned as soon as they are
ready, so the row order is not guaranteed unless `input_format_vortex_preserve_order` is set. The
reads of segments that are close in the file are merged into one request (up to 4 MiB for local
files and 16 MiB for remote storage). Filter pushdown reduces the amount of data decoded by
selective queries.
)DOCS_MD"});
}

void registerVortexSchemaReader(FormatFactory & factory);
void registerVortexSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Vortex",
        [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        {
            return std::make_shared<VortexSchemaReader>(buf, settings);
        });

    /// The inferred types depend on these settings, so they must be a part of the schema cache key.
    factory.registerAdditionalInfoForSchemaCacheGetter(
        "Vortex",
        [](const FormatSettings & settings)
        {
            return fmt::format(
                "schema_inference_make_columns_nullable={};schema_inference_make_json_columns_nullable={};"
                "schema_inference_allow_nullable_tuple_type={};"
                "allow_geoparquet_parser={}",
                settings.schema_inference_make_columns_nullable,
                settings.schema_inference_make_json_columns_nullable,
                settings.schema_inference_allow_nullable_tuple_type,
                settings.parquet.allow_geoparquet_parser);
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatVortex(FormatFactory &);
void registerInputFormatVortex(FormatFactory &) {}

void registerVortexSchemaReader(FormatFactory &);
void registerVortexSchemaReader(FormatFactory &) {}
}

#endif
