#include <Processors/Formats/Impl/DWARFBlockInputFormat.h>
#include <Common/CurrentThread.h>
#if USE_DWARF_PARSER && defined(__ELF__) && !defined(OS_FREEBSD)

#include <Formats/FormatFactory.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnIndex.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnUnique.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/castColumnToRequestedType.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/SharedThreadPools.h>
#include <IO/WithFileName.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/copyData.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/PODArray.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_ELF;
    extern const int CANNOT_PARSE_DWARF;
    extern const int THERE_IS_NO_COLUMN;
}

enum DwarfColumn
{
    COL_OFFSET,
    COL_SIZE,
    COL_TAG,
    COL_UNIT_NAME,
    COL_UNIT_OFFSET,

    COL_ANCESTOR_TAGS,
    COL_ANCESTOR_OFFSETS,

    /// A few very common attributes get their own columns, just for convenience.
    /// We put their values *both* in the dedicated columns and in the attr_str/attr_int arrays.
    /// This duplication wastes considerable time and space (tens of percent), but I can't think of
    /// an alternative that wouldn't be really inconvenient or confusing:
    ///  * omitting these attributes from the arrays would make collecting attribute stats inconvenient,
    ///    and would lose information about the form of the attribute,
    ///  * using empty value for the attribute would be confusing and error-prone, e.g. when collecting stats
    ///    about all attribute values the user would need to add these columns too, somehow,
    ///  * not having these dedicated columns would make it inconvenient to look up entry name/file/line.
    ///    (But maybe that's fine? I.e. maybe it's not very commonly used and maybe the array lookup is not that inconvenient? Idk.)

    COL_NAME,
    COL_LINKAGE_NAME,
    COL_DECL_FILE,
    COL_DECL_LINE,
    COL_RANGES,

    COL_ATTR_NAME,
    COL_ATTR_FORM,
    COL_ATTR_INT,
    COL_ATTR_STR,

    COL_COUNT,
};

static NamesAndTypesList getHeaderForDWARF()
{
    NamesAndTypes cols(COL_COUNT);
    cols[COL_OFFSET] = {"offset", std::make_shared<DataTypeUInt64>()};
    cols[COL_SIZE] = {"size", std::make_shared<DataTypeUInt32>()};
    cols[COL_TAG] = {"tag", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())};
    cols[COL_UNIT_NAME] = {"unit_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())};
    cols[COL_UNIT_OFFSET] = {"unit_offset", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>())};
    cols[COL_ANCESTOR_TAGS] = {"ancestor_tags", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    cols[COL_ANCESTOR_OFFSETS] = {"ancestor_offsets", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    cols[COL_NAME] = {"name", std::make_shared<DataTypeString>()};
    cols[COL_LINKAGE_NAME] = {"linkage_name", std::make_shared<DataTypeString>()};
    cols[COL_DECL_FILE] = {"decl_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())};
    cols[COL_DECL_LINE] = {"decl_line", std::make_shared<DataTypeUInt32>()};
    cols[COL_RANGES] = {"ranges", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()}))};
    cols[COL_ATTR_NAME] = {"attr_name", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    cols[COL_ATTR_FORM] = {"attr_form", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    cols[COL_ATTR_INT] = {"attr_int", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    cols[COL_ATTR_STR] = {"attr_str", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    return NamesAndTypesList(cols.begin(), cols.end());
}

static const std::unordered_map<std::string, size_t> & getColumnNameToIdx()
{
    static std::once_flag once;
    static std::unordered_map<std::string, size_t> name_to_idx;
    std::call_once(once, [&] {
        size_t i = 0;
        for (const auto & c : getHeaderForDWARF())
        {
            name_to_idx.emplace(c.name, i);
            ++i;
        }
    });
    return name_to_idx;
}

static const DataTypes & getColumnTypes()
{
    static const DataTypes types = []
    {
        DataTypes res;
        for (const auto & c : getHeaderForDWARF())
            res.push_back(c.type);
        return res;
    }();
    return types;
}

/// The DWARF format has a fixed set of columns, so a name that is not one of them can not be read.
static size_t getColumnIdx(const std::string & name)
{
    const auto & name_to_idx = getColumnNameToIdx();
    auto it = name_to_idx.find(name);
    if (it == name_to_idx.end())
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", name);
    return it->second;
}

static_assert(COL_COUNT == 16);
static_assert(CH_DWARF_COL_OFFSET == (1u << COL_OFFSET));
static_assert(CH_DWARF_COL_SIZE == (1u << COL_SIZE));
static_assert(CH_DWARF_COL_TAG == (1u << COL_TAG));
static_assert(CH_DWARF_COL_UNIT_NAME == (1u << COL_UNIT_NAME));
static_assert(CH_DWARF_COL_UNIT_OFFSET == (1u << COL_UNIT_OFFSET));
static_assert(CH_DWARF_COL_ANCESTOR_TAGS == (1u << COL_ANCESTOR_TAGS));
static_assert(CH_DWARF_COL_ANCESTOR_OFFSETS == (1u << COL_ANCESTOR_OFFSETS));
static_assert(CH_DWARF_COL_NAME == (1u << COL_NAME));
static_assert(CH_DWARF_COL_LINKAGE_NAME == (1u << COL_LINKAGE_NAME));
static_assert(CH_DWARF_COL_DECL_FILE == (1u << COL_DECL_FILE));
static_assert(CH_DWARF_COL_DECL_LINE == (1u << COL_DECL_LINE));
static_assert(CH_DWARF_COL_RANGES == (1u << COL_RANGES));
static_assert(CH_DWARF_COL_ATTR_NAME == (1u << COL_ATTR_NAME));
static_assert(CH_DWARF_COL_ATTR_FORM == (1u << COL_ATTR_FORM));
static_assert(CH_DWARF_COL_ATTR_INT == (1u << COL_ATTR_INT));
static_assert(CH_DWARF_COL_ATTR_STR == (1u << COL_ATTR_STR));

namespace
{

/// Rows per chunk, as in the previous parser.
constexpr size_t MAX_ROWS_PER_CHUNK = 65536;
/// Entries of each tag/attribute/form name table; the value is the index into the table.
constexpr size_t NAME_TABLE_SIZE = 65536;
/// The first allocation of any sink is at least this big, to avoid a chain of tiny reallocations.
constexpr size_t MIN_SINK_BYTES = 4096;
/// A sanity bound for one reservation request; real allocations are limited by the memory tracker.
constexpr size_t MAX_SINK_BYTES = size_t(1) << 46;

/// Element width of every sink, as required by dwarf_rust.h.
constexpr std::array<uint32_t, CH_DWARF_SINK_COUNT> SINK_ELEM_SIZE = {
    8, /// OFFSET
    4, /// SIZE
    2, /// TAG
    2, /// ANCESTOR_TAGS
    8, /// ANCESTOR_OFFSETS
    8, /// ANCESTOR_ARRAY_OFFSETS
    1, /// NAME_CHARS
    8, /// NAME_OFFSETS
    1, /// LINKAGE_NAME_CHARS
    8, /// LINKAGE_NAME_OFFSETS
    1, /// DECL_FILE_CHARS
    8, /// DECL_FILE_OFFSETS
    4, /// DECL_LINE
    8, /// RANGES_START
    8, /// RANGES_END
    8, /// RANGES_OFFSETS
    2, /// ATTR_NAME
    2, /// ATTR_FORM
    8, /// ATTR_INT
    1, /// ATTR_STR_CHARS
    8, /// ATTR_STR_OFFSETS
    8, /// ATTR_OFFSETS
};

/// A type-erased handle to the `PaddedPODArray` of a ClickHouse column that backs one Rust sink.
struct SinkBinding
{
    void * array = nullptr;
    uint32_t elem_size = 0;

    bool isActive() const { return array != nullptr; }
};

template <typename T>
SinkBinding bindArray(PaddedPODArray<T> & array)
{
    return SinkBinding{&array, static_cast<uint32_t>(sizeof(T))};
}

template <typename Func>
void visitArray(const SinkBinding & binding, Func && func)
{
    switch (binding.elem_size)
    {
        case 1:
            func(*static_cast<PaddedPODArray<UInt8> *>(binding.array));
            return;
        case 2:
            func(*static_cast<PaddedPODArray<UInt16> *>(binding.array));
            return;
        case 4:
            func(*static_cast<PaddedPODArray<UInt32> *>(binding.array));
            return;
        case 8:
            func(*static_cast<PaddedPODArray<UInt64> *>(binding.array));
            return;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected DWARF sink element size {}", binding.elem_size);
    }
}

/// Describes a freshly created (empty) array to Rust.
void describeEmptySink(const SinkBinding & binding, ChDwarfSink & sink)
{
    visitArray(binding, [&](auto & array)
    {
        if (!array.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A DWARF sink must start empty");
        sink.data = array.capacity() ? reinterpret_cast<uint8_t *>(array.data()) : nullptr;
        sink.len = 0;
        sink.cap = array.capacity();
        sink.elem_size = binding.elem_size;
        sink.reserved = 0;
    });
}

/// Grows the array to at least `min_cap` elements (geometrically, `PODArray` rounds the allocation up to a
/// power of two), keeping the `sink.len` elements Rust has initialized so far, and refreshes the descriptor.
void growSink(const SinkBinding & binding, ChDwarfSink & sink, size_t min_cap)
{
    if (!binding.isActive())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser asked to reserve space in a sink that is not part of the projection");
    if (sink.elem_size != binding.elem_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The DWARF parser changed the element size of a sink from {} to {}", binding.elem_size, sink.elem_size);

    visitArray(binding, [&](auto & array)
    {
        if (sink.len > array.capacity() || sink.cap != array.capacity())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The DWARF parser passed an inconsistent sink descriptor: len {}, cap {}, actual cap {}",
                sink.len,
                sink.cap,
                array.capacity());

        if (min_cap > array.capacity())
        {
            const size_t wanted = std::max(min_cap, MIN_SINK_BYTES / binding.elem_size);
            if (wanted > MAX_SINK_BYTES / binding.elem_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser asked for an unreasonable sink capacity of {} elements", wanted);

            /// `PODArray` only preserves the elements up to `size()`, so expose what Rust has written.
            array.resize_assume_reserved(sink.len);
            array.reserve(wanted);
        }

        sink.data = array.capacity() ? reinterpret_cast<uint8_t *>(array.data()) : nullptr;
        sink.cap = array.capacity();
    });
}

/// Makes the array's size equal to the number of elements Rust has initialized.
void commitSink(const SinkBinding & binding, const ChDwarfSink & sink)
{
    if (sink.elem_size != binding.elem_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The DWARF parser changed the element size of a sink from {} to {}", binding.elem_size, sink.elem_size);

    visitArray(binding, [&](auto & array)
    {
        const uint8_t * expected_data = array.capacity() ? reinterpret_cast<const uint8_t *>(array.data()) : nullptr;
        if (sink.len > array.capacity() || sink.cap != array.capacity() || sink.data != expected_data)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The DWARF parser returned an inconsistent sink descriptor: len {}, cap {}, actual cap {}",
                sink.len,
                sink.cap,
                array.capacity());

        array.resize_assume_reserved(sink.len);
    });
}

/// State shared with the C callbacks of one Rust call. Callbacks must not throw across the FFI boundary,
/// so they store the first exception here and report a failure status; the caller rethrows it.
struct RustCallbackContext
{
    explicit RustCallbackContext(const std::atomic<int> & is_stopped_) : is_stopped(is_stopped_) {}

    const std::atomic<int> & is_stopped;
    std::exception_ptr exception;

    /// Output arrays of one chunk, by sink id; inactive sinks have no array.
    std::array<SinkBinding, CH_DWARF_SINK_COUNT> sinks;

    /// Output arrays of a name table.
    SinkBinding name_chars;
    SinkBinding name_offsets;
};

struct ErrorBuffer
{
    char data[CH_DWARF_ERROR_BUF_SIZE] = {};

    const char * message()
    {
        data[CH_DWARF_ERROR_BUF_SIZE - 1] = '\0';
        return data[0] ? data : "no error message";
    }
};

}

extern "C"
{

static int32_t chDwarfReserveBatch(
    void * opaque, ChDwarfBatchSinks * sinks, const ChDwarfReserveRequest * requests, size_t request_count) noexcept
{
    auto & context = *static_cast<RustCallbackContext *>(opaque);
    try
    {
        for (size_t i = 0; i < request_count; ++i)
        {
            const ChDwarfReserveRequest & request = requests[i];
            if (request.sink_id >= CH_DWARF_SINK_COUNT)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser asked to reserve space in unknown sink {}", request.sink_id);
            growSink(context.sinks[request.sink_id], sinks->sinks[request.sink_id], request.min_cap);
        }
        return CH_DWARF_RESERVE_OK;
    }
    catch (...)
    {
        if (!context.exception)
            context.exception = std::current_exception();
        return CH_DWARF_RESERVE_FAILED;
    }
}

static int32_t chDwarfReserveNameTable(void * opaque, ChDwarfStringSink * sink, size_t min_chars_cap, size_t min_offsets_cap) noexcept
{
    auto & context = *static_cast<RustCallbackContext *>(opaque);
    try
    {
        growSink(context.name_chars, sink->chars, min_chars_cap);
        growSink(context.name_offsets, sink->offsets, min_offsets_cap);
        return CH_DWARF_RESERVE_OK;
    }
    catch (...)
    {
        if (!context.exception)
            context.exception = std::current_exception();
        return CH_DWARF_RESERVE_FAILED;
    }
}

static int32_t chDwarfCancel(void * opaque) noexcept
{
    auto & context = *static_cast<RustCallbackContext *>(opaque);
    if (context.is_stopped.load())
        return CH_DWARF_CANCEL_STOP;

    try
    {
        /// Rust allocates only a bounded amount of scratch memory itself; this is where its work is
        /// accounted against the query's memory limit and where the limit exception is raised.
        CurrentMemoryTracker::check();
    }
    catch (...)
    {
        if (!context.exception)
            context.exception = std::current_exception();
        return CH_DWARF_CANCEL_FAILED;
    }
    return CH_DWARF_CANCEL_CONTINUE;
}

}

namespace
{

/// Converts a failed Rust status into an exception. The first exception stored by a callback is the root
/// cause of any callback failure, so it takes precedence over the generic status.
[[noreturn]] void throwDwarfStatus(int32_t status, ErrorBuffer & error, const RustCallbackContext * context)
{
    if (context && context->exception)
        std::rethrow_exception(context->exception);

    switch (status)
    {
        case CH_DWARF_ERR_ELF:
        case CH_DWARF_ERR_MISSING_REQUIRED_SECTION:
            throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "{}", error.message());
        case CH_DWARF_ERR_DWARF:
        case CH_DWARF_ERR_MISSING_AUX_SECTION:
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "{}", error.message());
        case CH_DWARF_ERR_CALLBACK:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser reported a callback failure, but no callback failed: {}", error.message());
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser failed with status {}: {}", status, error.message());
    }
}

/// Builds a `ColumnUnique` with the 65536 names of tags/attributes/forms, produced by Rust in a single call.
ColumnPtr makeNameDictionary(int32_t kind)
{
    auto names = ColumnString::create();

    const std::atomic<int> never_stopped{0};
    RustCallbackContext context(never_stopped);
    context.name_chars = bindArray(names->getChars());
    context.name_offsets = bindArray(names->getOffsets());

    ChDwarfStringSink sink{};
    describeEmptySink(context.name_chars, sink.chars);
    describeEmptySink(context.name_offsets, sink.offsets);

    ErrorBuffer error;
    int32_t status = ch_dwarf_write_name_table(kind, &sink, chDwarfReserveNameTable, &context, error.data);
    if (status != CH_DWARF_OK)
        throwDwarfStatus(status, error, &context);

    commitSink(context.name_chars, sink.chars);
    commitSink(context.name_offsets, sink.offsets);
    if (names->size() != NAME_TABLE_SIZE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser produced {} names instead of {}", names->size(), NAME_TABLE_SIZE);
    if (names->getOffsets().back() != names->getChars().size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser produced an inconsistent name table");

    return ColumnUnique<ColumnString>::create(std::move(names), /*is_nullable*/ false);
}

const ColumnPtr & getSharedNameDictionary(int32_t kind)
{
    switch (kind)
    {
        case CH_DWARF_NAME_TABLE_TAG:
        {
            static const ColumnPtr dict = [] {
                MemoryTrackerBlockerInThread blocker;
                return makeNameDictionary(CH_DWARF_NAME_TABLE_TAG);
            }();
            return dict;
        }
        case CH_DWARF_NAME_TABLE_ATTR:
        {
            static const ColumnPtr dict = [] {
                MemoryTrackerBlockerInThread blocker;
                return makeNameDictionary(CH_DWARF_NAME_TABLE_ATTR);
            }();
            return dict;
        }
        case CH_DWARF_NAME_TABLE_FORM:
        {
            static const ColumnPtr dict = [] {
                MemoryTrackerBlockerInThread blocker;
                return makeNameDictionary(CH_DWARF_NAME_TABLE_FORM);
            }();
            return dict;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected DWARF name table kind {}", kind);
    }
}

/// Converts a plain string column to `LowCardinality(String)` in bulk, fast-pathing empty strings
/// (which always map to dictionary index 0) and consecutive duplicate strings (such as `decl_file`).
MutableColumnPtr makeLowCardinalityString(const ColumnString & full_column)
{
    auto holder = ColumnString::create();
    holder->insertDefault();
    auto unique = ColumnUnique<ColumnString>::create(std::move(holder), /*is_nullable*/ false);
    const auto & chars = full_column.getChars();
    const auto & offsets = full_column.getOffsets();
    const size_t n = offsets.size();

    if (chars.empty())
    {
        auto positions_zero = ColumnVector<UInt16>::create(n, static_cast<UInt16>(0));
        return ColumnLowCardinality::create(MutableColumnPtr(std::move(unique)), MutableColumnPtr(std::move(positions_zero)), /*is_shared=*/false);
    }

    auto positions_u16 = ColumnVector<UInt16>::create(n);
    auto & pos16 = positions_u16->getData();
    const char * prev_ptr = nullptr;
    size_t prev_len = 0;
    size_t prev_idx = 0;
    size_t prev_off = 0;

    for (size_t i = 0; i < n; ++i)
    {
        const size_t end_off = offsets[i];
        const size_t len = end_off - prev_off;
        size_t idx = 0;
        if (len != 0)
        {
            const char * ptr = reinterpret_cast<const char *>(chars.data() + prev_off);
            if (prev_ptr != nullptr && len == prev_len && memcmp(ptr, prev_ptr, len) == 0)
            {
                idx = prev_idx;
            }
            else
            {
                idx = unique->uniqueInsertData(ptr, len);
                prev_ptr = ptr;
                prev_len = len;
                prev_idx = idx;
            }
            if (unlikely(idx > UINT16_MAX))
            {
                auto result = ColumnLowCardinality::create(
                    MutableColumnPtr(ColumnUnique<ColumnString>::create(ColumnString::create()->cloneResized(1), /*is_nullable*/ false)),
                    MutableColumnPtr(ColumnVector<UInt16>::create()),
                    /*is_shared=*/false);
                result->insertRangeFromFullColumn(full_column, 0, full_column.size());
                return result;
            }
        }
        pos16[i] = static_cast<UInt16>(idx);
        prev_off = end_off;
    }

    return ColumnLowCardinality::create(MutableColumnPtr(std::move(unique)), MutableColumnPtr(std::move(positions_u16)), /*is_shared=*/false);
}

}

DWARFBlockInputFormat::DWARFBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_, size_t num_threads_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_), num_threads(num_threads_)
{
    /// Reject unknown column names up front instead of failing in the middle of parsing.
    for (const std::string & name : getPort().getHeader().getNames())
    {
        size_t idx = getColumnIdx(name);
        output_columns.push_back(idx);
        need[idx] = true;
    }

    if (need[COL_TAG] || need[COL_ANCESTOR_TAGS])
        tag_dict_column = getSharedNameDictionary(CH_DWARF_NAME_TABLE_TAG);
    if (need[COL_ATTR_NAME])
        attr_name_dict_column = getSharedNameDictionary(CH_DWARF_NAME_TABLE_ATTR);
    if (need[COL_ATTR_FORM])
        attr_form_dict_column = getSharedNameDictionary(CH_DWARF_NAME_TABLE_FORM);
    for (size_t i = 0; i < COL_COUNT; ++i)
    {
        if (need[i])
            projection.column_mask |= 1u << i;
    }
    const bool need_ancestors = need[COL_ANCESTOR_TAGS] || need[COL_ANCESTOR_OFFSETS];
    const bool need_attrs = need[COL_ATTR_NAME] || need[COL_ATTR_FORM] || need[COL_ATTR_INT] || need[COL_ATTR_STR];
    projection.need_ancestor_offsets_vec = need_ancestors ? 1 : 0;
    projection.need_attr_offsets_vec = need_attrs ? 1 : 0;

    sink_active[CH_DWARF_SINK_OFFSET] = need[COL_OFFSET];
    sink_active[CH_DWARF_SINK_SIZE] = need[COL_SIZE];
    sink_active[CH_DWARF_SINK_TAG] = need[COL_TAG];
    sink_active[CH_DWARF_SINK_ANCESTOR_TAGS] = need[COL_ANCESTOR_TAGS];
    sink_active[CH_DWARF_SINK_ANCESTOR_OFFSETS] = need[COL_ANCESTOR_OFFSETS];
    sink_active[CH_DWARF_SINK_ANCESTOR_ARRAY_OFFSETS] = need_ancestors;
    sink_active[CH_DWARF_SINK_NAME_CHARS] = sink_active[CH_DWARF_SINK_NAME_OFFSETS] = need[COL_NAME];
    sink_active[CH_DWARF_SINK_LINKAGE_NAME_CHARS] = sink_active[CH_DWARF_SINK_LINKAGE_NAME_OFFSETS] = need[COL_LINKAGE_NAME];
    sink_active[CH_DWARF_SINK_DECL_FILE_CHARS] = sink_active[CH_DWARF_SINK_DECL_FILE_OFFSETS] = need[COL_DECL_FILE];
    sink_active[CH_DWARF_SINK_DECL_LINE] = need[COL_DECL_LINE];
    sink_active[CH_DWARF_SINK_RANGES_START] = sink_active[CH_DWARF_SINK_RANGES_END] = sink_active[CH_DWARF_SINK_RANGES_OFFSETS]
        = need[COL_RANGES];
    sink_active[CH_DWARF_SINK_ATTR_NAME] = need[COL_ATTR_NAME];
    sink_active[CH_DWARF_SINK_ATTR_FORM] = need[COL_ATTR_FORM];
    sink_active[CH_DWARF_SINK_ATTR_INT] = need[COL_ATTR_INT];
    sink_active[CH_DWARF_SINK_ATTR_STR_CHARS] = sink_active[CH_DWARF_SINK_ATTR_STR_OFFSETS] = need[COL_ATTR_STR];
    sink_active[CH_DWARF_SINK_ATTR_OFFSETS] = need_attrs;
}

DWARFBlockInputFormat::~DWARFBlockInputFormat()
{
    stopThreads();
    releaseState();
}

void DWARFBlockInputFormat::onCancel() noexcept
{
    {
        /// Taking the mutex orders the flag with the waiters' predicate checks, so that no wake-up is lost.
        std::lock_guard lock(mutex);
        is_stopped = 1;
    }
    deliver_chunk.notify_all();
    wake_up_threads.notify_all();
}

bool DWARFBlockInputFormat::loadInput()
{
    /// If it's a local file, mmap it. The ReadBuffer does not expose its descriptor, so (like the previous
    /// parser) we map a dedicated read-only view of the whole file by its path.
    if (ReadBufferFromFileBase * file_in = dynamic_cast<ReadBufferFromFileBase *>(in))
    {
        size_t offset = 0;
        if (file_in->isRegularLocalFile(&offset) && offset == 0)
        {
            mapped_file.emplace(file_in->getFileName(), 0);
            input_data = reinterpret_cast<const uint8_t *>(mapped_file->getData());
            input_size = mapped_file->getLength();
            return true;
        }
    }

    /// If can't mmap, read the entire input into memory. It is never resized after this point.
    {
        auto buf = WriteBufferFromVector<PODArray<char>>(file_contents);
        copyData(*in, buf, is_stopped);
        buf.finalize();
    }
    if (is_stopped)
    {
        PODArray<char> empty;
        file_contents.swap(empty);
        return false;
    }
    input_data = reinterpret_cast<const uint8_t *>(file_contents.data());
    input_size = file_contents.size();
    return true;
}

void DWARFBlockInputFormat::initializeIfNeeded()
{
    if (initialized || is_stopped)
        return;

    LOG_DEBUG(getLogger("DWARF"), "Opening ELF");

    bool opened = false;
    SCOPE_EXIT({
        if (!opened)
            releaseState();
    });

    if (!loadInput())
        return; /// Cancelled while reading the input; the caller sees `is_stopped`.

    ErrorBuffer error;
    {
        ChDwarfOptions options{};
        ChDwarfFile * raw_file = nullptr;
        int32_t status = ch_dwarf_file_open(input_data, input_size, &options, &raw_file, error.data);
        file.reset(raw_file); /// Owned from here on, even on error.
        if (status != CH_DWARF_OK)
            throwDwarfStatus(status, error, nullptr);
        if (!file)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser returned no file handle");
    }
    CurrentMemoryTracker::check();

    size_t unit_count = 0;
    {
        int32_t status = ch_dwarf_file_unit_count(file.get(), &unit_count);
        if (status != CH_DWARF_OK)
            throwDwarfStatus(status, error, nullptr);
    }

    for (size_t i = 0; i < unit_count; ++i)
    {
        UnitState unit;
        ChDwarfUnit * raw_unit = nullptr;
        uint64_t unit_offset = 0;
        int32_t status = ch_dwarf_unit_open(file.get(), i, &raw_unit, &unit_offset, error.data);
        unit.handle.reset(raw_unit); /// Owned from here on, even on error.
        if (status != CH_DWARF_OK)
            throwDwarfStatus(status, error, nullptr);
        if (!unit.handle)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser returned no unit handle");
        CurrentMemoryTracker::check();

        unit.unit_offset = unit_offset;
        units_queue.push_back(std::move(unit));
    }

    LOG_DEBUG(getLogger("DWARF"), "{} units, reading in {} threads", units_queue.size(), num_threads);

    /// From here on the destructor/`resetParser` own the cleanup, after joining the workers.
    initialized = true;
    opened = true;

    runner.emplace(getFormatParsingThreadPool().get(), ThreadName::DWARF_DECODER);
    for (size_t i = 0; i < num_threads; ++i)
        runner.value().enqueueAndKeepTrack(
            [this, thread_group = CurrentThread::getGroup()]()
            {
                try
                {
                    std::unique_lock lock(mutex);
                    while (!units_queue.empty() && !is_stopped)
                    {
                        if (delivery_queue.size() > num_threads)
                        {
                            wake_up_threads.wait(lock);
                            continue;
                        }
                        UnitState unit = std::move(units_queue.front());
                        units_queue.pop_front();
                        ++units_in_progress;

                        lock.unlock();

                        ChunkResult result = parseChunk(unit);

                        lock.lock();

                        --units_in_progress;
                        if (result.chunk)
                        {
                            delivery_queue.emplace_back(std::move(result.chunk), result.bytes_advanced);
                            deliver_chunk.notify_one();
                        }
                        if (!result.finished)
                        {
                            units_queue.push_front(std::move(unit));
                        }
                        else
                        {
                            /// The last unit may complete without a final chunk, and `read` must notice.
                            deliver_chunk.notify_all();
                        }
                    }
                }
                catch (...)
                {
                    std::lock_guard lock(mutex);
                    background_exception = std::current_exception();
                    deliver_chunk.notify_all();
                }
            });
}

void DWARFBlockInputFormat::stopThreads()
{
    {
        std::unique_lock lock(mutex); // required even if is_stopped is atomic
        is_stopped = true;
    }
    wake_up_threads.notify_all();
    if (runner)
        runner->waitForAllToFinishAndRethrowFirstError();
}

void DWARFBlockInputFormat::releaseState()
{
    /// Destruction order: workers are already joined; free the unit handles, then the file handle,
    /// and only then the bytes they point into.
    delivery_queue.clear();
    units_queue.clear();
    file.reset();
    mapped_file.reset();
    {
        PODArray<char> empty;
        file_contents.swap(empty);
    }
    input_data = nullptr;
    input_size = 0;
}

DWARFBlockInputFormat::ChunkResult DWARFBlockInputFormat::parseChunk(UnitState & unit)
{
    RustCallbackContext context(is_stopped);

    /// Fresh empty column buffers; Rust appends to the arrays of the ones the projection needs.
    auto col_offset = ColumnVector<UInt64>::create();
    auto col_size = ColumnVector<UInt32>::create();
    auto col_tag = ColumnVector<UInt16>::create();
    auto col_ancestor_tags = ColumnVector<UInt16>::create();
    auto col_ancestor_dwarf_offsets = ColumnVector<UInt64>::create();
    auto col_ancestor_array_offsets = ColumnVector<UInt64>::create();
    auto col_name = ColumnString::create();
    auto col_linkage_name = ColumnString::create();
    auto col_decl_file = ColumnString::create(); /// full strings, converted to LowCardinality below
    auto col_decl_line = ColumnVector<UInt32>::create();
    auto col_ranges_start = ColumnVector<UInt64>::create();
    auto col_ranges_end = ColumnVector<UInt64>::create();
    auto col_ranges_offsets = ColumnVector<UInt64>::create();
    auto col_attr_name = ColumnVector<UInt16>::create();
    auto col_attr_form = ColumnVector<UInt16>::create();
    auto col_attr_int = ColumnVector<UInt64>::create();
    auto col_attr_str = ColumnString::create(); /// full strings, converted to LowCardinality below
    auto col_attr_offsets = ColumnVector<UInt64>::create();

    auto bind = [&](ChDwarfSinkId id, auto & array)
    {
        if (sink_active[id])
            context.sinks[id] = bindArray(array);
    };
    bind(CH_DWARF_SINK_OFFSET, col_offset->getData());
    bind(CH_DWARF_SINK_SIZE, col_size->getData());
    bind(CH_DWARF_SINK_TAG, col_tag->getData());
    bind(CH_DWARF_SINK_ANCESTOR_TAGS, col_ancestor_tags->getData());
    bind(CH_DWARF_SINK_ANCESTOR_OFFSETS, col_ancestor_dwarf_offsets->getData());
    bind(CH_DWARF_SINK_ANCESTOR_ARRAY_OFFSETS, col_ancestor_array_offsets->getData());
    bind(CH_DWARF_SINK_NAME_CHARS, col_name->getChars());
    bind(CH_DWARF_SINK_NAME_OFFSETS, col_name->getOffsets());
    bind(CH_DWARF_SINK_LINKAGE_NAME_CHARS, col_linkage_name->getChars());
    bind(CH_DWARF_SINK_LINKAGE_NAME_OFFSETS, col_linkage_name->getOffsets());
    bind(CH_DWARF_SINK_DECL_FILE_CHARS, col_decl_file->getChars());
    bind(CH_DWARF_SINK_DECL_FILE_OFFSETS, col_decl_file->getOffsets());
    bind(CH_DWARF_SINK_DECL_LINE, col_decl_line->getData());
    bind(CH_DWARF_SINK_RANGES_START, col_ranges_start->getData());
    bind(CH_DWARF_SINK_RANGES_END, col_ranges_end->getData());
    bind(CH_DWARF_SINK_RANGES_OFFSETS, col_ranges_offsets->getData());
    bind(CH_DWARF_SINK_ATTR_NAME, col_attr_name->getData());
    bind(CH_DWARF_SINK_ATTR_FORM, col_attr_form->getData());
    bind(CH_DWARF_SINK_ATTR_INT, col_attr_int->getData());
    bind(CH_DWARF_SINK_ATTR_STR_CHARS, col_attr_str->getChars());
    bind(CH_DWARF_SINK_ATTR_STR_OFFSETS, col_attr_str->getOffsets());
    bind(CH_DWARF_SINK_ATTR_OFFSETS, col_attr_offsets->getData());

    ChDwarfBatchSinks sinks{};
    for (size_t id = 0; id < CH_DWARF_SINK_COUNT; ++id)
    {
        if (sink_active[id])
        {
            if (context.sinks[id].elem_size != SINK_ELEM_SIZE[id])
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected element size of DWARF sink {}", id);
            describeEmptySink(context.sinks[id], sinks.sinks[id]);
        }
    }

    ChDwarfProgress progress{};
    ErrorBuffer error;
    int32_t status = ch_dwarf_unit_parse_chunk(
        file.get(),
        unit.handle.get(),
        &projection,
        &sinks,
        chDwarfReserveBatch,
        chDwarfCancel,
        &context,
        MAX_ROWS_PER_CHUNK,
        &progress,
        error.data);

    if (status == CH_DWARF_CANCELLED)
    {
        /// The partially filled batch is discarded together with the local columns.
        if (!is_stopped)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser reported cancellation, but the read was not cancelled");
        ChunkResult cancelled;
        cancelled.finished = true;
        return cancelled;
    }
    if (status != CH_DWARF_CHUNK_READY && status != CH_DWARF_UNIT_DONE)
        throwDwarfStatus(status, error, &context);
    if (context.exception)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A DWARF parser callback failed, but the parser reported success");

    const bool unit_done = status == CH_DWARF_UNIT_DONE;
    const size_t num_rows = progress.rows;
    if (progress.unit_done != (unit_done ? 1u : 0u))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser returned an inconsistent unit completion flag");
    if (num_rows > MAX_ROWS_PER_CHUNK)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser returned {} rows, more than {}", num_rows, MAX_ROWS_PER_CHUNK);
    if (num_rows == 0 && !unit_done)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser made no progress");

    /// Rust only keeps the unit name valid until the next call on this unit, so copy it.
    if (progress.unit_name_ptr)
        unit.unit_name.assign(reinterpret_cast<const char *>(progress.unit_name_ptr), progress.unit_name_len);

    ChunkResult result;
    result.bytes_advanced = progress.bytes_advanced;
    result.finished = unit_done;
    if (num_rows == 0)
        return result;

    /// Adopt what Rust initialized, then check that the parallel buffers are consistent.
    for (size_t id = 0; id < CH_DWARF_SINK_COUNT; ++id)
    {
        if (sink_active[id])
            commitSink(context.sinks[id], sinks.sinks[id]);
        else if (sinks.sinks[id].len != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The DWARF parser wrote to DWARF sink {}, which is not part of the projection", id);
    }

    auto expect_size = [&](ChDwarfSinkId id, size_t expected)
    {
        if (sink_active[id] && sinks.sinks[id].len != expected)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The DWARF parser returned {} elements in sink {}, expected {}",
                sinks.sinks[id].len,
                static_cast<int>(id),
                expected);
    };
    for (ChDwarfSinkId id :
         {CH_DWARF_SINK_OFFSET,
          CH_DWARF_SINK_SIZE,
          CH_DWARF_SINK_TAG,
          CH_DWARF_SINK_ANCESTOR_ARRAY_OFFSETS,
          CH_DWARF_SINK_NAME_OFFSETS,
          CH_DWARF_SINK_LINKAGE_NAME_OFFSETS,
          CH_DWARF_SINK_DECL_FILE_OFFSETS,
          CH_DWARF_SINK_DECL_LINE,
          CH_DWARF_SINK_RANGES_OFFSETS,
          CH_DWARF_SINK_ATTR_OFFSETS})
        expect_size(id, num_rows);

    if (sink_active[CH_DWARF_SINK_ANCESTOR_ARRAY_OFFSETS])
    {
        size_t count = col_ancestor_array_offsets->getData().back();
        expect_size(CH_DWARF_SINK_ANCESTOR_TAGS, count);
        expect_size(CH_DWARF_SINK_ANCESTOR_OFFSETS, count);
    }
    if (sink_active[CH_DWARF_SINK_RANGES_OFFSETS])
    {
        size_t count = col_ranges_offsets->getData().back();
        expect_size(CH_DWARF_SINK_RANGES_START, count);
        expect_size(CH_DWARF_SINK_RANGES_END, count);
    }
    if (sink_active[CH_DWARF_SINK_ATTR_OFFSETS])
    {
        size_t count = col_attr_offsets->getData().back();
        expect_size(CH_DWARF_SINK_ATTR_NAME, count);
        expect_size(CH_DWARF_SINK_ATTR_FORM, count);
        expect_size(CH_DWARF_SINK_ATTR_INT, count);
        expect_size(CH_DWARF_SINK_ATTR_STR_OFFSETS, count);
    }
    expect_size(CH_DWARF_SINK_NAME_CHARS, sink_active[CH_DWARF_SINK_NAME_CHARS] ? col_name->getOffsets().back() : 0);
    expect_size(CH_DWARF_SINK_LINKAGE_NAME_CHARS, sink_active[CH_DWARF_SINK_LINKAGE_NAME_CHARS] ? col_linkage_name->getOffsets().back() : 0);
    expect_size(CH_DWARF_SINK_DECL_FILE_CHARS, sink_active[CH_DWARF_SINK_DECL_FILE_CHARS] ? col_decl_file->getOffsets().back() : 0);
    expect_size(CH_DWARF_SINK_ATTR_STR_CHARS, sink_active[CH_DWARF_SINK_ATTR_STR_CHARS] ? col_attr_str->getOffsets().back() : 0);

    ColumnPtr immutable_attr_offsets = std::move(col_attr_offsets);
    ColumnPtr immutable_ancestor_array_offsets = std::move(col_ancestor_array_offsets);

    const auto & header = getPort().getHeader();
    Columns cols;
    cols.reserve(output_columns.size());
    for (size_t column_idx : output_columns)
    {
        switch (column_idx)
        {
            case COL_OFFSET:
                cols.push_back(std::exchange(col_offset, nullptr));
                break;
            case COL_SIZE:
                cols.push_back(std::exchange(col_size, nullptr));
                break;
            case COL_TAG:
                cols.push_back(ColumnLowCardinality::create(tag_dict_column, std::exchange(col_tag, nullptr), /*is_shared*/ true));
                break;
            case COL_UNIT_NAME:
            {
                auto dict = ColumnString::create();
                dict->insertDefault();
                dict->insertData(unit.unit_name.data(), unit.unit_name.size());
                auto index = ColumnVector<UInt8>::create();
                index->insert(1);
                auto indices = index->replicate({num_rows});
                cols.push_back(ColumnLowCardinality::create(
                    ColumnUnique<ColumnString>::create(std::move(dict), /*is_nullable*/ false), indices, /*is_shared*/ false));
                break;
            }
            case COL_UNIT_OFFSET:
            {
                auto dict = ColumnVector<UInt64>::create();
                dict->insertDefault();
                dict->insertValue(unit.unit_offset);
                auto index = ColumnVector<UInt8>::create();
                index->insert(1);
                auto indices = index->replicate({num_rows});
                cols.push_back(ColumnLowCardinality::create(
                    ColumnUnique<ColumnVector<UInt64>>::create(std::move(dict), /*is_nullable*/ false), indices, /*is_shared*/ false));
                break;
            }
            case COL_ANCESTOR_TAGS:
                cols.push_back(ColumnArray::create(
                    ColumnLowCardinality::create(tag_dict_column, std::exchange(col_ancestor_tags, nullptr), /*is_shared*/ true),
                    immutable_ancestor_array_offsets));
                break;
            case COL_ANCESTOR_OFFSETS:
                cols.push_back(ColumnArray::create(std::exchange(col_ancestor_dwarf_offsets, nullptr), immutable_ancestor_array_offsets));
                break;
            case COL_NAME:
                cols.push_back(std::exchange(col_name, nullptr));
                break;
            case COL_LINKAGE_NAME:
                cols.push_back(std::exchange(col_linkage_name, nullptr));
                break;
            case COL_DECL_FILE:
                cols.push_back(makeLowCardinalityString(*col_decl_file));
                break;
            case COL_DECL_LINE:
                cols.push_back(std::exchange(col_decl_line, nullptr));
                break;
            case COL_RANGES:
                cols.push_back(ColumnArray::create(
                    ColumnTuple::create(Columns{std::exchange(col_ranges_start, nullptr), std::exchange(col_ranges_end, nullptr)}),
                    std::exchange(col_ranges_offsets, nullptr)));
                break;
            case COL_ATTR_NAME:
                cols.push_back(ColumnArray::create(
                    ColumnLowCardinality::create(attr_name_dict_column, std::exchange(col_attr_name, nullptr), /*is_shared*/ true),
                    immutable_attr_offsets));
                break;
            case COL_ATTR_FORM:
                cols.push_back(ColumnArray::create(
                    ColumnLowCardinality::create(attr_form_dict_column, std::exchange(col_attr_form, nullptr), /*is_shared*/ true),
                    immutable_attr_offsets));
                break;
            case COL_ATTR_INT:
                cols.push_back(ColumnArray::create(std::exchange(col_attr_int, nullptr), immutable_attr_offsets));
                break;
            case COL_ATTR_STR:
                cols.push_back(ColumnArray::create(makeLowCardinalityString(*col_attr_str), immutable_attr_offsets));
                break;

            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column index");
        }
    }

    /// The requested header may declare types other than the ones produced above.
    const auto & native_types = getColumnTypes();
    const auto names = header.getNames();
    for (size_t pos = 0; pos < names.size(); ++pos)
    {
        ColumnWithTypeAndName column(cols[pos], native_types[output_columns[pos]], names[pos]);
        castColumnToRequestedType(column, header.getByName(names[pos]).type);
        cols[pos] = std::move(column.column);
    }

    result.chunk = Chunk(std::move(cols), num_rows);
    return result;
}

Chunk DWARFBlockInputFormat::read()
{
    initializeIfNeeded();

    std::unique_lock lock(mutex);
    bool ok = false;
    SCOPE_EXIT({
        if (!ok)
        {
            is_stopped = true;
            wake_up_threads.notify_all();
        }
    });

    while (true)
    {
        if (is_stopped)
            return {};
        if (background_exception)
            std::rethrow_exception(background_exception);

        if (!delivery_queue.empty())
        {
            Chunk chunk = std::move(delivery_queue.front().first);
            approx_bytes_read_for_chunk = delivery_queue.front().second;
            delivery_queue.pop_front();
            wake_up_threads.notify_one();
            ok = true;
            return chunk;
        }

        if (units_queue.empty() && units_in_progress == 0)
            return {};

        deliver_chunk.wait(lock);
    }
}

void DWARFBlockInputFormat::resetParser()
{
    stopThreads();

    runner.reset();
    releaseState();
    background_exception = nullptr;
    is_stopped = false;
    units_in_progress = 0;
    approx_bytes_read_for_chunk = 0;
    initialized = false;

    IInputFormat::resetParser();
}

DWARFSchemaReader::DWARFSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList DWARFSchemaReader::readSchema()
{
    return getHeaderForDWARF();
}

void registerDWARFSchemaReader(FormatFactory & factory);
void registerDWARFSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "DWARF",
        [](ReadBuffer & buf, const FormatSettings &)
        {
            return std::make_shared<DWARFSchemaReader>(buf);
        }
    );
}

void registerInputFormatDWARF(FormatFactory & factory);
void registerInputFormatDWARF(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "DWARF",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings &,
           bool /* is_remote_fs */,
           FormatParserSharedResourcesPtr parser_shared_resources,
           FormatFilterInfoPtr) -> InputFormatPtr
        {
            return std::make_shared<DWARFBlockInputFormat>(
                buf, std::make_shared<const Block>(sample), settings, parser_shared_resources->getParsingThreadsPerReader());
        });
    factory.markFormatSupportsSubsetOfColumns("DWARF");

    factory.setDocumentation("DWARF", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

The `DWARF` format parses DWARF (versions 2–5) debug symbols from an ELF64 little-endian binary that contains a program header table (such as an executable, shared library, or separate debug file; relocatable `.o` and `.dwo` object files without program headers are not supported).
It is similar to `dwarfdump`, but much faster (hundreds of MB/s) and supporting SQL.
It produces one row for each Debug Information Entry (DIE) in the `.debug_info` section
and includes "null"-entries that the DWARF encoding uses to terminate lists of children in the tree. Malformed unit headers, unclosed DIE trees, and out-of-bounds section references are rejected with a parsing exception.

<Info>
`.debug_info` consists of *units*, which correspond to compilation units:
- Each unit is a tree of *DIE*s, with a `compile_unit` DIE as its root.
- Each DIE has a *tag* and a list of *attributes*.
- Each attribute has a *name* and a *value* (and also a *form*, which specifies how the value is encoded).

The DIEs represent things from the source code, and their *tag* tells you what kind of thing it is. For example, there are:

- functions (tag = `subprogram`)
- classes/structs/enums (`class_type`/`structure_type`/`enumeration_type`)
- variables (`variable`)
- function arguments (`formal_parameter`).

The tree structure mirrors the corresponding source code. For example, a `class_type` DIE can contain `subprogram` DIEs representing methods of the class.
</Info>

The `DWARF` format outputs the following columns:

- `offset` - position of the DIE in the `.debug_info` section
- `size` - number of bytes in the encoded DIE (including attributes)
- `tag` - type of the DIE; the conventional "DW_TAG_" prefix is omitted
- `unit_name` - name of the compilation unit containing this DIE
- `unit_offset` - position of the compilation unit containing this DIE in the `.debug_info` section
- `ancestor_tags` - array of tags of the ancestors of the current DIE in the tree, in order from innermost to outermost
- `ancestor_offsets` - offsets of ancestors, parallel to `ancestor_tags`
- a few common attributes duplicated from the attributes array for convenience:
  - `name`
  - `linkage_name` - mangled fully qualified name; typically only functions have it (but not all functions)
  - `decl_file` - name of the source code file where this entity was declared
  - `decl_line` - line number in the source code where this entity was declared
- parallel arrays describing attributes:
  - `attr_name` - name of the attribute; the conventional "DW_AT_" prefix is omitted
  - `attr_form` - how the attribute is encoded and interpreted; the conventional DW_FORM_ prefix is omitted
  - `attr_int` - integer value of the attribute; 0 if the attribute doesn't have a numeric value
  - `attr_str` - string value of the attribute; empty if the attribute doesn't have a string value

## Example usage {#example-usage}

The `DWARF` format can be used to find compilation units that have the most function definitions (including template instantiations and functions from included header files):

```sql title="Query"
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```
```text title="Response"
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatDWARF(FormatFactory &);
void registerDWARFSchemaReader(FormatFactory &);
void registerInputFormatDWARF(FormatFactory &)
{
}

void registerDWARFSchemaReader(FormatFactory &) {}
}

#endif
