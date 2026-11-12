#include <Common/ListWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/Formats/Impl/Avro/AvroRustPlan.h>
#include <Processors/Formats/Impl/Avro/AvroRustSchema.h>

#if USE_AVRO_RUST

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUTImpl.h>
#include <Common/PODArray.h>
#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>
#include <Core/AccurateComparison.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/DateTimeTransforms.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>
#include <base/EnumReflection.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <array>
#include <exception>
#include <list>
#include <mutex>
#include <unordered_map>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <unordered_set>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int THERE_IS_NO_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
    extern const int TYPE_MISMATCH;
    extern const int CANNOT_PARSE_UUID;
    extern const int LOGICAL_ERROR;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace AvroRust
{
namespace
{

template <typename Source, typename Target, typename Column, bool is_ipv4 = false>
void convertAndInsert(Column & column, Source value, TypeIndex type_index)
{
    Target converted;
    if constexpr (std::is_floating_point_v<Source>)
    {
        if ((!std::is_floating_point_v<Target> && isNaN(value))
            || !accurate::convertNumeric<Source, Target, false>(value, converted))
            throw Exception(
                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "Cannot convert Avro value {} to {}", value, magic_enum::enum_name(type_index));
    }
    else
        converted = static_cast<Target>(value);

    if constexpr (is_ipv4)
        column.insertValue(IPv4(converted));
    else
        column.insertValue(converted);
}

template <typename T>
void insertNumber(IColumn & column, WhichDataType type, T value)
{
    switch (type.idx)
    {
        case TypeIndex::UInt8:
            convertAndInsert<T, UInt8>(assert_cast<ColumnUInt8 &>(column), value, type.idx);
            break;
        case TypeIndex::Date:
        case TypeIndex::UInt16:
            convertAndInsert<T, UInt16>(assert_cast<ColumnUInt16 &>(column), value, type.idx);
            break;
        case TypeIndex::DateTime:
        case TypeIndex::UInt32:
            convertAndInsert<T, UInt32>(assert_cast<ColumnUInt32 &>(column), value, type.idx);
            break;
        case TypeIndex::UInt64:
            convertAndInsert<T, UInt64>(assert_cast<ColumnUInt64 &>(column), value, type.idx);
            break;
        case TypeIndex::Int8:
            convertAndInsert<T, Int8>(assert_cast<ColumnInt8 &>(column), value, type.idx);
            break;
        case TypeIndex::Int16:
            convertAndInsert<T, Int16>(assert_cast<ColumnInt16 &>(column), value, type.idx);
            break;
        case TypeIndex::Date32:
        case TypeIndex::Int32:
            convertAndInsert<T, Int32>(assert_cast<ColumnInt32 &>(column), value, type.idx);
            break;
        case TypeIndex::Int64:
            convertAndInsert<T, Int64>(assert_cast<ColumnInt64 &>(column), value, type.idx);
            break;
        case TypeIndex::Float32:
            convertAndInsert<T, Float32>(assert_cast<ColumnFloat32 &>(column), value, type.idx);
            break;
        case TypeIndex::Float64:
            convertAndInsert<T, Float64>(assert_cast<ColumnFloat64 &>(column), value, type.idx);
            break;
        case TypeIndex::Decimal32:
            convertAndInsert<T, Int32>(assert_cast<ColumnDecimal<Decimal32> &>(column), value, type.idx);
            break;
        case TypeIndex::Decimal64:
            convertAndInsert<T, Int64>(assert_cast<ColumnDecimal<Decimal64> &>(column), value, type.idx);
            break;
        case TypeIndex::DateTime64:
            convertAndInsert<T, Int64>(assert_cast<ColumnDecimal<DateTime64> &>(column), value, type.idx);
            break;
        case TypeIndex::IPv4:
            convertAndInsert<T, UInt32, ColumnIPv4, true>(assert_cast<ColumnIPv4 &>(column), value, type.idx);
            break;
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type is not compatible with Avro");
    }
}

bool canBeDeserializedFromFixed(const DataTypePtr & type, size_t size)
{
    switch (type->getTypeId())
    {
        case TypeIndex::String:
            return true;
        case TypeIndex::FixedString:
        case TypeIndex::IPv6:
        case TypeIndex::Int8:
        case TypeIndex::UInt8:
        case TypeIndex::Int16:
        case TypeIndex::UInt16:
        case TypeIndex::Int32:
        case TypeIndex::UInt32:
        case TypeIndex::Int64:
        case TypeIndex::UInt64:
        case TypeIndex::Int128:
        case TypeIndex::UInt128:
        case TypeIndex::Int256:
        case TypeIndex::UInt256:
            return type->getSizeOfValueInMemory() == size;
        default:
            return false;
    }
}

std::string concatPath(const std::string & prefix, std::string_view name)
{
    return prefix.empty() ? std::string(name) : prefix + "." + std::string(name);
}

/// A path guard, not a global visited set: shared named types in sibling
/// columns are independent mappings. Skipped back edges never enter this guard.
struct PathGuard
{
    UnorderedSetWithMemoryTracking<UInt32> & path;
    UInt32 id;

    PathGuard(UnorderedSetWithMemoryTracking<UInt32> & path_, Node node) : path(path_), id(node.id())
    {
        if (!path.insert(id).second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Recursive Avro schema is not supported: type '{}' references itself", node.name());
    }

    ~PathGuard()
    {
        path.erase(id);
    }
};

using ProgramPtr = std::shared_ptr<AvroProgram>;

/// Only immutable programs are shared. Materializers, state, and tracked output
/// buffers belong to a `Plan`.
///
/// The key is the exact JSON text of the schema, which determines the Rust schema completely, followed by the
/// header, the settings and the sink/operation mapping that the program was compiled for, each part framed by its
/// length so names and embedded zero bytes are unambiguous. It is neither a fingerprint (forgeable) nor the
/// canonical form (not bounded by the size of the schema: a short reference to a named type is printed as its full
/// name, so a compact schema has a canonical form millions of times larger). Every part is linear in an input.
///
/// The cache is bounded by bytes, not by entries: an entry counts its key, its program and a fixed overhead.
/// The least recently used entries are evicted until the cache fits, and an entry that alone does not fit is not cached.
struct ProgramCache
{
    static constexpr size_t MAX_BYTES = 16 * 1024 * 1024;
    /// A list node, an index node and the control block of the shared pointer, which neither the key nor the program counts.
    static constexpr size_t ENTRY_OVERHEAD = 256;

    struct Entry
    {
        std::string key;
        ProgramPtr program;
        size_t bytes;
    };

    std::mutex mutex;
    ListWithMemoryTracking<Entry> entries;
    UnorderedMapWithMemoryTracking<std::string_view, ListWithMemoryTracking<Entry>::iterator> index;
    size_t bytes = 0;

    /// The caller holds `mutex` for all of these.
    ProgramPtr find(const std::string & key)
    {
        auto it = index.find(key);
        if (it == index.end())
            return nullptr;
        entries.splice(entries.begin(), entries, it->second);
        return it->second->program;
    }

    void insert(std::string && key, const ProgramPtr & program)
    {
        const size_t entry_bytes = ENTRY_OVERHEAD + key.size() + avro_program_memory_size(program.get());
        if (entry_bytes > MAX_BYTES)
            return;
        entries.push_front({std::move(key), program, entry_bytes});
        try
        {
            index.emplace(std::string_view(entries.front().key), entries.begin());
        }
        catch (...)
        {
            entries.pop_front();
            throw;
        }
        bytes += entry_bytes;
        /// The new entry fits alone, so the loop ends before it reaches it.
        while (bytes > MAX_BYTES)
        {
            const auto & last = entries.back();
            index.erase(std::string_view(last.key));
            bytes -= last.bytes;
            entries.pop_back();
        }
    }
};

void appendKeyPart(std::string & key, std::string_view value)
{
    key += std::to_string(value.size());
    key += ':';
    if (!value.empty())
        key.append(value);
}

/// How much of the canonical form of a schema a message may carry.
constexpr size_t MAX_SCHEMA_IN_MESSAGE_BYTES = 4096;

/// The loop over the items of an array or a map looks at the cancellation of the query once per this many items. The
/// executor cannot be interrupted, and the items of one container are as many as the bytes of the input allow, or, for
/// items that occupy no bytes, as the budget of a row (16777216) allows for each row of a batch, so the loop has to be.
/// The check reads the state of the query, which costs more than an item: an array with fewer items never looks.
constexpr UInt64 ITEMS_PER_CANCELLATION_CHECK = 1 << 16;

/// Calls `item` `count` times, and throws if the query was cancelled, between chunks of `ITEMS_PER_CANCELLATION_CHECK` items.
template <typename Item>
void forEachItem(UInt64 count, Item && item)
{
    for (UInt64 begin = 0; begin < count; begin += ITEMS_PER_CANCELLATION_CHECK)
    {
        /// `QueryStatus`, not `IProcessor::isCancelled`: the latter is also raised by a `CancelReason::PartialResult` stop,
        /// which must still return its partial result. The same as in `IRowInputFormat::read`.
        if (begin)
            CurrentThread::checkIfNotCancelled();
        const UInt64 end = count - begin > ITEMS_PER_CANCELLATION_CHECK ? begin + ITEMS_PER_CANCELLATION_CHECK : count;
        for (UInt64 i = begin; i < end; ++i)
            item();
    }
}

/// Appends `count` default values to `column`, as `count` calls of `insertDefault` do. `insertManyDefaults` is the same,
/// but for `Nothing` and `Tuple()` (which have no data), and for the elements of a tuple and the nested column of a
/// `Nullable`, it loops over `insertDefault`: here they are extended at once.
void insertDefaults(IColumn & column, UInt64 count)
{
    if (!count)
        return;
    if (typeid_cast<ColumnNothing *>(&column))
    {
        /// A column of `IColumnDummy` only counts: it takes the length of the range and does not look at the source.
        column.insertRangeFrom(column, 0, count);
    }
    else if (auto * tuple = typeid_cast<ColumnTuple *>(&column))
    {
        /// The size of a tuple with elements is that of its first element, and only an empty one has a length of its own.
        if (tuple->tupleSize() == 0)
            tuple->addSize(count);
        for (size_t i = 0; i < tuple->tupleSize(); ++i)
            insertDefaults(tuple->getColumn(i), count);
    }
    else if (auto * nullable = typeid_cast<ColumnNullable *>(&column))
    {
        insertDefaults(nullable->getNestedColumn(), count);
        auto & null_map = nullable->getNullMapData();
        null_map.resize_fill(null_map.size() + count, 1);
    }
    else
        column.insertManyDefaults(count);
}

}

struct Plan::Impl
{
    /// Appends one value of a column to it: a value that the executor decoded into the sinks, or a default one. Returns
    /// whether the file had the value (`false`: the missing value was replaced by the default, `input_format_null_as_default`).
    struct Reader : std::function<bool(IColumn &)>
    {
        using std::function<bool(IColumn &)>::function;

        /// Set only for a reader that takes nothing from the sinks and appends the default value of the column it is
        /// given (`insertDefault`; for a tuple, of each of its elements). Its value is what the reader returns. A
        /// container whose items have such a reader appends all of them at once (`insertDefaults`) instead of calling
        /// the reader for each, which for items that occupy no bytes (`null`, an empty record: the executor counts
        /// them without iterating) would be up to 16777216 calls per row for no data. Never set for a reader that can
        /// throw (one that a `value_validator` rejected) or that decodes a value.
        std::optional<bool> defaults_only;
    };
    using Action = std::function<void(MutableColumns &, VectorWithMemoryTracking<UInt8> &)>;

    /// What a `Buffer::direct` does with the part of an output column that its sink is bound to.
    enum class DirectMode
    {
        /// Sets the fill levels of the sink to the ones of the column, which the executor continues from. Allocates nothing.
        Bind,
        /// Makes the column at least `capacity` (and `capacity2`) elements long, which may allocate, and points the sink to it.
        Grow,
        /// Shrinks the column to the fill levels of the sink: the rows that the executor completed.
        Finish,
    };

    struct Buffer
    {
        AvroSinkDesc desc{};
        PODArray<UInt8> data;
        PODArray<UInt64> offsets;
        UInt64 cursor = 0;
        UInt64 previous_offset = 0;
        size_t direct_column = std::numeric_limits<size_t>::max();
        /// Binds the sink to the output column `direct_column` instead of staging, see `DirectMode`.
        std::function<void(IColumn &, AvroSink &, UInt64, UInt64, DirectMode)> direct;
        /// A direct sink that gets one element (a `String`, an offset) for every row, and so can have room for all
        /// the rows of the call from the start. The sinks of the items of arrays do not: the arrays may be empty.
        bool per_row = true;
    };

    ProgramPtr program;
    std::unique_ptr<AvroState, decltype(&avro_state_free)> state{nullptr, avro_state_free};
    VectorWithMemoryTracking<Buffer> buffers;
    VectorWithMemoryTracking<AvroSink> sinks;
    /// The sinks that are bound to output columns, and the staging sinks of values wider than `STAGING_BYTES`: the
    /// only ones that need more than their fill levels after a call. A call visits these and not all the sinks.
    VectorWithMemoryTracking<UInt32> direct_sinks;
    VectorWithMemoryTracking<UInt32> wide_sinks;
    VectorWithMemoryTracking<AvroPlanNode> nodes;
    VectorWithMemoryTracking<bool> found;
    /// Per column of the header: whether a row can leave it without a value read from the file, which is what
    /// `missing` reports. False only for a column that its reader fills in every row, and then it is neither
    /// tracked while decoding nor present in `missing` (its vector stays empty). True for a column
    ///  - the file does not have (never read, so every row is missing),
    ///  - found below a union branch or in the items of an array of records (a row that does not take that branch,
    ///    or has no items, reads nothing of it), or
    ///  - whose reader reports the Avro null of a union or a null as the default value of a non-`Nullable` type.
    VectorWithMemoryTracking<UInt8> may_miss;
    /// The columns with `may_miss`, ascending: all the per-row work on `read_columns` and `missing` is over these.
    VectorWithMemoryTracking<size_t> miss_columns;
    /// Set while the reader of one column is created: that reader can return `false`.
    bool reader_may_miss = false;
    VectorWithMemoryTracking<UInt8> read_columns;
    VectorWithMemoryTracking<size_t> initial_sizes;
    /// Sizes of the `missing` vectors of the destination when the current call started: the rows of the call start there.
    /// Only for the columns with `may_miss`.
    VectorWithMemoryTracking<size_t> missing_bases;
    UnorderedSetWithMemoryTracking<UInt32> path;
    Action action;
    UInt64 committed_rows = 0;
    const bool null_as_default;
    const FormatSettings settings;
    const ValueValidator value_validator;
    std::string_view current_column;
    size_t validation_depth = 0;

    Impl(const Block & header, const Schema & schema, bool allow_missing_fields, bool null_as_default_,
         const FormatSettings & settings_, const NameSet & absent_columns, ValueValidator value_validator_)
        : found(header.columns()), may_miss(header.columns()), read_columns(header.columns()), initial_sizes(header.columns()),
          missing_bases(header.columns()), null_as_default(null_as_default_), settings(settings_), value_validator(std::move(value_validator_))
    {
        if (schema.root().kind() != AVRO_KIND_RECORD)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Root schema must be a record");
        action = createAction(header, schema.root(), "", true);
        if (!allow_missing_fields)
        {
            for (size_t i = 0; i < header.columns(); ++i)
                if (!found[i] && !absent_columns.contains(header.getByPosition(i).name))
                    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Field {} not found in Avro schema", header.getByPosition(i).name);
        }
        for (size_t i = 0; i < header.columns(); ++i)
        {
            if (!found[i])
                may_miss[i] = 1;
            if (may_miss[i])
                miss_columns.push_back(i);
        }
        VectorWithMemoryTracking<AvroSinkDesc> descriptors;
        descriptors.reserve(buffers.size());
        for (const auto & buffer : buffers)
            descriptors.push_back(buffer.desc);
        std::string key;
        appendKeyPart(key, schema.source());
        appendKeyPart(key, std::to_string(header.columns()));
        for (const auto & column : header)
        {
            appendKeyPart(key, column.name);
            appendKeyPart(key, column.type->getName());
        }
        appendKeyPart(key, null_as_default ? "null_as_default" : "null_is_error");
        /// The schema text determines the schema, with its logical annotations and attributes, but not what is read
        /// of it. Include the entire sink/operation mapping as well, so any setting or annotation which changes
        /// the Rust program also changes its identity.
        appendKeyPart(key, {reinterpret_cast<const char *>(descriptors.data()), descriptors.size() * sizeof(AvroSinkDesc)});
        appendKeyPart(key, {reinterpret_cast<const char *>(nodes.data()), nodes.size() * sizeof(AvroPlanNode)});
        static ProgramCache cache;
        {
            std::lock_guard lock(cache.mutex);
            if (auto cached = cache.find(key))
                program = std::move(cached);
            else
            {
                char error[1024]{};
                AvroProgram * compiled = nullptr;
                check(avro_program_compile(schema.get(), descriptors.data(), descriptors.size(), nodes.data(), nodes.size(),
                                           &compiled, error, sizeof(error)), error);
                program = ProgramPtr(compiled, avro_program_free);
                cache.insert(std::move(key), program);
            }
        }
        nodes.clear();
        nodes.shrink_to_fit();
        /// The nesting of the data (arrays, maps and recursive records that are skipped) is kept on a stack of frames on the heap,
        /// 24 bytes per level, not on the native stack, so it is bounded by the hard cap of the library (0): 100000 levels, 2.4 MB per
        /// state. The schema depth (`MAX_AVRO_SCHEMA_DEPTH`) does not apply to it: a schema of a few levels describes data of any
        /// depth with a recursive record, and the decoder over `contrib/avro`, which recursed on the native stack, read what its stack
        /// had room for, about as many levels as the cap.
        state.reset(avro_state_new(0));
        if (!state)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create Avro decoder state");
        sinks.resize(buffers.size());
        for (UInt32 i = 0; i < buffers.size(); ++i)
        {
            if (buffers[i].direct)
                direct_sinks.push_back(i);
            else if (width(buffers[i].desc) > STAGING_BYTES)
                wide_sinks.push_back(i);
        }
    }

    UInt32 addSink(UInt32 kind, UInt32 arg = 0, UInt32 flags = 0)
    {
        UInt32 id = static_cast<UInt32>(buffers.size());
        buffers.emplace_back();
        buffers.back().desc = {kind, arg, flags};
        return id;
    }

    size_t addNode(Node node, UInt32 action_kind, UInt32 sink = AVRO_NO_SINK, UInt32 sink2 = AVRO_NO_SINK,
                   UInt32 arg = 0, UInt32 children = 0)
    {
        nodes.push_back({action_kind, node.id(), sink, sink2, arg, children, 0, 0, 0, 0});
        return nodes.size() - 1;
    }

    void skip(Node node)
    {
        addNode(node, AVRO_ACTION_SKIP);
    }

    template <typename T>
    T next(UInt32 id)
    {
        auto & buffer = buffers[id];
        T value;
        memcpy(&value, buffer.data.data() + buffer.cursor * sizeof(T), sizeof(T));
        ++buffer.cursor;
        return value;
    }

    std::string_view nextBytes(UInt32 id)
    {
        auto & buffer = buffers[id];
        const UInt64 end = buffer.offsets[buffer.cursor++];
        const UInt64 start = buffer.previous_offset;
        buffer.previous_offset = end;
        return {reinterpret_cast<const char *>(buffer.data.data() + start), end - start};
    }

    std::string_view nextFixed(UInt32 id, UInt32 width)
    {
        auto & buffer = buffers[id];
        const UInt64 start = buffer.cursor++ * width;
        return {reinterpret_cast<const char *>(buffer.data.data() + start), width};
    }

    UInt64 nextCount(UInt32 id)
    {
        const UInt64 end = next<UInt64>(id);
        auto & previous = buffers[id].previous_offset;
        const UInt64 count = end - previous;
        previous = end;
        return count;
    }

    /// The kind of staging sink whose bytes are the bytes of a column of `type`, when the values of the Avro
    /// `node` are staged in it: the same width and representation, so that `count` values are one `memcpy`
    /// instead of a call and a conversion for each of them. `nullopt` for every other pairing.
    static std::optional<UInt32> bulkSinkKind(Node node, const DataTypePtr & type)
    {
        const auto index = WhichDataType(type).idx;
        switch (node.kind())
        {
            case AVRO_KIND_INT:
                return index == TypeIndex::Int32 ? std::optional<UInt32>(AVRO_SINK_INT32) : std::nullopt;
            case AVRO_KIND_LONG:
                return index == TypeIndex::Int64 ? std::optional<UInt32>(AVRO_SINK_INT64) : std::nullopt;
            case AVRO_KIND_FLOAT:
                return index == TypeIndex::Float32 ? std::optional<UInt32>(AVRO_SINK_FLOAT32) : std::nullopt;
            case AVRO_KIND_DOUBLE:
                return index == TypeIndex::Float64 ? std::optional<UInt32>(AVRO_SINK_FLOAT64) : std::nullopt;
            case AVRO_KIND_BOOLEAN:
                return index == TypeIndex::UInt8 ? std::optional<UInt32>(AVRO_SINK_BOOL) : std::nullopt;
            default:
                return std::nullopt;
        }
    }

    template <typename T>
    void takeNumbersAs(IColumn & column, UInt32 sink, UInt64 count)
    {
        auto & buffer = buffers[sink];
        auto & data = assert_cast<ColumnVector<T> &>(column).getData();
        const size_t old_size = data.size();
        data.resize(old_size + count);
        memcpy(data.data() + old_size, buffer.data.data() + buffer.cursor * sizeof(T), count * sizeof(T));
        buffer.cursor += count;
    }

    /// Appends the next `count` values of a sink for which `bulkSinkKind` returned its kind to `column`.
    void takeNumbers(IColumn & column, UInt32 sink, UInt64 count)
    {
        if (!count)
            return;
        switch (buffers[sink].desc.kind)
        {
            case AVRO_SINK_INT32:
                takeNumbersAs<Int32>(column, sink, count);
                break;
            case AVRO_SINK_INT64:
                takeNumbersAs<Int64>(column, sink, count);
                break;
            case AVRO_SINK_FLOAT32:
                takeNumbersAs<Float32>(column, sink, count);
                break;
            case AVRO_SINK_FLOAT64:
                takeNumbersAs<Float64>(column, sink, count);
                break;
            default:
                takeNumbersAs<UInt8>(column, sink, count);
                break;
        }
    }

    /// The reader of an array whose items are all numbers that `bulkSinkKind` accepts: the items of one array, or
    /// the elements of the tuples of one array, are taken a whole array at a time. Adds the array, its items and
    /// their sinks to the program exactly as the reader for any items does, or nothing and `nullptr`.
    ///
    /// With a `direct_column` (the output column that the array is, and every row of it has the array) the sinks are
    /// bound to the column: the executor writes the end of each array to its offsets and the items to its nested
    /// column, and there is nothing left to read, so the numbers are not written to a staging buffer and copied.
    Reader createBulkArrayReader(Node node, const DataTypePtr & nested_type, size_t direct_column)
    {
        const bool direct = direct_column != std::numeric_limits<size_t>::max();
        const auto items = node.child(0);
        if (const auto kind = bulkSinkKind(items, nested_type))
        {
            const UInt32 offsets = addSink(AVRO_SINK_ARRAY_OFFSETS);
            addNode(node, AVRO_ACTION_ARRAY, offsets, AVRO_NO_SINK, 0, 1);
            const UInt32 sink = addSink(*kind);
            addNode(items, AVRO_ACTION_VALUE, sink);
            if (direct)
            {
                bindArrayOffsets(offsets, direct_column);
                bindArrayItemsOfKind(sink, direct_column, *kind);
                return [](IColumn &)
                {
                    return true;
                };
            }
            return [this, offsets, sink](IColumn & column)
            {
                auto & array = assert_cast<ColumnArray &>(column);
                const UInt64 count = nextCount(offsets);
                takeNumbers(array.getData(), sink, count);
                array.getOffsets().push_back(array.getOffsets().back() + count);
                return true;
            };
        }

        if (items.kind() != AVRO_KIND_RECORD || !WhichDataType(nested_type).isTuple())
            return nullptr;
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*nested_type);
        const auto item_info = items.info();
        if (item_info.child_count == 0 || item_info.child_count != tuple_type.getElements().size())
            return nullptr;
        /// Fields in the order of the record: the sink kind and the position of the element they go to.
        VectorWithMemoryTracking<std::pair<UInt32, size_t>> fields;
        UnorderedSetWithMemoryTracking<size_t> positions;
        for (UInt32 i = 0; i < item_info.child_count; ++i)
        {
            const auto position = tuple_type.tryGetPositionByName(std::string(items.fieldName(i)));
            if (!position || !positions.insert(*position).second)
                return nullptr;
            const auto kind = bulkSinkKind(items.child(i), tuple_type.getElements()[*position]);
            if (!kind)
                return nullptr;
            fields.emplace_back(*kind, *position);
        }

        const UInt32 offsets = addSink(AVRO_SINK_ARRAY_OFFSETS);
        addNode(node, AVRO_ACTION_ARRAY, offsets, AVRO_NO_SINK, 0, 1);
        addNode(items, AVRO_ACTION_RECORD, AVRO_NO_SINK, AVRO_NO_SINK, 0, item_info.child_count);
        VectorWithMemoryTracking<std::pair<UInt32, size_t>> sinks_and_positions;
        for (UInt32 i = 0; i < item_info.child_count; ++i)
        {
            const UInt32 sink = addSink(fields[i].first);
            addNode(items.child(i), AVRO_ACTION_VALUE, sink);
            sinks_and_positions.emplace_back(sink, fields[i].second);
        }
        if (direct)
        {
            bindArrayOffsets(offsets, direct_column);
            for (size_t i = 0; i < sinks_and_positions.size(); ++i)
                bindArrayTupleItemsOfKind(sinks_and_positions[i].first, direct_column, sinks_and_positions[i].second, fields[i].first);
            return [](IColumn &)
            {
                return true;
            };
        }
        return [this, offsets, elements = std::move(sinks_and_positions)](IColumn & column)
        {
            auto & array = assert_cast<ColumnArray &>(column);
            const UInt64 count = nextCount(offsets);
            auto & tuple = assert_cast<ColumnTuple &>(array.getData());
            for (const auto & [sink, position] : elements)
                takeNumbers(tuple.getColumn(position), sink, count);
            array.getOffsets().push_back(array.getOffsets().back() + count);
            return true;
        };
    }

    /// Binds `sink`, which writes numbers of type `T`, to the values of the output column `column` that `locate` finds
    /// in it: the column itself, the items of an array, or an element of the tuples of an array.
    template <typename T, typename Locate>
    void bindNumbers(UInt32 sink, size_t column, Locate locate)
    {
        auto & buffer = buffers[sink];
        buffer.direct_column = column;
        buffer.direct = [locate](IColumn & output, AvroSink & destination, UInt64 capacity, UInt64, DirectMode mode)
        {
            auto & data = locate(output);
            if (mode == DirectMode::Bind)
            {
                destination.len = data.size();
                return;
            }
            if (mode == DirectMode::Finish)
            {
                data.resize(destination.len);
                return;
            }
            data.resize(std::max<UInt64>(data.size(), capacity));
            destination.data = reinterpret_cast<uint8_t *>(data.data());
            destination.data_size = data.size() * sizeof(T);
        };
    }

    template <typename T>
    void bindNumber(UInt32 sink, size_t column)
    {
        bindNumbers<T>(sink, column, [](IColumn & output) -> auto & { return assert_cast<ColumnVector<T> &>(output).getData(); });
    }

    /// The sink of the offsets of the output array column `column`: the executor appends the end of each array to
    /// the offsets of the column, from the size of its items, and the column is as long as the rows that completed.
    void bindArrayOffsets(UInt32 sink, size_t column)
    {
        auto & buffer = buffers[sink];
        buffer.direct_column = column;
        buffer.direct = [](IColumn & output, AvroSink & destination, UInt64 capacity, UInt64, DirectMode mode)
        {
            auto & offsets = assert_cast<ColumnArray &>(output).getOffsets();
            if (mode == DirectMode::Bind)
            {
                destination.len = offsets.size();
                destination.offset = offsets.empty() ? 0 : offsets.back();
                return;
            }
            if (mode == DirectMode::Finish)
            {
                offsets.resize(destination.len);
                return;
            }
            offsets.resize(std::max<UInt64>(offsets.size(), capacity));
            destination.data = reinterpret_cast<uint8_t *>(offsets.data());
            destination.data_size = offsets.size() * sizeof(UInt64);
        };
    }

    /// The sink of the items of the output array column `column`, written to its nested column of numbers `T`.
    template <typename T>
    void bindArrayItems(UInt32 sink, size_t column)
    {
        bindNumbers<T>(
            sink, column,
            [](IColumn & output) -> auto & { return assert_cast<ColumnVector<T> &>(assert_cast<ColumnArray &>(output).getData()).getData(); });
        /// An array may have no items: the nested column grows as the items come, not with the number of rows.
        buffers[sink].per_row = false;
    }

    /// The sink of one element of the tuples of the output array column `column`, written to that element of its nested column.
    template <typename T>
    void bindArrayTupleItems(UInt32 sink, size_t column, size_t position)
    {
        bindNumbers<T>(
            sink, column,
            [position](IColumn & output) -> auto &
            {
                auto & tuple = assert_cast<ColumnTuple &>(assert_cast<ColumnArray &>(output).getData());
                return assert_cast<ColumnVector<T> &>(tuple.getColumn(position)).getData();
            });
        buffers[sink].per_row = false;
    }

    /// `bindArrayItems` for the sink of kind `kind` (one that `bulkSinkKind` returns).
    void bindArrayItemsOfKind(UInt32 sink, size_t column, UInt32 kind)
    {
        switch (kind)
        {
            case AVRO_SINK_INT32:
                bindArrayItems<Int32>(sink, column);
                break;
            case AVRO_SINK_INT64:
                bindArrayItems<Int64>(sink, column);
                break;
            case AVRO_SINK_FLOAT32:
                bindArrayItems<Float32>(sink, column);
                break;
            case AVRO_SINK_FLOAT64:
                bindArrayItems<Float64>(sink, column);
                break;
            default:
                bindArrayItems<UInt8>(sink, column);
                break;
        }
    }

    void bindArrayTupleItemsOfKind(UInt32 sink, size_t column, size_t position, UInt32 kind)
    {
        switch (kind)
        {
            case AVRO_SINK_INT32:
                bindArrayTupleItems<Int32>(sink, column, position);
                break;
            case AVRO_SINK_INT64:
                bindArrayTupleItems<Int64>(sink, column, position);
                break;
            case AVRO_SINK_FLOAT32:
                bindArrayTupleItems<Float32>(sink, column, position);
                break;
            case AVRO_SINK_FLOAT64:
                bindArrayTupleItems<Float64>(sink, column, position);
                break;
            default:
                bindArrayTupleItems<UInt8>(sink, column, position);
                break;
        }
    }

    void bindString(UInt32 sink, size_t column)
    {
        auto & buffer = buffers[sink];
        buffer.direct_column = column;
        /// `ColumnString` stores exact lengths, without a trailing zero byte.
        buffer.direct = [](IColumn & output, AvroSink & destination, UInt64 capacity, UInt64 capacity2, DirectMode mode)
        {
            auto & string = assert_cast<ColumnString &>(output);
            auto & data = string.getChars();
            auto & offsets = string.getOffsets();
            if (mode == DirectMode::Bind)
            {
                destination.len = data.size();
                destination.len2 = offsets.size();
                destination.offset = destination.len;
                return;
            }
            if (mode == DirectMode::Finish)
            {
                data.resize(destination.len);
                offsets.resize(destination.len2);
                return;
            }
            data.resize(std::max<UInt64>(data.size(), capacity));
            offsets.resize(std::max<UInt64>(offsets.size(), capacity2));
            destination.data = reinterpret_cast<uint8_t *>(data.data());
            destination.data_size = data.size();
            destination.data2 = reinterpret_cast<uint8_t *>(offsets.data());
            destination.data2_size = offsets.size() * sizeof(UInt64);
        };
    }

    Reader createReader(Node node, const DataTypePtr & type, size_t direct_column = std::numeric_limits<size_t>::max());
    Reader createReaderImpl(Node node, const DataTypePtr & type, size_t direct_column);
    Reader createIntegerReader(Node node, const DataTypePtr & type, UInt32 sink);
    template <typename DecimalType> Reader createDecimalReader(Node node, const DataTypePtr & type);
    static std::optional<size_t> variantMetadataPosition(Node node, const DataTypePtr & type);
    Reader createVariantReader(Node node, const DataTypePtr & type, size_t metadata_position);
    Action createAction(const Block & header, Node node, const std::string & current_path, bool direct);

    static UInt64 width(const AvroSinkDesc & desc)
    {
        switch (desc.kind)
        {
            case AVRO_SINK_INT32:
            case AVRO_SINK_FLOAT32:
                return 4;
            case AVRO_SINK_INT64:
            case AVRO_SINK_FLOAT64:
            case AVRO_SINK_ARRAY_OFFSETS:
            case AVRO_SINK_MAP_OFFSETS:
                return 8;
            case AVRO_SINK_BOOL:
            case AVRO_SINK_NULL_MAP:
            case AVRO_SINK_BYTES:
                return 1;
            default:
                return desc.arg;
        }
    }

    /// Staging is sized by what the executor asks for, never by a schema-declared width: a sink starts with
    /// at most `STAGING_BYTES` and a `fixed` of 64 MiB is allocated only when a value of it is actually decoded.
    static constexpr UInt64 STAGING_BYTES = 1024;
    static constexpr UInt64 STAGING_ELEMENTS = 16;

    static UInt64 stagingElements(UInt64 element_width)
    {
        return std::min(STAGING_ELEMENTS, STAGING_BYTES / element_width);
    }

    /// Wide sinks hold one decoded value per element, so keeping every buffer across calls would retain
    /// schema-sized memory for the whole lifetime of the plan: a sink that the call did not write is freed.
    ///
    /// A sink that the call did write keeps its buffer for the next call when the buffer is no larger than
    /// `WIDE_RETAIN_FACTOR` times what the call wrote (or `WIDE_RETAIN_MIN_BYTES`, for a few small values).
    /// The next call writes about as much, and without this it would grow the buffer again from nothing, with
    /// a copy at every doubling, on every call. The factor is the slack of the growth itself: the
    /// executor's request is doubled by `grow` and then rounded up to a power of two by the array, which can
    /// allocate almost four times what ends up written, so a smaller factor would free the buffer of a steady
    /// workload after every call. What is kept is of the order of the batch that was just decoded, which the
    /// output columns hold at the same time. A buffer that is larger than that is a leftover of a bigger call.
    static constexpr UInt64 WIDE_RETAIN_MIN_BYTES = 1024 * 1024;
    static constexpr UInt64 WIDE_RETAIN_FACTOR = 4;

    void releaseWideStaging()
    {
        for (const UInt32 i : wide_sinks)
        {
            auto & buffer = buffers[i];
            const UInt64 element_width = width(buffer.desc);
            /// `len` is in elements and the sink holds `data_size` bytes, so the product cannot overflow.
            const UInt64 written = std::min<UInt64>(sinks[i].len, sinks[i].data_size / element_width) * element_width;
            if (written == 0 || buffer.data.allocated_bytes() > std::max(WIDE_RETAIN_MIN_BYTES, written * WIDE_RETAIN_FACTOR))
                buffer.data = PODArray<UInt8>();
        }
    }

    void grow(UInt32 id, UInt64 count, UInt64 count2, MutableColumns & columns)
    {
        auto & buffer = buffers[id];
        auto & sink = sinks[id];
        const auto element_width = width(buffer.desc);
        const UInt64 old_count = sink.data_size / element_width;
        const UInt64 old_count2 = sink.data2_size / sizeof(UInt64);
        const UInt64 max_count = std::numeric_limits<size_t>::max() / element_width;
        const UInt64 max_count2 = std::numeric_limits<size_t>::max() / sizeof(UInt64);
        if (count > old_count && old_count <= max_count / 2)
            count = std::max({count, stagingElements(element_width), old_count * 2});
        if (count2 > old_count2 && old_count2 <= max_count2 / 2)
            count2 = std::max({count2, STAGING_ELEMENTS, old_count2 * 2});
        if (count > max_count || count2 > max_count2)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Avro sink size exceeds addressable memory");
        if (buffer.direct)
        {
            buffer.direct(*columns[buffer.direct_column], sink, count, count2, DirectMode::Grow);
            return;
        }
        buffer.data.resize(std::max<UInt64>(buffer.data.size(), count * element_width));
        buffer.offsets.resize(std::max<UInt64>(buffer.offsets.size(), count2));
        sink.data = reinterpret_cast<uint8_t *>(buffer.data.data());
        sink.data_size = buffer.data.size();
        sink.data2 = reinterpret_cast<uint8_t *>(buffer.offsets.data());
        sink.data2_size = buffer.offsets.size() * sizeof(UInt64);
    }

    struct Outcome
    {
        /// All `max_rows` rows are decoded. False only with `allow_incomplete`: the input ended at a row boundary or inside a row.
        bool complete = false;
        /// Rows completed by the call: all of them are in the columns.
        UInt64 rows = 0;
        /// Bytes of `input` they occupy.
        UInt64 consumed = 0;
    };

    /// With `allow_incomplete`, input that ends at a row boundary or inside a row is reported as `complete == false`
    /// instead of an exception: the rows that completed before the end stay in the columns, the row that the input
    /// cut is rolled back (nothing of it is left in the columns or in `missing`) and is decoded again from its start
    /// by a call with more input. Without it that is an exception, as is every other error.
    ///
    /// `append_missing`: the `missing` vectors of the call are appended to what they hold, otherwise replaced.
    ///
    /// With a `source` the input is the first window of a payload that continues (`more_input`): the executor runs in
    /// streaming mode and pauses a row that the window cuts, and every time it needs more input the call hands the
    /// bytes that are done with to the source and goes on with the window it returns, so that no row is held whole.
    /// Then `allow_incomplete` is not used: the end of the last window (`more == false`) is an exception, and
    /// `Outcome::consumed` counts bytes of that window.
    Outcome decode(std::span<const UInt8> input, UInt64 max_rows, MutableColumns & columns,
                   VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> & missing, bool allow_incomplete, bool append_missing,
                   const InputSource * source = nullptr, bool more_input = false);
};

Plan::Impl::Reader Plan::Impl::createIntegerReader(Node node, const DataTypePtr & type, UInt32 sink)
{
    const WhichDataType target(type);
    const auto overflow = settings.date_time_overflow_behavior;
    if (target.isDate() || target.isDate32())
    {
        const Int32 min = target.isDate() ? 0 : DATE_LUT_MIN_EXTEND_DAY_NUM;
        const Int32 max = target.isDate() ? DATE_LUT_MAX_DAY_NUM : DATE_LUT_MAX_EXTEND_DAY_NUM;
        return [this, target, sink, overflow, min, max](IColumn & column)
        {
            Int32 days = next<Int32>(sink);
            if (days < min || days > max)
            {
                if (overflow == FormatSettings::DateTimeOverflowBehavior::Saturate)
                    days = std::clamp(days, min, max);
                else if (target.isDate())
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Input value {} exceeds the range of type Date, which is [0, {}]", days, max);
                else
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Input value {} exceeds the range of type Date32, which is [{}, {}]", days, min, max);
            }
            insertNumber(column, target, days);
            return true;
        };
    }
    if (node.logical() == AVRO_LOGICAL_DATE && target.isDateTime())
    {
        return [this, type, sink, overflow](IColumn & column)
        {
            Int32 days = next<Int32>(sink);
            if (days < 0 || days > MAX_DATETIME_DAY_NUM)
            {
                if (overflow == FormatSettings::DateTimeOverflowBehavior::Saturate)
                    days = std::clamp<Int32>(days, 0, MAX_DATETIME_DAY_NUM);
                else
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Input value {} exceeds the day range of type DateTime, which is [0, {}]", days, MAX_DATETIME_DAY_NUM);
            }
            const auto & zone = assert_cast<const DataTypeDateTime &>(*type).getTimeZone();
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(zone.fromDayNum(ExtendedDayNum(days))));
            return true;
        };
    }
    if (node.logical() == AVRO_LOGICAL_DATE && target.isDateTime64())
    {
        const auto & datetime = assert_cast<const DataTypeDateTime64 &>(*type);
        const Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64::NativeType>(datetime.getScale());
        const auto [min, max] = getDateTime64DayNumRange(multiplier, datetime.getTimeZone());
        return [this, type, sink, overflow, multiplier, min, max](IColumn & column)
        {
            Int32 days = next<Int32>(sink);
            if (days < min || days > max)
            {
                if (overflow == FormatSettings::DateTimeOverflowBehavior::Saturate)
                    days = days < min ? min : max;
                else
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Input value {} exceeds the day range representable by type {}, which is [{}, {}]",
                        days, type->getName(), min, max);
            }
            const auto & datetime_type = assert_cast<const DataTypeDateTime64 &>(*type);
            const Int64 seconds = datetime_type.getTimeZone().fromDayNum(ExtendedDayNum(days));
            assert_cast<ColumnDecimal<DateTime64> &>(column).insertValue(
                DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(seconds, 0, multiplier));
            return true;
        };
    }
    return [this, target, sink](IColumn & column)
    {
        insertNumber(column, target, next<Int32>(sink));
        return true;
    };
}

template <typename DecimalType>
Plan::Impl::Reader Plan::Impl::createDecimalReader(Node node, const DataTypePtr & type)
{
    const auto info = node.info();
    const auto & decimal = assert_cast<const DecimalType &>(*type);
    if (decimal.getScale() != info.scale || decimal.getPrecision() != info.precision)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot insert Avro decimal with scale {} and precision {} to ClickHouse type {} with scale {} and precision {}",
            info.scale, info.precision, type->getName(), decimal.getScale(), decimal.getPrecision());

    const bool fixed = node.kind() == AVRO_KIND_FIXED;
    const UInt32 sink = addSink(fixed ? AVRO_SINK_FIXED_BYTES : AVRO_SINK_BYTES, fixed ? info.fixed_size : 0);
    addNode(node, AVRO_ACTION_VALUE, sink);
    return [this, type, sink, fixed, fixed_size = info.fixed_size](IColumn & column)
    {
        constexpr size_t size = sizeof(typename DecimalType::FieldType);
        const auto bytes = fixed ? nextFixed(sink, fixed_size) : nextBytes(sink);
        if (bytes.empty() || bytes.size() > size)
            throw Exception(
                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "Cannot parse type {}, expected non-empty binary data with size equal to or less than {}, got {}",
                type->getName(), size, bytes.size());
        std::array<UInt8, size> padded{};
        padded.fill(static_cast<UInt8>(bytes.front()) & 128 ? 0xff : 0);
        memcpy(padded.data() + size - bytes.size(), bytes.data(), bytes.size());
        typename DecimalType::FieldType value;
        ReadBufferFromString input(std::string_view(reinterpret_cast<const char *>(padded.data()), size));
        readBinaryBigEndian(value.value, input);
        assert_cast<typename DecimalType::ColumnType &>(column).insertValue(value);
        return true;
    };
}

Plan::Impl::Reader Plan::Impl::createReader(Node node, const DataTypePtr & type, size_t direct_column)
{
    if (!value_validator)
        return createReaderImpl(node, type, direct_column);
    if (auto error = value_validator(current_column, node, validation_depth))
    {
        skip(node);
        return [message = std::move(*error)](IColumn &) -> bool
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "{}", message);
        };
    }

    const bool container = node.kind() == AVRO_KIND_MAP || node.kind() == AVRO_KIND_ARRAY;
    validation_depth += container;
    SCOPE_EXIT({ validation_depth -= container; });
    return createReaderImpl(node, type, direct_column);
}

/// Iceberg v3 stores a `variant` in Avro as a record of exactly two `bytes` fields, `metadata` and `value`, which is read
/// into `Dynamic` or `JSON`. The position of the `metadata` field when `node` is such a record and `type` such a column.
std::optional<size_t> Plan::Impl::variantMetadataPosition(Node node, const DataTypePtr & type)
{
    if (!isDynamic(type) && !isObject(removeNullable(type)))
        return std::nullopt;
    if (node.kind() != AVRO_KIND_RECORD || node.info().child_count != 2 || node.child(0).kind() != AVRO_KIND_BYTES
        || node.child(1).kind() != AVRO_KIND_BYTES)
        return std::nullopt;
    if (node.fieldName(0) == "metadata" && node.fieldName(1) == "value")
        return 0;
    if (node.fieldName(0) == "value" && node.fieldName(1) == "metadata")
        return 1;
    return std::nullopt;
}

Plan::Impl::Reader Plan::Impl::createVariantReader(Node node, const DataTypePtr & type, size_t metadata_position)
{
    addNode(node, AVRO_ACTION_RECORD, AVRO_NO_SINK, AVRO_NO_SINK, 0, 2);
    std::array<UInt32, 2> blobs{};
    for (UInt32 i = 0; i < 2; ++i)
    {
        blobs[i] = addSink(AVRO_SINK_BYTES);
        addNode(node.child(i), AVRO_ACTION_VALUE, blobs[i]);
    }
    return [this, blobs, metadata_position, decoder = Parquet::VariantValueDecoder(type, String(current_column), settings)](IColumn & column)
    {
        const auto first = nextBytes(blobs[0]);
        const auto second = nextBytes(blobs[1]);
        decoder.decode(metadata_position == 0 ? first : second, metadata_position == 0 ? second : first, column);
        return true;
    };
}

Plan::Impl::Reader Plan::Impl::createReaderImpl(Node node, const DataTypePtr & type, size_t direct_column)
{
    checkStackSize();
    const bool direct = direct_column != std::numeric_limits<size_t>::max();
    if (type->lowCardinality())
    {
        const auto & dictionary_type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
        auto nested_reader = createReader(node, dictionary_type);
        /// Reuse the plain column rather than allocating one per value.
        auto temporary_column = std::make_shared<MutableColumnPtr>(dictionary_type->createColumn());
        return [nested = std::move(nested_reader), temporary = std::move(temporary_column)](IColumn & column)
        {
            if ((*temporary)->size())
                (*temporary)->popBack((*temporary)->size());
            const bool read = nested(**temporary);
            assert_cast<ColumnLowCardinality &>(column).insertFromFullColumn(**temporary, 0);
            return read;
        };
    }

    const WhichDataType target(type);
    const auto info = node.info();
    switch (node.kind())
    {
        case AVRO_KIND_STRING:
        case AVRO_KIND_BYTES:
        {
            if (target.isUUID() || target.isString() || target.isFixedString())
            {
                const UInt32 sink = addSink(AVRO_SINK_BYTES);
                addNode(node, AVRO_ACTION_VALUE, sink);
                if (direct && target.isString())
                {
                    bindString(sink, direct_column);
                    return [](IColumn &)
                    {
                        return true;
                    };
                }
                return [this, sink, uuid = target.isUUID()](IColumn & column)
                {
                    const auto bytes = nextBytes(sink);
                    if (uuid)
                    {
                        if (bytes.size() != 36)
                            throw Exception(ErrorCodes::CANNOT_PARSE_UUID, "Cannot parse uuid {}", bytes);
                        const auto value = parseUUID({reinterpret_cast<const UInt8 *>(bytes.data()), bytes.size()});
                        assert_cast<DataTypeUUID::ColumnType &>(column).insertValue(value);
                    }
                    else
                        column.insertData(bytes.data(), bytes.size());
                    return true;
                };
            }
            if (target.isDecimal32())
                return createDecimalReader<DataTypeDecimal32>(node, type);
            if (target.isDecimal64())
                return createDecimalReader<DataTypeDecimal64>(node, type);
            if (target.isDecimal128())
                return createDecimalReader<DataTypeDecimal128>(node, type);
            if (target.isDecimal256())
                return createDecimalReader<DataTypeDecimal256>(node, type);
            if (target.isDateTime64())
                return createDecimalReader<DataTypeDateTime64>(node, type);
            break;
        }
        case AVRO_KIND_INT:
        case AVRO_KIND_LONG:
        case AVRO_KIND_FLOAT:
        case AVRO_KIND_DOUBLE:
        case AVRO_KIND_BOOLEAN:
        {
            if (!type->isValueRepresentedByNumber())
                break;
            const auto kind = node.kind();
            const UInt32 sink = addSink(
                kind == AVRO_KIND_INT ? AVRO_SINK_INT32
                : kind == AVRO_KIND_LONG ? AVRO_SINK_INT64
                : kind == AVRO_KIND_FLOAT ? AVRO_SINK_FLOAT32
                : kind == AVRO_KIND_DOUBLE ? AVRO_SINK_FLOAT64 : AVRO_SINK_BOOL);
            addNode(node, AVRO_ACTION_VALUE, sink);
            if (direct)
            {
                bool bound = true;
                if (kind == AVRO_KIND_INT && target.idx == TypeIndex::Int32)
                    bindNumber<Int32>(sink, direct_column);
                else if (kind == AVRO_KIND_LONG && target.idx == TypeIndex::Int64)
                    bindNumber<Int64>(sink, direct_column);
                else if (kind == AVRO_KIND_FLOAT && target.idx == TypeIndex::Float32)
                    bindNumber<Float32>(sink, direct_column);
                else if (kind == AVRO_KIND_DOUBLE && target.idx == TypeIndex::Float64)
                    bindNumber<Float64>(sink, direct_column);
                else if (kind == AVRO_KIND_BOOLEAN && target.idx == TypeIndex::UInt8)
                    bindNumber<UInt8>(sink, direct_column);
                else
                    bound = false;
                if (bound)
                    return [](IColumn &)
                    {
                        return true;
                    };
            }
            if (kind == AVRO_KIND_INT)
                return createIntegerReader(node, type, sink);
            return [this, sink, kind, target](IColumn & column)
            {
                if (kind == AVRO_KIND_LONG)
                    insertNumber(column, target, next<Int64>(sink));
                else if (kind == AVRO_KIND_FLOAT)
                    insertNumber(column, target, next<Float32>(sink));
                else if (kind == AVRO_KIND_DOUBLE)
                    insertNumber(column, target, next<Float64>(sink));
                else
                    insertNumber(column, target, next<UInt8>(sink));
                return true;
            };
        }
        case AVRO_KIND_ARRAY:
        {
            if (target.isArray())
            {
                const auto & nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
                /// Without a validator, which looks at every value on its way: numbers that are the bytes of
                /// the column are not read one by one.
                if (!value_validator)
                    if (auto bulk_reader = createBulkArrayReader(node, nested_type, direct_column))
                        return bulk_reader;
                const UInt32 offsets = addSink(AVRO_SINK_ARRAY_OFFSETS);
                addNode(node, AVRO_ACTION_ARRAY, offsets, AVRO_NO_SINK, 0, 1);
                auto nested_reader = createReader(node.child(0), nested_type);
                /// Items that nothing is read for are not visited: the executor has counted them (it spends a row's budget,
                /// not time, on them), and `count` default values are appended to the column at once. The reader of the
                /// items stays in the program for what it validates when it is created.
                const bool defaults = nested_reader.defaults_only.has_value();
                return [this, offsets, defaults, nested = std::move(nested_reader)](IColumn & column)
                {
                    auto & array = assert_cast<ColumnArray &>(column);
                    const UInt64 count = nextCount(offsets);
                    if (defaults)
                        insertDefaults(array.getData(), count);
                    else
                        forEachItem(count, [&] { nested(array.getData()); });
                    array.getOffsets().push_back(array.getOffsets().back() + count);
                    return true;
                };
            }
            const auto items = node.child(0);
            if (target.isMap() && items.kind() == AVRO_KIND_RECORD && items.info().child_count == 2)
            {
                PathGuard guard(path, items);
                const auto & map_type = assert_cast<const DataTypeMap &>(*type);
                const UInt32 offsets = addSink(AVRO_SINK_ARRAY_OFFSETS);
                addNode(node, AVRO_ACTION_ARRAY, offsets, AVRO_NO_SINK, 0, 1);
                addNode(items, AVRO_ACTION_RECORD, AVRO_NO_SINK, AVRO_NO_SINK, 0, 2);
                const bool key_name = items.fieldName(0) == "key" || items.fieldName(1) == "key";
                const bool value_name = items.fieldName(0) == "value" || items.fieldName(1) == "value";
                const size_t key_index = key_name && value_name && items.fieldName(1) == "key" ? 1 : 0;
                auto first_reader = createReader(items.child(0), key_index == 0 ? map_type.getKeyType() : map_type.getValueType());
                auto second_reader = createReader(items.child(1), key_index == 1 ? map_type.getKeyType() : map_type.getValueType());
                /// A key and a value that are both defaults (`null`s) are appended at once.
                const bool defaults = first_reader.defaults_only && second_reader.defaults_only;
                return [this, offsets, key_index, defaults, first = std::move(first_reader), second = std::move(second_reader)](IColumn & column)
                {
                    auto & map = assert_cast<ColumnMap &>(column);
                    const UInt64 count = nextCount(offsets);
                    auto & first_column = map.getNestedData().getColumn(key_index == 0 ? 0 : 1);
                    auto & second_column = map.getNestedData().getColumn(key_index == 1 ? 0 : 1);
                    if (defaults)
                    {
                        insertDefaults(first_column, count);
                        insertDefaults(second_column, count);
                    }
                    else
                    {
                        forEachItem(
                            count,
                            [&]
                            {
                                first(first_column);
                                second(second_column);
                            });
                    }
                    auto & ends = map.getNestedColumn().getOffsets();
                    ends.push_back(ends.back() + count);
                    return true;
                };
            }
            break;
        }
        case AVRO_KIND_UNION:
        {
            const UInt32 count = info.child_count;
            if (count == 1)
            {
                addNode(node, AVRO_ACTION_UNION, AVRO_NO_SINK, AVRO_NO_SINK, 0, 1);
                auto nested_reader = createReader(node.child(0), type);
                return [nested = std::move(nested_reader)](IColumn & column)
                {
                    nested(column);
                    return true;
                };
            }
            if (value_validator)
            {
                /// A validated metadata field uses the same destination type for its allowed branches.
                /// Keep null as a child reader so a caller can reject a nested null only when it is visited.
                reader_may_miss = true;
                const UInt32 tags = addSink(AVRO_SINK_UNION_TAG, 4);
                addNode(node, AVRO_ACTION_UNION, tags, AVRO_NO_SINK, 0, count);
                VectorWithMemoryTracking<Reader> readers;
                for (UInt32 i = 0; i < count; ++i)
                    readers.push_back(createReader(node.child(i), type));
                return [this, tags, children = std::move(readers)](IColumn & column)
                {
                    return children[next<UInt32>(tags)](column);
                };
            }
            if (count == 2 && (node.child(0).kind() == AVRO_KIND_NULL || node.child(1).kind() == AVRO_KIND_NULL))
            {
                const UInt32 non_null = node.child(0).kind() == AVRO_KIND_NULL ? 1 : 0;
                if (const auto metadata_position = variantMetadataPosition(node.child(non_null), type))
                {
                    const UInt32 null_map = addSink(AVRO_SINK_NULL_MAP);
                    addNode(node, AVRO_ACTION_UNION, null_map, AVRO_NO_SINK, 1 - non_null, count);
                    Reader variant_reader;
                    for (UInt32 i = 0; i < count; ++i)
                    {
                        if (i == non_null)
                            variant_reader = createVariantReader(node.child(i), type, *metadata_position);
                        else
                            skip(node.child(i));
                    }
                    /// A null is the default value, which is a variant null for `Dynamic` and an empty object for `JSON`.
                    return [this, null_map, nested = std::move(variant_reader)](IColumn & column)
                    {
                        if (next<UInt8>(null_map))
                        {
                            column.insertDefault();
                            return true;
                        }
                        return nested(column);
                    };
                }
                if (!target.isNullable() && !null_as_default)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot insert Avro Union(Null, {}) into non-nullable type {}. To use default value on NULL, enable setting "
                        "input_format_null_as_default", kindName(node.child(non_null).kind()), type->getName());
                /// The null of the union has no value to read in a type that cannot hold it.
                if (!target.isNullable())
                    reader_may_miss = true;
                const UInt32 null_map = addSink(AVRO_SINK_NULL_MAP);
                addNode(node, AVRO_ACTION_UNION, null_map, AVRO_NO_SINK, 1 - non_null, count);
                Reader nested_reader;
                for (UInt32 i = 0; i < count; ++i)
                {
                    if (i == non_null)
                        nested_reader = createReader(node.child(i), target.isNullable() ? removeNullable(type) : type);
                    else
                        skip(node.child(i));
                }
                return [this, null_map, nullable = target.isNullable(), nested = std::move(nested_reader)](IColumn & column)
                {
                    const bool is_null = next<UInt8>(null_map);
                    if (is_null)
                    {
                        column.insertDefault();
                        return nullable;
                    }
                    if (nullable)
                    {
                        auto & result = assert_cast<ColumnNullable &>(column);
                        nested(result.getNestedColumn());
                        result.getNullMapData().push_back(UInt8{0});
                    }
                    else
                        nested(column);
                    return true;
                };
            }
            if (target.isVariant())
            {
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*type);
                const UInt32 tags = addSink(AVRO_SINK_UNION_TAG, 4);
                addNode(node, AVRO_ACTION_UNION, tags, AVRO_NO_SINK, 0, count);
                VectorWithMemoryTracking<Reader> branch_readers(count);
                VectorWithMemoryTracking<ColumnVariant::Discriminator> branch_discriminators(count, ColumnVariant::NULL_DISCRIMINATOR);
                UnorderedSetWithMemoryTracking<ColumnVariant::Discriminator> used;
                for (UInt32 i = 0; i < count; ++i)
                {
                    const auto branch = node.child(i);
                    if (branch.kind() == AVRO_KIND_NULL)
                    {
                        skip(branch);
                        continue;
                    }
                    auto branch_type = inferType(branch);
                    branch_readers[i] = createReader(branch, branch_type);
                    const auto discriminator = variant_type.tryGetVariantDiscriminator(branch_type->getName());
                    if (!discriminator)
                        throw Exception(
                            ErrorCodes::ILLEGAL_COLUMN,
                            "Destination {} and Avro Union containing {} are not compatible. If this is an issue, then let the Input "
                            "Format infer the schema from the Avro message instead of providing custom Variant type.",
                            variant_type.getName(), branch_type->getName());
                    branch_discriminators[i] = *discriminator;
                    used.insert(*discriminator);
                }
                if (used.size() != variant_type.getVariants().size())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The number of distinct (non-null) union types in Avro record ({}) does not match the number of types in "
                        "destination Variant type ({}).", used.size(), variant_type.getVariants().size());
                return [this, tags, readers = std::move(branch_readers), discriminators = std::move(branch_discriminators)](IColumn & column)
                {
                    auto & variant = assert_cast<ColumnVariant &>(column);
                    const UInt32 tag = next<UInt32>(tags);
                    const auto global = discriminators[tag];
                    if (global == ColumnVariant::NULL_DISCRIMINATOR)
                        variant.insertDefault();
                    else
                    {
                        const auto local = variant.localDiscriminatorByGlobal(global);
                        auto & nested = variant.getVariantByLocalDiscriminator(local);
                        readers[tag](nested);
                        variant.getLocalDiscriminators().push_back(local);
                        variant.getOffsets().push_back(nested.size() - 1);
                    }
                    return true;
                };
            }
            break;
        }
        case AVRO_KIND_NULL:
        {
            if (!target.isNullable() && !null_as_default)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot insert Avro Null into non-nullable type {}. To use default value on NULL, enable setting "
                    "input_format_null_as_default", type->getName());
            if (!target.isNullable())
                reader_may_miss = true;
            skip(node);
            Reader reader = [nullable = target.isNullable()](IColumn & column)
            {
                column.insertDefault();
                return nullable;
            };
            reader.defaults_only = target.isNullable();
            return reader;
        }
        case AVRO_KIND_ENUM:
        {
            if (!target.isString() && !target.isEnum())
                break;
            const UInt32 sink = addSink(AVRO_SINK_ENUM_INDEX, 4);
            addNode(node, AVRO_ACTION_VALUE, sink);
            VectorWithMemoryTracking<std::string> enum_symbols;
            Row enum_mapping;
            for (UInt32 i = 0; i < info.symbol_count; ++i)
            {
                const std::string symbol(node.symbol(i));
                if (target.isEnum())
                    enum_mapping.push_back(dynamic_cast<const IDataTypeEnum &>(*type).castToValue(symbol));
                else
                    enum_symbols.push_back(symbol);
            }
            return [this, sink, symbols = std::move(enum_symbols), mapping = std::move(enum_mapping)](IColumn & column)
            {
                const UInt32 index = next<UInt32>(sink);
                if (!mapping.empty())
                    column.insert(mapping[index]);
                else
                    column.insertData(symbols[index].data(), symbols[index].size());
                return true;
            };
        }
        case AVRO_KIND_FIXED:
        {
            const UInt32 size = info.fixed_size;
            if (canBeDeserializedFromFixed(type, size) || target.isUUID())
            {
                const UInt32 sink = addSink(AVRO_SINK_FIXED_BYTES, size);
                addNode(node, AVRO_ACTION_VALUE, sink);
                return [this, sink, size, uuid = target.isUUID(), binary_uuid = size == 16 && node.logical() == AVRO_LOGICAL_UUID](IColumn & column)
                {
                    const auto bytes = nextFixed(sink, size);
                    if (uuid)
                    {
                        UUID value;
                        if (binary_uuid)
                        {
                            ReadBufferFromString input(bytes);
                            readBinaryBigEndian(UUIDHelpers::getHighBytes(value), input);
                            readBinaryBigEndian(UUIDHelpers::getLowBytes(value), input);
                        }
                        else
                        {
                            if (size != 36)
                                throw Exception(
                                    ErrorCodes::CANNOT_PARSE_UUID,
                                    "Cannot parse UUID from type Fixed, because it's size ({}) is not equal to the size of UUID (36)", size);
                            value = parseUUID({reinterpret_cast<const UInt8 *>(bytes.data()), bytes.size()});
                        }
                        assert_cast<DataTypeUUID::ColumnType &>(column).insertValue(value);
                    }
                    else
                        column.insertData(bytes.data(), bytes.size());
                    return true;
                };
            }
            if (target.isDecimal32())
                return createDecimalReader<DataTypeDecimal32>(node, type);
            if (target.isDecimal64())
                return createDecimalReader<DataTypeDecimal64>(node, type);
            if (target.isDecimal128())
                return createDecimalReader<DataTypeDecimal128>(node, type);
            if (target.isDecimal256())
                return createDecimalReader<DataTypeDecimal256>(node, type);
            if (target.isDateTime64())
                return createDecimalReader<DataTypeDateTime64>(node, type);
            break;
        }
        case AVRO_KIND_RECORD:
        {
            if (const auto metadata_position = variantMetadataPosition(node, type))
                return createVariantReader(node, type, *metadata_position);
            PathGuard guard(path, node);
            if (!target.isTuple())
                break;
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
            if (info.child_count != tuple_type.getElements().size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "The number of leaves in record doesn't match the number of elements in tuple");
            addNode(node, AVRO_ACTION_RECORD, AVRO_NO_SINK, AVRO_NO_SINK, 0, info.child_count);
            VectorWithMemoryTracking<std::pair<Reader, size_t>> element_readers;
            /// A record of fields that are all defaults (an empty one too) is a tuple of defaults. Field names are unique,
            /// so every element of the tuple has a reader.
            bool defaults = true;
            for (UInt32 i = 0; i < info.child_count; ++i)
            {
                const size_t position = tuple_type.getPositionByName(std::string(node.fieldName(i)));
                element_readers.emplace_back(createReader(node.child(i), tuple_type.getElements()[position]), position);
                defaults = defaults && element_readers.back().first.defaults_only;
            }
            Reader tuple_reader = [readers = std::move(element_readers)](IColumn & column)
            {
                auto & tuple = assert_cast<ColumnTuple &>(column);
                for (const auto & [reader, position] : readers)
                    reader(tuple.getColumn(position));
                if (tuple.tupleSize() == 0)
                    tuple.addSize(1);
                return true;
            };
            if (defaults)
                tuple_reader.defaults_only = true;
            return tuple_reader;
        }
        case AVRO_KIND_MAP:
        {
            if (!target.isMap())
                break;
            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            const auto key_type = map_type.getKeyType();
            const UInt32 offsets = addSink(AVRO_SINK_MAP_OFFSETS);
            const UInt32 keys = addSink(AVRO_SINK_BYTES);
            addNode(node, AVRO_ACTION_MAP, offsets, keys, 0, 1);
            auto value_reader = createReader(node.child(0), map_type.getValueType());
            return [this, offsets, keys, key_type, values = std::move(value_reader)](IColumn & column)
            {
                auto & map = assert_cast<ColumnMap &>(column);
                const UInt64 count = nextCount(offsets);
                forEachItem(
                    count,
                    [&]
                    {
                        const auto key = nextBytes(keys);
                        auto & key_column = map.getNestedData().getColumn(0);
                        if (WhichDataType(key_type).isString())
                            key_column.insertData(key.data(), key.size());
                        else
                        {
                            ReadBufferFromString input(key);
                            key_type->getDefaultSerialization()->deserializeWholeText(key_column, input, settings);
                        }
                        values(map.getNestedData().getColumn(1));
                    });
                auto & ends = map.getNestedColumn().getOffsets();
                ends.push_back(ends.back() + count);
                return true;
            };
        }
    }

    if (target.isNullable())
    {
        auto nested_reader = createReader(node, removeNullable(type));
        return [nested = std::move(nested_reader)](IColumn & column)
        {
            auto & nullable = assert_cast<ColumnNullable &>(column);
            nested(nullable.getNestedColumn());
            nullable.getNullMapData().push_back(UInt8{0});
            return true;
        };
    }
    throw Exception(
        ErrorCodes::ILLEGAL_COLUMN, "Type {} is not compatible with Avro {}:\n{}",
        type->getName(), kindName(node.kind()), node.getSchema().canonicalPrefix(MAX_SCHEMA_IN_MESSAGE_BYTES));
}

Plan::Impl::Action Plan::Impl::createAction(const Block & header, Node node, const std::string & current_path, bool direct)
{
    checkStackSize();
    if (header.has(current_path))
    {
        const size_t index = header.getPositionByName(current_path);
        const auto & column = header.getByPosition(index);
        try
        {
            current_column = column.name;
            reader_may_miss = false;
            auto column_reader = createReader(node, column.type, direct ? index : std::numeric_limits<size_t>::max());
            found[index] = true;
            /// Below a union branch (`direct` is false) a row that takes another branch reads nothing of the column.
            if (!direct || reader_may_miss)
            {
                may_miss[index] = 1;
                return [index, reader = std::move(column_reader)](MutableColumns & columns, VectorWithMemoryTracking<UInt8> & read)
                {
                    read[index] = reader(*columns[index]);
                };
            }
            return [index, reader = std::move(column_reader)](MutableColumns & columns, VectorWithMemoryTracking<UInt8> &)
            {
                reader(*columns[index]);
            };
        }
        catch (Exception & error)
        {
            error.addMessage("column " + column.name);
            throw;
        }
    }

    const bool requested = std::any_of(header.begin(), header.end(), [&](const auto & column)
    {
        return column.name.starts_with(current_path);
    });
    if (!requested)
    {
        skip(node);
        return [](MutableColumns &, VectorWithMemoryTracking<UInt8> &)
        {
        };
    }

    const auto info = node.info();
    /// `createAction` only descends while a finite header column path has the current prefix, so
    /// requested recursion is bounded by that path. `createReader` keeps the cycle guard for Tuples.
    if (node.kind() == AVRO_KIND_RECORD)
    {
        addNode(node, AVRO_ACTION_RECORD, AVRO_NO_SINK, AVRO_NO_SINK, 0, info.child_count);
        VectorWithMemoryTracking<Action> field_actions;
        for (UInt32 i = 0; i < info.child_count; ++i)
            field_actions.push_back(createAction(header, node.child(i), concatPath(current_path, node.fieldName(i)), direct));
        return [children = std::move(field_actions)](MutableColumns & columns, VectorWithMemoryTracking<UInt8> & read)
        {
            for (const auto & child : children)
                child(columns, read);
        };
    }
    if (node.kind() == AVRO_KIND_UNION)
    {
        const UInt32 tags = addSink(AVRO_SINK_UNION_TAG, 4);
        addNode(node, AVRO_ACTION_UNION, tags, AVRO_NO_SINK, 0, info.child_count);
        VectorWithMemoryTracking<Action> branch_actions;
        for (UInt32 i = 0; i < info.child_count; ++i)
        {
            const auto child = node.child(i);
            const auto name = child.hasName() ? child.simpleName() : std::string_view(kindName(child.kind()));
            branch_actions.push_back(createAction(header, child, concatPath(current_path, name), false));
        }
        return [this, tags, children = std::move(branch_actions)](MutableColumns & columns, VectorWithMemoryTracking<UInt8> & read)
        {
            children[next<UInt32>(tags)](columns, read);
        };
    }
    if (node.kind() == AVRO_KIND_ARRAY)
    {
        const auto names = Nested::getAllNestedColumnsForTable(header, current_path);
        const auto items = node.child(0);
        if (!names.empty() && items.kind() == AVRO_KIND_RECORD)
        {
            UnorderedMapWithMemoryTracking<std::string, DataTypePtr> nested_types;
            bool compatible = true;
            for (const auto & name : names)
            {
                const auto & type = header.getByName(name).type;
                if (!isArray(type))
                {
                    compatible = false;
                    break;
                }
                nested_types[Nested::splitName(name).second] = assert_cast<const DataTypeArray &>(*type).getNestedType();
            }
            const auto item_info = items.info();
            for (UInt32 i = 0; i < item_info.child_count && compatible; ++i)
                compatible = nested_types.contains(std::string(items.fieldName(i)));
            if (compatible)
            {
                PathGuard guard(path, items);
                const UInt32 offsets = addSink(AVRO_SINK_ARRAY_OFFSETS);
                addNode(node, AVRO_ACTION_ARRAY, offsets, AVRO_NO_SINK, 0, 1);
                addNode(items, AVRO_ACTION_RECORD, AVRO_NO_SINK, AVRO_NO_SINK, 0, item_info.child_count);
                VectorWithMemoryTracking<std::pair<size_t, Reader>> nested_readers;
                for (UInt32 i = 0; i < item_info.child_count; ++i)
                {
                    const std::string name(items.fieldName(i));
                    const size_t index = header.getPositionByName(Nested::concatenateName(current_path, name));
                    auto reader = createReader(items.child(i), nested_types.at(name));
                    found[index] = true;
                    /// A row without items reads nothing of the column.
                    may_miss[index] = 1;
                    nested_readers.emplace_back(index, std::move(reader));
                }
                const bool defaults = std::all_of(nested_readers.begin(), nested_readers.end(), [](const auto & entry) { return entry.second.defaults_only.has_value(); });
                return [this, offsets, defaults, readers = std::move(nested_readers)](MutableColumns & columns, VectorWithMemoryTracking<UInt8> & read)
                {
                    const UInt64 count = nextCount(offsets);
                    if (defaults)
                    {
                        /// A row without items leaves `read` as it is, otherwise each field has what its reader returns.
                        for (const auto & [index, reader] : readers)
                        {
                            if (count)
                                read[index] = *reader.defaults_only;
                            insertDefaults(assert_cast<ColumnArray &>(*columns[index]).getData(), count);
                        }
                    }
                    else
                    {
                        forEachItem(
                            count,
                            [&]
                            {
                                for (const auto & [index, reader] : readers)
                                    read[index] = reader(assert_cast<ColumnArray &>(*columns[index]).getData());
                            });
                    }
                    for (const auto & entry : readers)
                    {
                        auto & ends = assert_cast<ColumnArray &>(*columns[entry.first]).getOffsets();
                        ends.push_back(ends.back() + count);
                    }
                };
            }
        }
    }
    skip(node);
    return [](MutableColumns &, VectorWithMemoryTracking<UInt8> &)
    {
    };
}

namespace
{

/// A failing row can have appended array elements without its final offset, or
/// a Variant value without its discriminator. Top-level `popBack` alone cannot
/// remove those writes. Reconstruct each nested length from committed offsets.
void truncateColumn(IColumn & column, size_t rows)
{
    if (auto * array = typeid_cast<ColumnArray *>(&column))
    {
        auto & offsets = array->getOffsets();
        const size_t elements = rows ? offsets[rows - 1] : 0;
        offsets.resize(rows);
        truncateColumn(array->getData(), elements);
    }
    else if (auto * map = typeid_cast<ColumnMap *>(&column))
        truncateColumn(map->getNestedColumn(), rows);
    else if (auto * string = typeid_cast<ColumnString *>(&column))
    {
        auto & offsets = string->getOffsets();
        const size_t chars = rows ? offsets[rows - 1] : 0;
        offsets.resize(rows);
        string->getChars().resize(chars);
    }
    else if (auto * nullable = typeid_cast<ColumnNullable *>(&column))
    {
        nullable->getNullMapData().resize(rows);
        truncateColumn(nullable->getNestedColumn(), rows);
    }
    else if (auto * tuple = typeid_cast<ColumnTuple *>(&column))
    {
        if (tuple->tupleSize() == 0 && tuple->size() > rows)
            tuple->popBack(tuple->size() - rows);
        for (size_t i = 0; i < tuple->tupleSize(); ++i)
            truncateColumn(tuple->getColumn(i), rows);
    }
    else if (auto * variant = typeid_cast<ColumnVariant *>(&column))
    {
        /// The number of alternatives is bounded by the discriminator width.
        std::array<size_t, ColumnVariant::NULL_DISCRIMINATOR> sizes{};
        const auto & discriminators = variant->getLocalDiscriminators();
        const auto & offsets = variant->getOffsets();
        for (size_t i = 0; i < rows; ++i)
            if (discriminators[i] != ColumnVariant::NULL_DISCRIMINATOR)
                sizes[discriminators[i]] = std::max(sizes[discriminators[i]], static_cast<size_t>(offsets[i] + 1));
        variant->getLocalDiscriminators().resize(rows);
        variant->getOffsets().resize(rows);
        for (size_t i = 0; i < variant->getVariants().size(); ++i)
            truncateColumn(variant->getVariantByLocalDiscriminator(static_cast<ColumnVariant::Discriminator>(i)), sizes[i]);
    }
    else if (column.size() > rows)
        column.popBack(column.size() - rows);
}

/// The statuses of the decoder that mean "the window ended before the row did": the input stopped inside a value,
/// or a block count does not fit the bytes that remain in the window. Both are decided by the status code the
/// executor returns, never by its message. Only a longer window can tell them from corrupt data.
bool isIncompleteInput(Int32 status)
{
    return status == -AVRO_ERR_EOF || status == -AVRO_ERR_COUNT_EXCEEDS_PAYLOAD;
}

/// Runs `function` and returns the exception it throws, `nullptr` if it does not throw.
///
/// `decode` must not hold a `catch` handler itself. The Debug build compiles with `-Og`, and Clang (21, x86-64) lowered
/// a function that had four landing pads merged into one `catch (...)` handler (the `check` call, the row loop and the
/// two `rethrow_exception` temporaries) so that the pad of `check` started with `mov rax, r12`: it replaced the
/// exception pointer, which the unwinder passes in `rax`, by an unrelated register, and `__cxa_begin_catch` then read
/// address 0x38 and crashed every query that failed inside the executor (`TOO_DEEP_RECURSION`, `INCORRECT_DATA`, ...).
/// Here the handler has the only landing pad of a function that is never inlined, with no other cleanup to merge with.
template <typename Function>
NO_INLINE std::exception_ptr captureException(Function && function) noexcept
{
    try
    {
        function();
    }
    catch (...)
    {
        return std::current_exception();
    }
    return nullptr;
}

}

Plan::Impl::Outcome Plan::Impl::decode(
    std::span<const UInt8> input, UInt64 max_rows, MutableColumns & columns,
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> & missing, bool allow_incomplete, bool append_missing,
    const InputSource * source, bool more_input)
{
    committed_rows = 0;
    if (columns.size() != found.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Avro plan and destination column counts differ");
    missing.resize(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        initial_sizes[i] = columns[i]->size();
        if (max_rows > std::numeric_limits<size_t>::max() - initial_sizes[i])
            throw Exception(ErrorCodes::INCORRECT_DATA, "Avro row count exceeds addressable memory");
        if (!may_miss[i])
        {
            /// No row of this column is ever missing, so nothing is written and its vector stays empty. The
            /// caller may reuse the vectors of a plan of another schema, which may have tracked this column.
            if (!append_missing)
                missing[i].clear();
            continue;
        }
        missing_bases[i] = append_missing ? missing[i].size() : 0;
        if (max_rows > std::numeric_limits<size_t>::max() - missing_bases[i])
            throw Exception(ErrorCodes::INCORRECT_DATA, "Avro row count exceeds addressable memory");
        missing[i].resize(missing_bases[i]);
        missing[i].resize(missing_bases[i] + max_rows, 0);
    }
    if (!max_rows)
        return {true, 0, 0};

    SCOPE_EXIT({ releaseWideStaging(); });

    /// Initialize every fill level before any allocation: even a memory limit
    /// exception while binding the first sink must preserve existing columns.
    for (auto & sink : sinks)
        sink = {};
    for (auto & buffer : buffers)
    {
        buffer.cursor = 0;
        buffer.previous_offset = 0;
    }
    for (const UInt32 i : direct_sinks)
        buffers[i].direct(*columns[buffers[i].direct_column], sinks[i], 0, 0, DirectMode::Bind);

    UInt64 decoded_rows = 0;
    /// Bytes of `input` (the window that is current) that the rows decoded so far, and the part of the row in progress that
    /// the executor is done with, occupy.
    UInt64 consumed = 0;
    Int32 decode_status = AVRO_OK;
    char error[1024]{};
    /// Streaming: `input` is a window of a payload that continues. The mode is set for every call, so a state that an
    /// earlier call left in it (by an exception) does not leak into this one.
    bool more = source && more_input;
    std::exception_ptr decode_error = captureException(
        [&]
        {
            for (const UInt32 i : direct_sinks)
            {
                const auto & buffer = buffers[i];
                const auto & sink = sinks[i];
                if (buffer.desc.kind == AVRO_SINK_BYTES)
                    grow(i, sink.len, sink.len2 + max_rows, columns);
                else if (buffer.per_row)
                    grow(i, sink.len + max_rows, 0, columns);
                else
                    grow(i, sink.len + stagingElements(width(buffer.desc)), 0, columns);
            }
            for (UInt32 i = 0; i < buffers.size(); ++i)
            {
                const auto & buffer = buffers[i];
                if (!buffer.direct)
                    grow(i, stagingElements(width(buffer.desc)), buffer.desc.kind == AVRO_SINK_BYTES ? STAGING_ELEMENTS : 0, columns);
            }

            /// Hands the bytes that are done with to the source and continues with the window that it returns.
            auto refill = [&]
            {
                const UInt64 unconsumed = input.size() - consumed;
                const InputWindow window = (*source)(consumed);
                if (window.more && window.bytes.size() <= unconsumed)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "The source of an Avro payload returned no new bytes and did not end");
                input = window.bytes;
                consumed = 0;
                more = window.more;
                avro_state_set_streaming(state.get(), more);
            };

            avro_state_set_streaming(state.get(), more);
            while (decoded_rows < max_rows)
            {
                AvroProgress progress{};
                decode_status = avro_decode(
                    state.get(), program.get(), input.data() ? reinterpret_cast<const uint8_t *>(input.data()) + consumed : nullptr,
                    input.size() - consumed, max_rows - decoded_rows, sinks.data(), sinks.size(),
                    &progress, error, sizeof(error));
                decoded_rows += progress.rows_done;
                consumed += progress.consumed;
                if (decode_status < 0)
                    break;
                /// `NEED_SPACE` pauses the row in the executor: the sinks keep what it wrote so far, past the rows
                /// completed, and the next call (same input position, `progress.consumed` is 0) continues the row
                /// without decoding any byte again. The executor keeps the sinks of the paused row, with their fill
                /// levels, so this call must pass the same `sinks` array and `grow` must change only the buffers of the
                /// sinks it names, and keep every byte of them (`resize` does). The array gets back the fill levels of
                /// those sinks only until the row is done: the work does not depend on how many sinks there are.
                /// Every sink that lacks room for the block is named: all of them are grown before the next call, so
                /// that a record of many fields is one stop and not one for each field.
                if (progress.status == AVRO_STATUS_NEED_SPACE)
                {
                    for (UInt64 k = 0; k < progress.need_count; ++k)
                        grow(progress.needs[k].sink, progress.needs[k].cap, progress.needs[k].cap2, columns);
                }
                else if (progress.status == AVRO_STATUS_NEED_INPUT)
                {
                    /// The window ended inside a row, which the executor paused (or, if rows were completed before it,
                    /// rolled back to start the next call): `consumed` bytes are done with and the source drops them.
                    /// The sinks and the columns stay as they are; the next call continues the row with the new window.
                    if (!more)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "The Avro decoder asked for more input after the last window");
                    refill();
                }
                else if (progress.status == AVRO_STATUS_ROW_LIMIT)
                {
                    /// Only a row that was paused for input and has been completed stops a call before `max_rows`;
                    /// the rows that follow are decoded by the next call.
                }
                else if (decoded_rows < max_rows)
                {
                    /// The window ended exactly at a row boundary. With more input to come the next window goes on;
                    /// without, a row-boundary EOF is normal for the Rust block API, but not for this exact-row-count
                    /// interface.
                    if (more)
                    {
                        refill();
                        continue;
                    }
                    decode_status = -AVRO_ERR_EOF;
                    memcpy(error, "EOF reached", sizeof("EOF reached"));
                    break;
                }
            }
        });

    /// Shrinking does not allocate. Compact branch sinks remain C++ staging;
    /// only unconditional native scalar/String sinks, and the arrays of numbers, point into output columns.
    for (const UInt32 i : direct_sinks)
        buffers[i].direct(*columns[buffers[i].direct_column], sinks[i], 0, 0, DirectMode::Finish);

    /// Input that ends at a row boundary or inside a row is a normal outcome only for a caller that can supply more
    /// bytes. The executor committed the rows before the cut and rolled the cut row back, so they are what is left.
    const bool incomplete = allow_incomplete && !decode_error && isIncompleteInput(decode_status);

    /// Leaves the committed rows only. The state of the executor is not touched: it is at a row boundary.
    auto trim = [&]
    {
        for (size_t i = 0; i < columns.size(); ++i)
            truncateColumn(*columns[i], initial_sizes[i] + committed_rows);
        for (const size_t i : miss_columns)
            missing[i].resize(missing_bases[i] + committed_rows);
    };
    auto rollback = [&]
    {
        trim();
        avro_state_reset(state.get());
    };

    /// The first failure wins: a row that cannot be materialized, then the failed growth, then the executor's status.
    std::exception_ptr failure = captureException(
        [&]
        {
            for (UInt64 row = 0; row < decoded_rows; ++row)
            {
                for (const size_t i : miss_columns)
                    read_columns[i] = 0;
                action(columns, read_columns);
                for (const size_t i : miss_columns)
                {
                    if (columns[i]->size() < initial_sizes[i] + row + 1)
                        columns[i]->insertDefault();
                    missing[i][missing_bases[i] + row] = !read_columns[i];
                }
                ++committed_rows;
            }
            /// The reader of a column that is never missing inserts its value in every row, so nothing has to be
            /// checked or filled in per row. Fewer values than rows is a defect of the plan, not of the data.
            if (!decode_error && decode_status >= 0)
            {
                for (size_t i = 0; i < columns.size(); ++i)
                    if (!may_miss[i] && columns[i]->size() < initial_sizes[i] + decoded_rows)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "Avro column {} has {} rows after decoding {} rows from {}",
                            i, columns[i]->size() - initial_sizes[i], decoded_rows, initial_sizes[i]);
            }
        });
    if (!failure)
        failure = decode_error;
    if (!failure && !incomplete)
        failure = captureException([&] { check(decode_status, error); });

    if (failure)
    {
        rollback();
        std::rethrow_exception(failure);
    }
    if (incomplete)
    {
        trim();
        return {false, committed_rows, consumed};
    }
    return {true, committed_rows, consumed};
}

Plan::Plan(
    const Block & header, const Schema & schema, bool allow_missing_fields, bool null_as_default,
    const FormatSettings & settings, const NameSet & absent_columns, ValueValidator value_validator)
    : impl(std::make_unique<Impl>(header, schema, allow_missing_fields, null_as_default, settings, absent_columns, std::move(value_validator)))
{
}

Plan::~Plan() = default;

void Plan::beginBlock()
{
    avro_state_reset(impl->state.get());
}

UInt64 Plan::decode(
    std::span<const UInt8> input, UInt64 max_rows, MutableColumns & columns,
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> & missing)
{
    return impl->decode(input, max_rows, columns, missing, false, false).consumed;
}

UInt64 Plan::decodeStream(
    InputWindow window, const InputSource & next, UInt64 max_rows, MutableColumns & columns,
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> & missing)
{
    return impl->decode(window.bytes, max_rows, columns, missing, false, true, &next, window.more).consumed;
}

Plan::DatumResult Plan::decodeDatum(
    std::span<const UInt8> input, bool more_input, MutableColumns & columns,
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> & missing)
{
    avro_state_reset(impl->state.get());
    const auto outcome = impl->decode(input, 1, columns, missing, more_input, false);
    return {outcome.complete, outcome.complete ? outcome.consumed : 0};
}

UInt64 Plan::committedRows() const
{
    return impl->committed_rows;
}

}
}

#endif
