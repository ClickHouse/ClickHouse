#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorToString.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/FieldToDataType.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/Arena.h>
#include <Common/FieldBinaryEncoding.h>
#include <Common/SipHash.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>

#include <algorithm>
#include <memory>


namespace DB
{

static AggregateFunctionPtr createAggregateFunctionMergedJSONPatch(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *);
void registerAggregateFunctionMergedJSONPatch(AggregateFunctionFactory & factory);

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** `mergedJSONPatch` stores the effective last-write-wins view of RFC 7396-style replacements over
  * `JSON` values. Each update is ordered by its sort key, and newer writes replace older writes for
  * the same path or any ancestor/descendant conflicting path.
  *
  * Arrays are preserved as atomic replacement values, including mixed arrays such as `[42, "x", {"k": 1}]`.
  *
  * Three `ColumnObject` limitations affect RFC 7396 conformance:
  *
  * 1. Null deletion: `ColumnObject` drops null-valued members on insertion, so a patch
  *    like `{"key": null}` cannot remove a key — `ColumnObject` cannot distinguish between
  *    "key is absent" and "key has null value".
  *
  * 2. Empty-object replacement: `ColumnObject` drops paths whose value is an empty object
  *    `{}` before the aggregate ever sees them. A newer patch `{"a": {}}` therefore cannot
  *    displace an older scalar or array at `a`; the old leaf survives unchanged instead of
  *    being replaced by `{}` as RFC 7396 requires.
  *
  * 3. Non-`Nullable` typed-path absence: when a `JSON` column has a typed path with a
  *    non-nullable type (e.g., `JSON(a UInt32)`), `ColumnObject` fills missing values with
  *    the type default (`0` for `UInt32`). The aggregate cannot distinguish "path absent
  *    from this patch" from "patch explicitly wrote the default value". A later patch that
  *    omits `a` therefore produces a batch entry `a = 0` that silently erases an older
  *    non-zero value. To avoid this, use `Nullable` typed paths
  *    (e.g., `JSON(a Nullable(UInt32))`): a `NULL` value in a nullable typed-path column
  *    unambiguously represents absence and is skipped by the aggregate.
  */
struct AggregateFunctionMergedJSONPatchData
{

    struct SortKey
    {
        Field value;

        SortKey() = default;

        explicit SortKey(Field value_)
            : value(std::move(value_))
        {
        }

        const Field & toField() const
        {
            return value;
        }

        bool operator<(const SortKey & other) const
        {
            return value < other.value;
        }

        bool operator<=(const SortKey & other) const
        {
            return value <= other.value;
        }

        bool operator>(const SortKey & other) const
        {
            return other < *this;
        }
    };

    struct EncodedField
    {
        enum class Kind : UInt8
        {
            Empty = 0,
            Int64 = 1,
            UInt64 = 2,
            String = 3,
            BinaryNonObjectField = 4,
        };

        Kind kind = Kind::Empty;
        Int64 inline_int64 = 0;
        UInt64 inline_uint64 = 0;
        /// Owned string storage for String and BinaryNonObjectField kinds.
        /// Using owned String rather than an arena pointer means the bytes are freed immediately
        /// when the Entry is erased, keeping memory proportional to the live state size rather
        /// than to the total number of updates processed.
        String data; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        EncodedField() = default;

        explicit EncodedField(Int64 value_)
            : kind(Kind::Int64)
            , inline_int64(value_)
        {
        }

        explicit EncodedField(UInt64 value_)
            : kind(Kind::UInt64)
            , inline_uint64(value_)
        {
        }

        EncodedField(Kind kind_, String data_)
            : kind(kind_)
            , data(std::move(data_))
        {
        }

        std::string_view dataView() const { return data; }

        Field get() const
        {
            switch (kind)
            {
                case Kind::Empty:
                    return {};
                case Kind::Int64:
                    return Field(inline_int64);
                case Kind::UInt64:
                    return Field(inline_uint64);
                case Kind::String:
                    return Field(data);
                case Kind::BinaryNonObjectField:
                {
                    ReadBufferFromString buf(data);
                    return decodeField(buf);
                }
            }

            UNREACHABLE();
        }
    };

    struct Entry
    {
        /// Owned string for the path. Freed when the entry is erased, so path memory is
        /// proportional to the number of live entries, not to the total number of updates.
        String path; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        EncodedField value;
        SortKey sort_key;

        std::string_view pathView() const { return path; }
    };

    VectorWithMemoryTracking<Entry> entries;

    static EncodedField encodeField(Field value)
    {
        switch (value.getType())
        {
            case Field::Types::Int64:
                return EncodedField(value.safeGet<Int64>());
            case Field::Types::UInt64:
                return EncodedField(value.safeGet<UInt64>());
            case Field::Types::String:
                return EncodedField(EncodedField::Kind::String, value.safeGet<String>());
            default:
            {
                WriteBufferFromOwnString buf;
                ::DB::encodeField(value, buf);
                return EncodedField(EncodedField::Kind::BinaryNonObjectField, std::move(buf.str()));
            }
        }
    }

    static bool isObjectField(const Field & value)
    {
        return value.getType() == Field::Types::Object;
    }

    static bool isDescendantPath(std::string_view ancestor, std::string_view path)
    {
        return path.size() > ancestor.size()
            && path.starts_with(ancestor)
            && path[ancestor.size()] == '.';
    }

    static bool pathsConflict(std::string_view lhs, std::string_view rhs)
    {
        return lhs == rhs || isDescendantPath(lhs, rhs) || isDescendantPath(rhs, lhs);
    }

    static auto findInsertPosition(VectorWithMemoryTracking<Entry> & entries, std::string_view path)
    {
        return std::lower_bound(
            entries.begin(),
            entries.end(),
            path,
            [](const Entry & entry, std::string_view rhs_path)
            {
                return entry.pathView() < rhs_path;
            });
    }

    bool hasNewerConflictingEntry(std::string_view path, const SortKey & sort_key) const
    {
        for (const auto & entry : entries)
        {
            if (pathsConflict(entry.pathView(), path) && entry.sort_key > sort_key)
                return true;
        }

        return false;
    }

    void eraseShadowedEntries(std::string_view path, const SortKey & sort_key)
    {
        entries.erase(
            std::remove_if(
                entries.begin(),
                entries.end(),
                [&](const Entry & entry)
                {
                    return pathsConflict(entry.pathView(), path) && entry.sort_key <= sort_key;
                }),
            entries.end());
    }

    void pushLeafEntry(std::string_view path, Field value, const SortKey & sort_key)
    {
        Entry entry;
        entry.path = path;
        entry.value = encodeField(std::move(value));
        entry.sort_key = sort_key;

        auto it = findInsertPosition(entries, path);
        entries.insert(it, std::move(entry));
    }

    void insertLeafEntry(std::string_view path, Field value, const SortKey & sort_key)
    {
        if (hasNewerConflictingEntry(path, sort_key))
            return;

        eraseShadowedEntries(path, sort_key);
        pushLeafEntry(path, std::move(value), sort_key);
    }

    void insertPathValue(std::string_view path, Field value, const SortKey & sort_key)
    {
        if (!isObjectField(value))
        {
            insertLeafEntry(path, std::move(value), sort_key);
            return;
        }

        const auto & object = value.safeGet<Object>();
        for (const auto & [child_key, child_value] : object)
        {
            String child_path(path);
            if (!child_path.empty())
                child_path += '.';
            child_path += child_key;
            insertPathValue(child_path, child_value, sort_key);
        }
    }

    static EncodedField readEncodedField(ReadBuffer & buf)
    {
        UInt8 encoded_kind = 0;
        readBinary(encoded_kind, buf);

        auto kind = static_cast<EncodedField::Kind>(encoded_kind);

        if (kind != EncodedField::Kind::Empty
            && kind != EncodedField::Kind::Int64
            && kind != EncodedField::Kind::UInt64
            && kind != EncodedField::Kind::String
            && kind != EncodedField::Kind::BinaryNonObjectField)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Invalid terminal kind while deserializing `mergedJSONPatch`: byte={}",
                static_cast<UInt64>(encoded_kind));
        }

        switch (kind)
        {
            case EncodedField::Kind::Empty:
                return EncodedField();
            case EncodedField::Kind::Int64:
            {
                Int64 value = 0;
                readVarInt(value, buf);
                return EncodedField(value);
            }
            case EncodedField::Kind::UInt64:
            {
                UInt64 value = 0;
                readVarUInt(value, buf);
                return EncodedField(value);
            }
            case EncodedField::Kind::String:
            case EncodedField::Kind::BinaryNonObjectField:
            {
                String stored;
                readStringBinary(stored, buf);
                return EncodedField(kind, std::move(stored));
            }
        }

        UNREACHABLE();
    }

    void add(const IColumn & json_column, size_t row_num, Arena &)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);

        /// Hash the row into a temporary SipHash — no arena allocation needed.
        SipHash hash;
        object_column.updateHashWithValue(row_num, hash);
        SortKey sort_key = SortKey(Field(hash.get128()));

        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void addWithKey(const IColumn & json_column, const IColumn & key_column, size_t row_num, Arena &)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        SortKey sort_key = SortKey(key_column[row_num]);
        addKeyValuePairs(object_column, row_num, sort_key);
    }

    /// A leaf entry used as a staging buffer before atomic batch insertion.
    /// path is owned (String) so collectLeaves and deserialize can move/copy into it safely.
    struct LeafRef
    {
        String path;
        Field value;
        SortKey sort_key;
    };

    /// Recursively flatten value into (path, scalar-or-array, sort_key) leaf entries.
    static void collectLeaves(String path, Field value, const SortKey & sort_key, std::vector<LeafRef> & out) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        if (!isObjectField(value))
        {
            out.push_back({std::move(path), std::move(value), sort_key});
            return;
        }
        const auto & object = value.safeGet<Object>();
        for (const auto & [child_key, child_value] : object)
        {
            String child_path = path.empty() ? child_key : path + '.' + child_key;
            collectLeaves(child_path, child_value, sort_key, out);
        }
    }

    void addKeyValuePairs(const ColumnObject & object_column, size_t row_num, const SortKey & sort_key)
    {
        /// Collect all leaf (path, value) pairs from the row, then insert them atomically.
        ///
        /// insertBatchAtomic scopes all conflict checks and erasures to the pre-existing state,
        /// so intra-row siblings (e.g. "a" and "a.b" from JSON(a UInt32, `a.b` UInt32)) cannot
        /// erase each other.
        ///
        /// SortedPathsIterator already skips null-valued dynamic paths (null = absent).
        /// For typed paths backed by Nullable columns, null also means absent, so we mirror
        /// that skip here: a Nullable typed path that is NULL in this row was not written by
        /// the patch and must not displace an older non-null value.
        ///
        /// Non-Nullable typed paths (e.g. UInt32) have no null representation.  Their column
        /// stores the type default (e.g. 0) whenever the patch omitted the path.  We cannot
        /// distinguish "written as 0" from "absent, filled with default", so we pass them
        /// through — see the documentation limitation for the consequence.
        std::vector<LeafRef> batch; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        ColumnObject::SortedPathsIterator it(object_column, row_num);
        while (!it.end())
        {
            auto path_info = it.getCurrentPathInfo();
            /// Skip Nullable typed paths that are null — null means the patch omitted this path.
            if (path_info.type == ColumnObject::SortedPathsIterator::PathType::TYPED
                && path_info.column->isNullAt(path_info.row))
            {
                it.next();
                continue;
            }
            Field value;
            path_info.column->get(path_info.row, value);
            collectLeaves(String(path_info.path), std::move(value), sort_key, batch);
            it.next();
        }

        insertBatchAtomic(batch);
    }

    /// Insert a flat list of leaf entries into this state, treating the entire batch as atomic
    /// with respect to the pre-existing state.
    ///
    /// All three call sites (addKeyValuePairs, merge, deserialize) use this path.  A state can
    /// legitimately contain conflicting-path entries at the same sort key (e.g. both "a" and
    /// "a.b" from a row of JSON(a UInt32, `a.b` UInt32)). Replaying them one-by-one through
    /// insertLeafEntry would let a later entry erase an earlier sibling, because
    /// eraseShadowedEntries uses sort_key <= incoming, which is true for equal keys.
    ///
    /// Three-phase approach:
    ///   1. Filter: find batch entries not blocked by a pre-existing newer entry.
    ///   2. Erase:  remove all pre-existing entries shadowed by any survivor.
    ///   3. Insert: push all survivors into the now-clean state.
    ///
    /// Phases 2 and 3 are kept strictly separate so that a survivor pushed in phase 3 is
    /// never seen by the erase pass of another survivor.  Using a positional index limit
    /// (entries[0..existing_count)) breaks down when pushLeafEntry inserts into a sorted
    /// position that falls below the limit, causing a later survivor's erase pass to
    /// mis-identify the newly pushed sibling as a pre-existing entry and erase it.
    void insertBatchAtomic(std::vector<LeafRef> & batch) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        size_t existing_count = entries.size();

        /// Phase 1: collect indices of batch entries not blocked by a pre-existing newer entry.
        std::vector<size_t> survivors; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        survivors.reserve(batch.size());
        for (size_t i = 0; i < batch.size(); ++i)
        {
            bool blocked = false;
            for (size_t j = 0; j < existing_count; ++j)
            {
                if (pathsConflict(entries[j].pathView(), batch[i].path) && entries[j].sort_key > batch[i].sort_key)
                {
                    blocked = true;
                    break;
                }
            }
            if (!blocked)
                survivors.push_back(i);
        }

        /// Phase 2: remove all pre-existing entries shadowed by any survivor.
        /// No insertions happen here, so entries contains only pre-existing data and the
        /// j < existing_count guard is unnecessary — entries.size() == existing_count still.
        for (size_t idx : survivors)
        {
            entries.erase(
                std::remove_if(
                    entries.begin(),
                    entries.end(),
                    [&](const Entry & e)
                    {
                        return pathsConflict(e.pathView(), batch[idx].path)
                            && e.sort_key <= batch[idx].sort_key;
                    }),
                entries.end());
        }

        /// Phase 3: push all survivors. The pre-existing state is already clean; batch
        /// siblings are not yet in entries, so pushLeafEntry cannot accidentally erase them.
        for (size_t idx : survivors)
            pushLeafEntry(batch[idx].path, std::move(batch[idx].value), batch[idx].sort_key);
    }

    void merge(const AggregateFunctionMergedJSONPatchData & other, Arena &)
    {
        std::vector<LeafRef> batch; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        batch.reserve(other.entries.size());
        for (const auto & entry : other.entries)
            batch.push_back({String(entry.pathView()), entry.value.get(), entry.sort_key});
        insertBatchAtomic(batch);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(entries.size(), buf);
        for (const auto & entry : entries)
        {
            writeStringBinary(entry.pathView(), buf);
            writeBinary(static_cast<UInt8>(entry.value.kind), buf);
            switch (entry.value.kind)
            {
                case EncodedField::Kind::Empty:
                    break;
                case EncodedField::Kind::Int64:
                    writeVarInt(entry.value.inline_int64, buf);
                    break;
                case EncodedField::Kind::UInt64:
                    writeVarUInt(entry.value.inline_uint64, buf);
                    break;
                case EncodedField::Kind::String:
                case EncodedField::Kind::BinaryNonObjectField:
                    writeStringBinary(entry.value.dataView(), buf);
                    break;
            }
            DB::encodeField(entry.sort_key.toField(), buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena &)
    {
        entries.clear();

        size_t size = 0;
        readVarUInt(size, buf);

        /// Read all entries into a batch first, then insert atomically.
        /// Inserting one-by-one through insertPathValue is incorrect: a state can contain
        /// conflicting-path siblings (e.g. "a" and "a.b") at the same sort key, and sequential
        /// insertion would let the second sibling erase the first.
        std::vector<LeafRef> batch; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        batch.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            LeafRef & lv = batch.emplace_back();
            readStringBinary(lv.path, buf);
            lv.value = readEncodedField(buf).get();
            lv.sort_key = SortKey(decodeField(buf));
        }

        insertBatchAtomic(batch);
    }

    void insertResultInto(IColumn & to, const DataTypePtr &) const
    {
        auto & result_column = assert_cast<ColumnObject &>(to);

        if (entries.empty())
        {
            result_column.insertDefault();
            return;
        }

        size_t current_size = result_column.size();
        auto [shared_data_paths, shared_data_values] = result_column.getSharedDataPathsAndValues();

        for (const auto & entry : entries)
        {
            std::string_view path = entry.pathView();
            Field value = entry.value.get();

            if (auto typed_it = result_column.getTypedPaths().find(path); typed_it != result_column.getTypedPaths().end())
            {
                typed_it->second->insert(value);
            }
            else if (auto dynamic_it = result_column.getDynamicPathsPtrs().find(path); dynamic_it != result_column.getDynamicPathsPtrs().end())
            {
                dynamic_it->second->insert(value);
            }
            else if (auto * dynamic_path_column = result_column.tryToAddNewDynamicPath(path))
            {
                dynamic_path_column->insert(value);
            }
            else if (!value.isNull())
            {
                /// Dynamic path limit reached: write directly to shared data using Dynamic
                /// binary serialization. This is the same encoding ColumnObject::insert uses
                /// for overflow paths and handles any Field including arrays containing objects.
                shared_data_paths->insertData(path.data(), path.size());
                auto & chars = shared_data_values->getChars();
                {
                    WriteBufferFromVector<ColumnString::Chars> value_buf(chars, AppendModeTag{});
                    DataTypeDynamic().getDefaultSerialization()->serializeBinary(value, value_buf, {});
                }
                shared_data_values->getOffsets().push_back(chars.size());
            }
        }

        result_column.getSharedDataOffsets().push_back(shared_data_paths->size());

        for (auto & [_, column] : result_column.getTypedPaths())
        {
            if (column->size() == current_size)
                column->insertDefault();
        }

        for (auto & [_, column] : result_column.getDynamicPathsPtrs())
        {
            if (column->size() == current_size)
                column->insertDefault();
        }
    }
};


class AggregateFunctionMergedJSONPatch final
    : public IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData, AggregateFunctionMergedJSONPatch>
{
private:
    bool has_sort_key;

public:
    explicit AggregateFunctionMergedJSONPatch(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData, AggregateFunctionMergedJSONPatch>(
            argument_types_, {}, argument_types_[0])
        , has_sort_key(argument_types_.size() > 1)
    {
    }

    String getName() const override
    {
        return "mergedJSONPatch";
    }

    bool allocatesMemoryInArena() const override
    {
        return false;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (has_sort_key)
            data(place).addWithKey(*columns[0], *columns[1], row_num, *arena);
        else
            data(place).add(*columns[0], row_num, *arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), *arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).deserialize(buf, *arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        data(place).insertResultInto(to, result_type);
    }
};


static AggregateFunctionPtr createAggregateFunctionMergedJSONPatch(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);

    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}. Expected 1 or 2 arguments (JSON value and optional sort key), got {} arguments",
            name, argument_types.size());

    if (!isObject(argument_types[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument for aggregate function {}. Expected type JSON",
            argument_types[0]->getName(), name);

    if (argument_types.size() == 2 && !argument_types[1]->isComparable())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of second argument for aggregate function {}. Expected comparable type for sort key",
            argument_types[1]->getName(), name);

    return std::make_shared<AggregateFunctionMergedJSONPatch>(argument_types);
}

void registerAggregateFunctionMergedJSONPatch(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {
        .returns_default_when_only_null = false,
        .is_order_dependent = true
    };

    FunctionDocumentation::Description description = R"(
Aggregates JSON values by merging them with last-write-wins semantics, implementing the core merge
behavior of RFC 7396 JSON Merge Patch at the path level.

The aggregate function stores state as triplets (key, value, sorting_key) where each key (JSON path)
only keeps the latest effective record according to the sorting_key. Object writes are flattened into
descendant paths, and ancestor non-object writes shadow conflicting descendants.

When called with one argument `mergedJSONPatch(json_col)`, a deterministic sort key is generated
from the serialized JSON object to ensure consistent ordering across distributed queries.

When called with two arguments `mergedJSONPatch(json_col, sort_key)`, the provided sort_key
determines which value wins for each JSON path. The value with the largest sort_key is retained.

If two conflicting patches have equal sort keys, the result is order-dependent: the patch processed
later wins the tie. This matches the aggregate implementation and means users should not rely on
`ORDER BY` being removed before aggregation to break ties deterministically.

LIMITATIONS (inherited from `ColumnObject`):

1. Null deletion: a patch `{"key": null}` does not remove the key. `ColumnObject` drops
    null-valued members on insertion, so the function cannot distinguish "key absent" from
    "key is null".

2. Empty-object replacement: a patch `{"a": {}}` cannot displace an older scalar or array
    at path `a`. `ColumnObject` silently drops paths whose value is an empty object `{}`,
    so the newer patch contributes nothing and the old value survives.

3. Non-Nullable typed-path absence: when a `JSON` column declares a typed path with a
    non-nullable type (e.g., `JSON(a UInt32)`), a row that omits `a` is stored with the
    type default value (e.g., `0`). The aggregate cannot tell "absent" from "explicitly
    written as the default", so a newer patch that omits `a` silently erases an older
    non-zero value. To avoid this, declare typed paths as `Nullable`
    (e.g., `JSON(a Nullable(UInt32))`). A null in a nullable typed path is treated as
    "path absent" and is correctly skipped.
)";

    FunctionDocumentation::Syntax syntax = "mergedJSONPatch(json[, sort_key])";

    FunctionDocumentation::Arguments arguments = {
        {"json", "JSON column to aggregate.", {"JSON"}},
        {"sort_key", "Optional. Comparable column that determines which write wins for each path. "
                     "The row with the largest sort_key value is retained. "
                     "When omitted, a deterministic key is derived from the serialized JSON object.", {}}
    };

    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a single JSON object that is the result of merging all input JSON objects.",
        {"JSON"}
    };

    FunctionDocumentation::Examples examples = {
        {
            "Basic usage with sort key",
            R"(
SELECT mergedJSONPatch(json, sort_key) FROM
(
    SELECT '{"a":1}'::JSON AS json, 1 AS sort_key
    UNION ALL
    SELECT '{"b":2}'::JSON, 2
    UNION ALL
    SELECT '{"a":3, "c":4}'::JSON, 3
);
            )",
            R"(
┌─mergedJSONPatch(json, sort_key)─┐
│ {"a":3,"b":2,"c":4}              │
└──────────────────────────────────┘
            )"
        }
    };

    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;

    FunctionDocumentation documentation = {
        description,
        syntax,
        arguments,
        {},
        returned_value,
        examples,
        introduced_in,
        category
    };

    factory.registerFunction("mergedJSONPatch", {createAggregateFunctionMergedJSONPatch, documentation, properties});
}

}

