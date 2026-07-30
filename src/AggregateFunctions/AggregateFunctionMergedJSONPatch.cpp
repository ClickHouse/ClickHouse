#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorToString.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/Arena.h>
#include <Common/FieldBinaryEncoding.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>

#include <algorithm>
#include <memory>
#include <boost/smart_ptr/intrusive_ptr.hpp>


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
  * Deep merging applies only to paths that `ColumnObject` stores as separate dynamic paths (i.e.
  * paths that `ColumnObject` has already flattened at parse time). Every typed path — regardless
  * of its declared type, including `Map(K,V)`, `JSON`, `Dynamic`, `Tuple(…)`, `Variant(…)`,
  * `Array(…)` — is stored as a single serialized blob and is treated as an atomic leaf: the whole
  * value is replaced by the newer patch. There is no deep merge inside any typed path.
  *
  * Five `ColumnObject` limitations affect RFC 7396 conformance:
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
  *
  * 4. All typed paths are atomic: every typed path — `Map(K,V)`, `JSON`, `Dynamic`,
  *    `Tuple(…)`, `Variant(…)`, `Array(…)`, or any other declared type — is stored as a
  *    single serialized value per row. The aggregate replaces the entire value atomically
  *    rather than deep-merging its contents key-by-key as RFC 7396 would require.
  *    Only paths that `ColumnObject` stores as dynamic paths (i.e. scalar leaves inferred
  *    at parse time) are subject to deep-merge path-by-path semantics.
  *
  * 5. Dot-in-key ambiguity: `ColumnObject` represents `{"a":{"b":1}}` and `{"a.b":1}` with
  *    the same internal path `a.b`. A single row can therefore expose both `a` and `a.b` as
  *    independent peers. When a newer patch writes only `a`, the ancestor/descendant conflict
  *    rule erases `a.b`; when it writes only `a.b`, the same rule erases `a`. Neither is
  *    strictly correct when both paths coexist. To avoid this, enable
  *    `json_type_escape_dots_in_keys = 1`: literal dots in JSON keys are then percent-encoded
  *    (e.g. `a.b` → `a%2Eb`), making them distinct from nested paths and eliminating the
  *    false conflict.
  */
struct AggregateFunctionMergedJSONPatchData
{

    /// Efficient sort-key value. Stores Int64/UInt64 inline (no allocation), String in-place,
    /// and falls back to a heap-allocated Field only for exotic types (Float, Decimal, UUID, …).
    /// Comparisons are a single tag-dispatch on a small enum instead of Field's heavyweight
    /// type dispatch.
    ///
    /// All leaf paths in one input row share the same sort key. To avoid copying a potentially
    /// large String N times (once per path), SortKeyData is ref-counted via boost::intrusive_ptr
    /// with a plain (non-atomic) int refcount — the aggregate state is single-threaded.
    struct SortKeyData
    {
        enum class Kind : UInt8
        {
            Int64  = 0,
            UInt64 = 1,
            String = 2,
            Field  = 3,  ///< fallback for Float, Decimal, UUID, etc.
        };

        Kind kind;
        Int64  i64 = 0;
        UInt64 u64 = 0;
        String str; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        Field  field;

        int refcount = 0;

        /// Construct from a Field, choosing the most compact representation.
        explicit SortKeyData(Field v)
        {
            switch (v.getType())
            {
                case Field::Types::Int64:
                    kind = Kind::Int64;
                    i64  = v.safeGet<Int64>();
                    break;
                case Field::Types::UInt64:
                    kind = Kind::UInt64;
                    u64  = v.safeGet<UInt64>();
                    break;
                case Field::Types::String:
                    kind = Kind::String;
                    str  = v.safeGet<String>();
                    break;
                default:
                    kind  = Kind::Field;
                    field = std::move(v);
                    break;
            }
        }

        bool operator<(const SortKeyData & other) const
        {
            /// Fast path: same kind on both sides (the common case for non-nullable, non-Variant keys).
            if (kind == other.kind)
            {
                switch (kind)
                {
                    case Kind::Int64:  return i64   < other.i64;
                    case Kind::UInt64: return u64   < other.u64;
                    case Kind::String: return str   < other.str;
                    case Kind::Field:  return field < other.field;
                }
                UNREACHABLE();
            }
            /// Kinds differ — this happens with Nullable(T) (null rows emit Field::Null, non-null
            /// rows emit the inner type) and Variant(...).  Fall back to Field comparison, which
            /// handles all type combinations correctly.
            return toField() < other.toField();
        }

        bool operator<=(const SortKeyData & other) const { return !(other < *this); }
        bool operator>(const SortKeyData & other) const  { return other < *this; }

        Field toField() const
        {
            switch (kind)
            {
                case Kind::Int64:  return Field(i64);
                case Kind::UInt64: return Field(u64);
                case Kind::String: return Field(str);
                case Kind::Field:  return field;
            }
            UNREACHABLE();
        }

        friend void intrusive_ptr_add_ref(SortKeyData * p) noexcept { ++p->refcount; }
        friend void intrusive_ptr_release(SortKeyData * p) noexcept { if (--p->refcount == 0) delete p; }
    };

    using SortKeyPtr = boost::intrusive_ptr<SortKeyData>;

    static SortKeyPtr makeSortKey(Field v)
    {
        return SortKeyPtr(new SortKeyData(std::move(v)));
    }

    /// Thin wrapper that lets call sites use comparison operators directly on a SortKeyPtr.
    /// We pass SortKeyPtr by const-ref everywhere; this avoids bumping the refcount on comparisons.
    struct SortKey
    {
        SortKeyPtr ptr;

        bool operator<(const SortKey & other) const  { return *ptr < *other.ptr; }
        bool operator<=(const SortKey & other) const { return *ptr <= *other.ptr; }
        bool operator>(const SortKey & other) const  { return *ptr > *other.ptr; }
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
            /// Dynamic binary format: encodeDataType(type) + type->serializeBinary(value).
            /// Used for typed-path leaf values so that types like Date, DateTime, UUID that
            /// have no corresponding Field type are preserved across serialization round-trips.
            DynamicBinary = 5,
        };

        Kind kind = Kind::Empty;
        Int64 inline_int64 = 0;
        UInt64 inline_uint64 = 0;
        /// Owned string storage for String, BinaryNonObjectField, and DynamicBinary kinds.
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
                case Kind::DynamicBinary:
                {
                    Field field;
                    ReadBufferFromString buf(data);
                    DataTypeDynamic().getDefaultSerialization()->deserializeBinary(field, buf, {});
                    return field;
                }
            }

            UNREACHABLE();
        }

        /// True if the stored value is a null / Nothing (written by Dynamic serialization for
        /// absent typed-path values that escaped the Nullable null-skip path).
        bool isNull() const
        {
            if (kind == Kind::Empty)
                return true;
            if (kind == Kind::DynamicBinary)
            {
                Field f = get();
                return f.isNull();
            }
            return false;
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

    void pushLeafEntry(std::string_view path, EncodedField value, const SortKey & sort_key)
    {
        Entry entry;
        entry.path = path;
        entry.value = std::move(value);
        entry.sort_key = sort_key;

        auto it = findInsertPosition(entries, path);
        entries.insert(it, std::move(entry));
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
            && kind != EncodedField::Kind::BinaryNonObjectField
            && kind != EncodedField::Kind::DynamicBinary)
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
            case EncodedField::Kind::DynamicBinary:
            {
                String stored;
                readStringBinary(stored, buf);
                return EncodedField(kind, std::move(stored));
            }
        }

        UNREACHABLE();
    }

    void addWithKey(const IColumn & json_column, const IColumn & key_column, size_t row_num, const DataTypeObject * obj_type = nullptr)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        /// Create one SortKeyData and share it across all leaf paths from this row.
        /// Copying a SortKeyPtr is O(1) regardless of key size.
        SortKey sort_key{makeSortKey(key_column[row_num])};
        addKeyValuePairs(object_column, row_num, sort_key, obj_type);
    }

    /// A leaf entry used as a staging buffer before atomic batch insertion.
    /// path is owned (String) so deserialize can move/copy into it safely.
    struct LeafRef
    {
        String path;
        EncodedField value;
        SortKey sort_key;
    };

    void addKeyValuePairs(const ColumnObject & object_column, size_t row_num, const SortKey & sort_key,
                          const DataTypeObject * obj_type)
    {
        /// Collect all leaf (path, value) pairs from the row, then insert them atomically.
        ///
        /// insertBatchAtomic scopes all conflict checks and erasures to the pre-existing state,
        /// so intra-row siblings (e.g. "a" and "a.b" from JSON(a UInt32, `a.b` UInt32)) cannot
        /// erase each other.
        ///
        /// SortedPathsIterator skips null dynamic paths (null = absent).
        /// For typed paths backed by Nullable columns isCurrentTypedNull() handles the same skip.
        ///
        /// Non-Nullable typed paths (e.g. UInt32) have no null representation.  Their column
        /// stores the type default (e.g. 0) whenever the patch omitted the path.  We cannot
        /// distinguish "written as 0" from "absent, filled with default", so we pass them
        /// through — see the documentation limitation for the consequence.
        std::vector<LeafRef> batch; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        const auto * typed_path_types = obj_type ? &obj_type->getTypedPaths() : nullptr;

        ColumnObject::SortedPathsIterator it(object_column, row_num);
        while (!it.end())
        {
            /// Skip Nullable typed paths that are null — null means the patch omitted this path.
            if (it.isCurrentTypedNull())
            {
                it.next();
                continue;
            }

            /// Serialize the value in Dynamic binary format directly from the column.
            /// This preserves the exact DataType (e.g. Date, Map, JSON) without going through
            /// Field, which loses type fidelity. All path types — TYPED, DYNAMIC, SHARED_DATA —
            /// are serialized as atomic leaves; typed Map/JSON paths are not flattened.
            WriteBufferFromOwnString val_buf;
            it.serializeCurrentValueBinary(typed_path_types, val_buf);
            batch.push_back({String(it.getCurrentPath()),
                EncodedField(EncodedField::Kind::DynamicBinary, std::move(val_buf.str())),
                sort_key});

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

    void merge(const AggregateFunctionMergedJSONPatchData & other)
    {
        std::vector<LeafRef> batch; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        batch.reserve(other.entries.size());
        for (const auto & entry : other.entries)
            batch.push_back({String(entry.pathView()), entry.value, entry.sort_key});
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
                case EncodedField::Kind::DynamicBinary:
                    writeStringBinary(entry.value.dataView(), buf);
                    break;
            }
            DB::encodeField(entry.sort_key.ptr->toField(), buf);
        }
    }

    void deserialize(ReadBuffer & buf)
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
            lv.value = readEncodedField(buf);
            lv.sort_key = SortKey{makeSortKey(decodeField(buf))};
        }

        insertBatchAtomic(batch);
    }

    void insertResultInto(IColumn & to, const DataTypePtr & /* result_type_ */) const
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

            if (auto typed_it = result_column.getTypedPaths().find(path); typed_it != result_column.getTypedPaths().end())
            {
                /// Typed columns accept a Field — the target column knows its declared type
                /// and stores the raw value regardless of the Field's coarse type tag.
                typed_it->second->insert(entry.value.get());
            }
            else if (entry.value.kind == EncodedField::Kind::DynamicBinary)
            {
                /// DynamicBinary stores encodeDataType + value bytes, exactly the format that
                /// ColumnDynamic and shared-data use internally. Deserialise directly into the
                /// target column to avoid FieldToDataType re-deriving the type (which would turn
                /// Date{18262} back into UInt16 when going through Field).
                ReadBufferFromString val_buf(entry.value.dataView());
                if (auto dynamic_it = result_column.getDynamicPathsPtrs().find(path);
                    dynamic_it != result_column.getDynamicPathsPtrs().end())
                {
                    DataTypeDynamic().getDefaultSerialization()->deserializeBinary(*dynamic_it->second, val_buf, {});
                }
                else if (auto * dynamic_path_column = result_column.tryToAddNewDynamicPath(path))
                {
                    DataTypeDynamic().getDefaultSerialization()->deserializeBinary(*dynamic_path_column, val_buf, {});
                }
                else
                {
                    /// Dynamic path limit reached: copy bytes directly to shared data.
                    /// The bytes are already in the correct format — no re-serialization needed.
                    auto type = decodeDataType(val_buf);
                    if (!isNothing(type))
                    {
                        shared_data_paths->insertData(path.data(), path.size());
                        auto & chars = shared_data_values->getChars();
                        /// Rewind and copy the full DynamicBinary blob (type tag + value).
                        std::string_view bytes = entry.value.dataView();
                        chars.insert(chars.end(), bytes.begin(), bytes.end());
                        shared_data_values->getOffsets().push_back(chars.size());
                    }
                }
            }
            else
            {
                Field value = entry.value.get();
                if (auto dynamic_it = result_column.getDynamicPathsPtrs().find(path);
                    dynamic_it != result_column.getDynamicPathsPtrs().end())
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
    /// Typed-path declarations from the input JSON type; null if the type has no typed paths.
    const DataTypeObject * obj_type;

public:
    explicit AggregateFunctionMergedJSONPatch(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData, AggregateFunctionMergedJSONPatch>(
            argument_types_, {}, argument_types_[0])
        , obj_type(typeid_cast<const DataTypeObject *>(argument_types_[0].get()))
    {
    }

    String getName() const override
    {
        return "mergedJSONPatch";
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).addWithKey(*columns[0], *columns[1], row_num, obj_type);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
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

    if (argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}. Expected 2 arguments (JSON value and sort key), got {} arguments",
            name, argument_types.size());

    if (!isObject(argument_types[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument for aggregate function {}. Expected type JSON",
            argument_types[0]->getName(), name);

    if (!argument_types[1]->isComparable())
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
descendant paths for paths that the `JSON` type stores as dynamic scalar leaves. Ancestor non-object
writes shadow conflicting descendants.

Every explicitly typed path (declared with `JSON(a Map(...))`, `JSON(a Tuple(...))`,
`JSON(a Variant(...))`, `JSON(a Dynamic)`, etc.) is treated as an atomic value: the whole path value
is replaced by the newer patch rather than deep-merged. Only untyped (dynamic) scalar paths follow
RFC 7396 deep-merge semantics.

The sort_key determines which value wins for each JSON path. The row with the largest sort_key is
retained. If two conflicting patches have equal sort keys, the result is order-dependent: the patch
processed later wins the tie. Users should not rely on `ORDER BY` to break ties deterministically.

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

4. All typed paths are atomic: every typed path (`Map(K,V)`, `JSON`, `Dynamic`, `Tuple(…)`,
    `Variant(…)`, `Array(…)`, or any other declared type) is stored as a single value. The
    aggregate replaces the entire value atomically rather than deep-merging its contents.
    Only dynamic (untyped, scalar) paths are deep-merged path-by-path.

5. Dot-in-key ambiguity: the `JSON` type represents `{"a":{"b":1}}` and `{"a.b":1}` with
    the same internal path `a.b`. A single row can therefore expose both `a` and `a.b` as
    independent peers. When a newer patch writes only `a`, the ancestor/descendant conflict
    rule erases `a.b`; when it writes only `a.b`, the same rule erases `a`. To avoid this,
    set `json_type_escape_dots_in_keys = 1`. With this setting, literal dots in JSON keys
    are percent-encoded (e.g. `a.b` becomes `a%2Eb`), making them distinct from nested
    paths and eliminating the false conflict.
)";

    FunctionDocumentation::Syntax syntax = "mergedJSONPatch(json, sort_key)";

    FunctionDocumentation::Arguments arguments = {
        {"json", "JSON column to aggregate.", {"JSON"}},
        {"sort_key", "Comparable column that determines which write wins for each path. "
                     "The row with the largest sort_key value is retained.", {}}
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

