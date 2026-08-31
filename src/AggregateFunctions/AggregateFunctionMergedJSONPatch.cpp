#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorToString.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Common/Arena.h>
#include <Common/FieldBinaryEncoding.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/CompareHelper.h>

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

/// Sort key types: concrete uniform structs, stored inline, no virtual dispatch.

/// Fixed-width numeric and date/time types: (U)Int*, Float*, Decimal*, Date, DateTime, DateTime64
template <typename ValueType>
struct KeyFixed
{
    ValueType value{};

    void set(const IColumn & column, size_t row)
    {
        value = assert_cast<const ColumnVectorOrDecimal<ValueType> &>(column).getData()[row];
    }

    bool less(const KeyFixed & other) const
    {
        // For floating-point types, use nan_direction_hint = -1 to treat NaN as the minimum value
        // (same as argMax / IColumn::compareAt semantics): finite sort keys always beat NaN.
        if constexpr (std::is_floating_point_v<ValueType>)
            return FloatCompareHelper<ValueType>::less(value, other.value, -1);
        else
            return value < other.value;
    }

    void serialize(WriteBuffer & buffer) const { writeBinaryLittleEndian(value, buffer); }

    void deserialize(ReadBuffer & buffer) { readBinaryLittleEndian(value, buffer); }
};

/// String sort keys: owns a String (avoids Field's wrapper)
struct KeyString
{
    String value;

    void set(const IColumn & column, size_t row)
    {
        value = column.getDataAt(row);
    }

    bool less(const KeyString & other) const { return value < other.value; }

    void serialize(WriteBuffer & buffer) const { writeStringBinary(value, buffer); }

    void deserialize(ReadBuffer & buffer) { readStringBinary(value, buffer); }
};

/// Generic fallback: UUID, IPv*, Decimal256, any other comparable scalar — owns a Field
struct KeyGeneric
{
    Field value;

    void set(const IColumn & column, size_t row) { column.get(row, value); }

    bool less(const KeyGeneric & other) const
    {
        // Field::operator< uses nan_direction_hint = 1 (NaN is maximum), but we need NaN to be
        // the minimum (same as argMax / IColumn::compareAt semantics) so finite keys always win.
        // Apply the NaN fix only when both sides hold a Float64 — if either side is Null (from
        // Nullable(Float*)) or any other type, fall through to Field::operator<, which already
        // orders Field::Null before all non-null values.
        if (value.getType() == Field::Types::Float64 && other.value.getType() == Field::Types::Float64)
            return FloatCompareHelper<Float64>::less(value.safeGet<Float64>(), other.value.safeGet<Float64>(), -1);
        return value < other.value;
    }

    void serialize(WriteBuffer & buffer) const { encodeField(value, buffer); }

    void deserialize(ReadBuffer & buffer) { value = decodeField(buffer); }
};

/// Value storage: a String binary blob
/// Encoding per path kind:
/// - Typed path: bare value via serialization (from typed_path_serializations map)
/// - Dynamic path: self-describing Dynamic binary (encodeDataType + serialized value)
/// - Shared-data path: Dynamic binary bytes copied verbatim

template <typename KeyData>
struct AggregateFunctionMergedJSONPatchData
{
    struct Entry
    {
        String path;
        String value_blob;
        KeyData sort_key;

        std::string_view pathView() const { return path; }
    };

    VectorWithMemoryTracking<Entry> entries;

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

    void pushLeafEntry(std::string_view path, String value_blob, const KeyData & sort_key)
    {
        Entry entry;
        entry.path = path;
        entry.value_blob = std::move(value_blob);
        entry.sort_key = sort_key;

        auto it = findInsertPosition(entries, path);
        entries.insert(it, std::move(entry));
    }

    /// Insert batch atomically: filter survivors, erase shadowed entries, push survivors.
    /// This avoids siblings within the batch erasing each other.
    void insertBatchAtomic(VectorWithMemoryTracking<Entry> & batch)
    {
        size_t existing_count = entries.size();

        VectorWithMemoryTracking<size_t> survivors;
        survivors.reserve(batch.size());
        for (size_t i = 0; i < batch.size(); ++i)
        {
            bool blocked = false;
            for (size_t j = 0; j < existing_count; ++j)
            {
                if (pathsConflict(entries[j].pathView(), batch[i].path) && batch[i].sort_key.less(entries[j].sort_key))
                {
                    blocked = true;
                    break;
                }
            }
            if (!blocked)
                survivors.push_back(i);
        }

        for (size_t idx : survivors)
        {
            entries.erase(
                std::remove_if(
                    entries.begin(),
                    entries.end(),
                    [&](const Entry & e)
                    {
                        return pathsConflict(e.pathView(), batch[idx].path)
                            && !batch[idx].sort_key.less(e.sort_key);
                    }),
                entries.end());
        }

        for (size_t idx : survivors)
            pushLeafEntry(batch[idx].path, std::move(batch[idx].value_blob), batch[idx].sort_key);
    }

    void merge(const AggregateFunctionMergedJSONPatchData & other)
    {
        VectorWithMemoryTracking<Entry> batch;
        batch.reserve(other.entries.size());
        for (const auto & entry : other.entries)
            batch.push_back({String(entry.pathView()), String(entry.value_blob), entry.sort_key});
        insertBatchAtomic(batch);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(entries.size(), buf);
        for (const auto & entry : entries)
        {
            writeStringBinary(entry.pathView(), buf);
            writeStringBinary(entry.value_blob, buf);
            entry.sort_key.serialize(buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        entries.clear();

        size_t size = 0;
        readVarUInt(size, buf);

        VectorWithMemoryTracking<Entry> batch;
        batch.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            Entry & lv = batch.emplace_back();
            readStringBinary(lv.path, buf);
            readStringBinary(lv.value_blob, buf);
            lv.sort_key.deserialize(buf);
        }

        insertBatchAtomic(batch);
    }

    void insertResultInto(
        IColumn & to,
        const UnorderedMapWithMemoryTracking<String, SerializationPtr> & typed_path_serializations) const
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
                ReadBufferFromString val_buf(entry.value_blob);
                auto ser_it = typed_path_serializations.find(String(path));
                if (ser_it != typed_path_serializations.end())
                    ser_it->second->deserializeBinary(*typed_it->second, val_buf, {});
            }
            else
            {
                ReadBufferFromString val_buf(entry.value_blob);
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
                    auto type = decodeDataType(val_buf);
                    if (!isNothing(type))
                    {
                        shared_data_paths->insertData(path.data(), path.size());
                        auto & chars = shared_data_values->getChars();
                        chars.insert(chars.end(), entry.value_blob.begin(), entry.value_blob.end());
                        shared_data_values->getOffsets().push_back(chars.size());
                    }
                }
            }
        }

        result_column.getSharedDataOffsets().push_back(shared_data_paths->size());

        for (auto & [_, column] : result_column.getTypedPaths())
            if (column->size() == current_size)
                column->insertDefault();

        for (auto & [_, column] : result_column.getDynamicPathsPtrs())
            if (column->size() == current_size)
                column->insertDefault();
    }
};

/// Template specialization for each KeyData type
template <typename KeyData>
class AggregateFunctionMergedJSONPatchImpl final
    : public IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData<KeyData>, AggregateFunctionMergedJSONPatchImpl<KeyData>>
{
private:
    UnorderedMapWithMemoryTracking<String, SerializationPtr> typed_path_serializations;

    using Data = AggregateFunctionMergedJSONPatchData<KeyData>;

public:
    explicit AggregateFunctionMergedJSONPatchImpl(
        const DataTypes & argument_types_,
        const UnorderedMapWithMemoryTracking<String, SerializationPtr> & typed_path_serializations_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionMergedJSONPatchImpl<KeyData>>(
            argument_types_, {}, argument_types_[0])
        , typed_path_serializations(typed_path_serializations_)
    {
    }

    String getName() const override { return "mergedJSONPatch"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data = this->data(place);
        KeyData sort_key;
        sort_key.set(*columns[1], row_num);

        const auto & object_column = assert_cast<const ColumnObject &>(*columns[0]);
        VectorWithMemoryTracking<typename Data::Entry> batch;

        ColumnObject::SortedPathsIterator iterator(object_column, row_num, /*skip_typed_nulls=*/true);
        for (; !iterator.end(); iterator.next())
        {
            WriteBufferFromOwnString value_buffer;
            iterator.serializeCurrentValueBinary(typed_path_serializations, value_buffer);
            batch.push_back({String(iterator.getCurrentPath()), std::move(value_buffer.str()), sort_key});
        }

        data.insertBatchAtomic(batch);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to, typed_path_serializations);
    }
};

/// Factory dispatch: pick KeyData type from argument_types[1]
/// Mirrors createWithTwoTypesSecond (argMax shape); also builds typed_path_serializations map (§3, §5)
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

    // Build typed-path serializations map from JSON type (§3, §5)
    // Resolved once at construction; used in add() and insertResultInto()
    UnorderedMapWithMemoryTracking<String, SerializationPtr> typed_path_serializations;
    {
        const auto * obj_type = typeid_cast<const DataTypeObject *>(argument_types[0].get());
        if (obj_type)
            typed_path_serializations = obj_type->getTypedPathSerializations();
    }

    // Validate and dispatch sort-key type
    // Mirror createWithTwoTypesSecond: numeric types, Date/DateTime with FieldType,
    // String, or KeyGeneric for other comparable scalars.
    // NOTE: Nullable(scalar) must always dispatch to KeyGeneric because KeyFixed and
    // KeyString have no null slot — only Field holds Field::Null to preserve sorting semantics
    // (null sorts before any non-null value).
    const IDataType * key_type = argument_types[1].get();
    bool is_nullable = false;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(key_type))
    {
        is_nullable = true;
        key_type = nullable->getNestedType().get();
    }

    WhichDataType which_key(key_type->getTypeId());

    // Reject composite and non-comparable types
    if (which_key.isVariant() || which_key.isDynamic() || which_key.isTuple() || which_key.isArray() || which_key.isMap())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of second argument for aggregate function {}. "
            "Composite and Variant/Dynamic sort key types are not supported. "
            "Use a scalar sort key (e.g. a numeric version or timestamp column).",
            argument_types[1]->getName(), name);

    if (!argument_types[1]->isComparable())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of second argument for aggregate function {}. Expected comparable type for sort key",
            argument_types[1]->getName(), name);

    // Dispatch to appropriate KeyData type (§5)
    // If type is Nullable, dispatch to KeyGeneric (Field preserves null semantics)
    if (is_nullable)
        return std::make_shared<AggregateFunctionMergedJSONPatchImpl<KeyGeneric>>(argument_types, typed_path_serializations);

    // Templated helper to dispatch fixed-width key types
    auto dispatchFixedType = [&]<typename T>()
    {
        return std::make_shared<AggregateFunctionMergedJSONPatchImpl<KeyFixed<T>>>(argument_types, typed_path_serializations);
    };

    if (which_key.idx == TypeIndex::UInt8) return dispatchFixedType.operator()<UInt8>();
    if (which_key.idx == TypeIndex::UInt16) return dispatchFixedType.operator()<UInt16>();
    if (which_key.idx == TypeIndex::UInt32) return dispatchFixedType.operator()<UInt32>();
    if (which_key.idx == TypeIndex::UInt64) return dispatchFixedType.operator()<UInt64>();
    if (which_key.idx == TypeIndex::UInt128) return dispatchFixedType.operator()<UInt128>();
    if (which_key.idx == TypeIndex::UInt256) return dispatchFixedType.operator()<UInt256>();
    if (which_key.idx == TypeIndex::Int8) return dispatchFixedType.operator()<Int8>();
    if (which_key.idx == TypeIndex::Int16) return dispatchFixedType.operator()<Int16>();
    if (which_key.idx == TypeIndex::Int32) return dispatchFixedType.operator()<Int32>();
    if (which_key.idx == TypeIndex::Int64) return dispatchFixedType.operator()<Int64>();
    if (which_key.idx == TypeIndex::Int128) return dispatchFixedType.operator()<Int128>();
    if (which_key.idx == TypeIndex::Int256) return dispatchFixedType.operator()<Int256>();
    if (which_key.idx == TypeIndex::Float32) return dispatchFixedType.operator()<Float32>();
    if (which_key.idx == TypeIndex::Float64) return dispatchFixedType.operator()<Float64>();
    if (which_key.idx == TypeIndex::Decimal32) return dispatchFixedType.operator()<Decimal32>();
    if (which_key.idx == TypeIndex::Decimal64) return dispatchFixedType.operator()<Decimal64>();
    if (which_key.idx == TypeIndex::Decimal128) return dispatchFixedType.operator()<Decimal128>();
    if (which_key.idx == TypeIndex::Decimal256) return dispatchFixedType.operator()<Decimal256>();
    if (which_key.idx == TypeIndex::Date) return dispatchFixedType.operator()<UInt16>();
    if (which_key.idx == TypeIndex::Date32) return dispatchFixedType.operator()<Int32>();
    if (which_key.idx == TypeIndex::DateTime) return dispatchFixedType.operator()<UInt32>();
    if (which_key.idx == TypeIndex::DateTime64) return dispatchFixedType.operator()<DateTime64>();

    if (which_key.idx == TypeIndex::String)
        return std::make_shared<AggregateFunctionMergedJSONPatchImpl<KeyString>>(argument_types, typed_path_serializations);

    // Fallback: UUID, IPv*, Decimal256, any other comparable scalar
    return std::make_shared<AggregateFunctionMergedJSONPatchImpl<KeyGeneric>>(argument_types, typed_path_serializations);
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
│ {"a":3,"b":2,"c":4}             │
└─────────────────────────────────┘
            )"
        }
    };

    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
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
