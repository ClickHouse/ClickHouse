#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorToString.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnObject.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>
#include <Common/Arena.h>
#include <Common/FieldBinaryEncoding.h>
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
  * It also inherits one important `ColumnObject` limitation: null-valued object members are dropped
  * on insertion. As a result, RFC 7396 null deletion semantics such as `{"key": null}` are not
  * representable here, because `ColumnObject` cannot distinguish between "key is absent" and
  * "key has null value".
  */
struct AggregateFunctionMergedJSONPatchData
{
    /// `JSON` / `ColumnObject` cannot insert arrays that mix scalar elements with nested `JSON`
    /// object elements, because array element type inference ends up with incompatible `String`
    /// and `JSON` types. `mergedJSONPatch` follows RFC 7396 and keeps arrays as atomic replacement
    /// values, so a heterogeneous source array can survive unchanged until aggregate finalization.
    ///
    /// To avoid `NO_COMMON_TYPE` during final insertion of the aggregate result, recursively detect
    /// arrays that contain both object and non-object elements and stringify only the object
    /// elements. This preserves RFC 7396 replacement semantics for the array as a whole while
    /// converting it to a representation that the current `JSON` type can store.
    static void normalizeMixedJSONArray(Field & value)
    {
        if (value.getType() != Field::Types::Array)
            return;

        auto & array = value.safeGet<Array>();
        bool has_object = false;
        bool has_non_object = false;

        for (auto & element : array)
        {
            normalizeMixedJSONArray(element);

            if (element.getType() == Field::Types::Object)
                has_object = true;
            else
                has_non_object = true;
        }

        if (has_object && has_non_object)
        {
            for (auto & element : array)
            {
                if (element.getType() == Field::Types::Object)
                    element = Field(convertObjectToString(element.safeGet<Object>()));
            }
        }
    }

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

    struct StringSlice
    {
        const char * data = nullptr;
        size_t size = 0;

        StringSlice() = default;

        StringSlice(const char * data_, size_t size_)
            : data(data_), size(size_)
        {
        }

        std::string_view view() const
        {
            return std::string_view(data, size);
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
        StringSlice data;

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

        EncodedField(Kind kind_, StringSlice data_)
            : kind(kind_)
            , data(data_)
        {
        }

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
                    return Field(String(data.view()));
                case Kind::BinaryNonObjectField:
                {
                    ReadBufferFromString buf(data.view());
                    return decodeField(buf);
                }
            }

            UNREACHABLE();
        }
    };

    struct Entry
    {
        StringSlice path;
        EncodedField value;
        SortKey sort_key;
    };

    Arena path_arena;
    Arena value_arena;
    std::vector<Entry> entries;

    static StringSlice copyToArena(Arena & arena, std::string_view data)
    {
        if (data.empty())
            return {};

        char * dst = arena.alloc(data.size());
        memcpy(dst, data.data(), data.size());
        return StringSlice(dst, data.size());
    }

    StringSlice copyPath(std::string_view path)
    {
        return copyToArena(path_arena, path);
    }

    EncodedField encodeFieldToArena(Field value)
    {
        switch (value.getType())
        {
            case Field::Types::Int64:
                return EncodedField(value.safeGet<Int64>());
            case Field::Types::UInt64:
                return EncodedField(value.safeGet<UInt64>());
            case Field::Types::String:
                return EncodedField(EncodedField::Kind::String, copyToArena(value_arena, value.safeGet<String>()));
            default:
            {
                WriteBufferFromOwnString buf;
                encodeField(value, buf);
                return EncodedField(EncodedField::Kind::BinaryNonObjectField, copyToArena(value_arena, buf.str()));
            }
        }
    }

    EncodedField cloneEncodedField(const EncodedField & value)
    {
        switch (value.kind)
        {
            case EncodedField::Kind::Empty:
                return EncodedField();
            case EncodedField::Kind::Int64:
                return EncodedField(value.inline_int64);
            case EncodedField::Kind::UInt64:
                return EncodedField(value.inline_uint64);
            case EncodedField::Kind::String:
            case EncodedField::Kind::BinaryNonObjectField:
                return EncodedField(value.kind, copyToArena(value_arena, value.data.view()));
        }

        UNREACHABLE();
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

    static auto findInsertPosition(std::vector<Entry> & entries, std::string_view path)
    {
        return std::lower_bound(
            entries.begin(),
            entries.end(),
            path,
            [](const Entry & entry, std::string_view rhs_path)
            {
                return entry.path.view() < rhs_path;
            });
    }

    static void insertNestedPath(Object & root, std::string_view path, Field value)
    {
        size_t dot = path.find('.');
        if (dot == std::string_view::npos)
        {
            root[String(path)] = std::move(value);
            return;
        }

        String head(path.substr(0, dot));
        std::string_view tail = path.substr(dot + 1);

        auto it = root.find(head);
        if (it == root.end() || it->second.getType() != Field::Types::Object)
            it = root.emplace(head, Field(Object{})).first;

        insertNestedPath(it->second.safeGet<Object>(), tail, std::move(value));
    }

    bool hasNewerConflictingEntry(std::string_view path, const SortKey & sort_key) const
    {
        for (const auto & entry : entries)
        {
            if (pathsConflict(entry.path.view(), path) && entry.sort_key > sort_key)
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
                    return pathsConflict(entry.path.view(), path) && entry.sort_key <= sort_key;
                }),
            entries.end());
    }

    void insertLeafEntry(std::string_view path, Field value, const SortKey & sort_key)
    {
        if (hasNewerConflictingEntry(path, sort_key))
            return;

        eraseShadowedEntries(path, sort_key);

        Entry entry;
        entry.path = copyPath(path);
        entry.value = encodeFieldToArena(std::move(value));
        entry.sort_key = sort_key;

        auto it = findInsertPosition(entries, path);
        entries.insert(it, std::move(entry));
    }

    void insertPathValue(std::string_view path, Field value, const SortKey & sort_key)
    {
        normalizeMixedJSONArray(value);

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

    static EncodedField readEncodedField(ReadBuffer & buf, AggregateFunctionMergedJSONPatchData & owner)
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
                size_t value_size = 0;
                readVarUInt(value_size, buf);

                StringSlice stored = {};
                if (value_size)
                {
                    char * dst = owner.value_arena.alloc(value_size);
                    buf.readStrict(dst, value_size);
                    stored = StringSlice(dst, value_size);
                }

                return EncodedField(kind, stored);
            }
        }

        UNREACHABLE();
    }

    void add(const IColumn & json_column, size_t row_num, Arena * arena)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);

        const char * begin = nullptr;
        auto serialized = object_column.serializeValueIntoArena(row_num, *arena, begin, nullptr);
        SortKey sort_key = SortKey(Field(String(serialized)));

        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void addWithKey(const IColumn & json_column, const IColumn & key_column, size_t row_num, Arena *)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        SortKey sort_key = SortKey(key_column[row_num]);
        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void addKeyValuePairs(const ColumnObject & object_column, size_t row_num, const SortKey & sort_key)
    {
        ColumnObject::SortedPathsIterator it(object_column, row_num);
        while (!it.end())
        {
            auto path_info = it.getCurrentPathInfo();

            Field value;
            path_info.column->get(path_info.row, value);
            normalizeMixedJSONArray(value);

            insertPathValue(path_info.path, std::move(value), sort_key);
            it.next();
        }
    }

    void merge(const AggregateFunctionMergedJSONPatchData & other, Arena *)
    {
        for (const auto & entry : other.entries)
            insertPathValue(entry.path.view(), entry.value.get(), entry.sort_key);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(entries.size(), buf);
        for (const auto & entry : entries)
        {
            writeStringBinary(entry.path.view(), buf);
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
                    writeStringBinary(entry.value.data.view(), buf);
                    break;
            }
            encodeField(entry.sort_key.toField(), buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        entries.clear();

        size_t size = 0;
        readVarUInt(size, buf);

        String path;
        for (size_t i = 0; i < size; ++i)
        {
            path.clear();
            readStringBinary(path, buf);
            EncodedField value = readEncodedField(buf, *this);
            SortKey sort_key = SortKey(decodeField(buf));
            insertPathValue(path, value.get(), sort_key);
        }
    }

    void insertResultInto(IColumn & to, const DataTypePtr &) const
    {
        auto & result_column = assert_cast<ColumnObject &>(to);

        Object result_object;
        for (const auto & entry : entries)
            insertNestedPath(result_object, entry.path.view(), entry.value.get());

        if (result_object.empty())
        {
            result_column.insertDefault();
            return;
        }

        result_column.insert(Field(result_object));
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
        return true;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (has_sort_key)
            data(place).addWithKey(*columns[0], *columns[1], row_num, arena);
        else
            data(place).add(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).deserialize(buf, arena);
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
        .is_order_dependent = false
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

LIMITATION: RFC 7396 null deletion semantics (where `{"key": null}` removes a key) are not supported.
ColumnObject silently drops null-valued keys during insertion, making it impossible to distinguish
between absent keys and keys with null values.
)";

    FunctionDocumentation::Syntax syntax = "mergedJSONPatch(json)";

    FunctionDocumentation::Arguments arguments = {
        {"json", "JSON column to aggregate.", {"JSON"}}
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

    FunctionDocumentation::IntroducedIn introduced_in = {25, 1};
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

