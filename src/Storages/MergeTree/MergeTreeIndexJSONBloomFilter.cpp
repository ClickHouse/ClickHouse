#include <Storages/MergeTree/MergeTreeIndexJSONBloomFilter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesCache.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BloomFilterHash.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/re2.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/transformEndianness.h>

#include <array>
#include <iterator>
#include <list>
#include <map>
#include <numeric>
#include <ranges>
#include <xxhash.h>

/// With XXH_INLINE_ALL (from contrib/xxHash) every XXH function is marked as unused,
/// so any actual use triggers this warning.
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_READ_ALL_DATA;
extern const int CORRUPTED_DATA;
extern const int INCORRECT_DATA;
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int LOGICAL_ERROR;
}

struct JSONBloomFilterDynamicProbe
{
    String path;
    UInt8 role;
    Field value;
    DataTypePtr value_type;
    DataTypePtr cast_type;
};

class JSONBloomPathMatcher
{
public:
    JSONBloomPathMatcher(
        std::vector<String> include_paths_,
        const std::vector<String> & include_path_regexps_,
        std::vector<String> skip_paths_,
        const std::vector<String> & skip_path_regexps_);

    bool shouldIndex(std::string_view path) const;
    bool shouldVisit(std::string_view path) const;
    const std::vector<String> & getIncludePaths() const { return include_paths; }
    const std::vector<String> & getIncludePathRegexps() const { return include_path_regexps; }
    const std::vector<String> & getSkipPaths() const { return skip_paths; }
    const std::vector<String> & getSkipPathRegexps() const { return skip_path_regexps; }

private:
    using Regexps = std::list<re2::RE2>;

    bool hasIncludeFilter() const { return !include_paths.empty() || !include_regexps.empty(); }
    static std::vector<String> normalizePaths(std::vector<String> paths);
    static std::vector<String> normalizeStrings(std::vector<String> values);
    static bool matchesAnyPathOrSubtree(std::string_view path, const std::vector<String> & paths);
    static bool matchesAnyAncestor(std::string_view path, const std::vector<String> & paths);
    static bool matchesAnyRegexp(std::string_view path, const Regexps & regexps);
    static void compileRegexps(const std::vector<String> & regexp_strings, Regexps & regexps);

    std::vector<String> include_paths;
    std::vector<String> include_path_regexps;
    std::vector<String> skip_paths;
    std::vector<String> skip_path_regexps;
    Regexps include_regexps;
    Regexps skip_regexps;
};

JSONBloomPathMatcher::JSONBloomPathMatcher(
    std::vector<String> include_paths_,
    const std::vector<String> & include_path_regexps_,
    std::vector<String> skip_paths_,
    const std::vector<String> & skip_path_regexps_)
    : include_paths(normalizePaths(std::move(include_paths_)))
    , include_path_regexps(normalizeStrings(include_path_regexps_))
    , skip_paths(normalizePaths(std::move(skip_paths_)))
    , skip_path_regexps(normalizeStrings(skip_path_regexps_))
{
    compileRegexps(include_path_regexps, include_regexps);
    compileRegexps(skip_path_regexps, skip_regexps);
}

std::vector<String> JSONBloomPathMatcher::normalizePaths(std::vector<String> paths)
{
    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());

    std::vector<String> result;
    result.reserve(paths.size());
    for (auto & path : paths)
    {
        if (!matchesAnyPathOrSubtree(path, result))
            result.emplace_back(std::move(path));
    }
    return result;
}

std::vector<String> JSONBloomPathMatcher::normalizeStrings(std::vector<String> values)
{
    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
    return values;
}

bool JSONBloomPathMatcher::matchesAnyPathOrSubtree(std::string_view path, const std::vector<String> & paths)
{
    while (true)
    {
        const auto it = std::lower_bound(
            paths.begin(), paths.end(), path, [](const String & lhs, std::string_view rhs) { return lhs.compare(rhs) < 0; });
        if (it != paths.end() && *it == path)
            return true;

        const auto separator = path.find_last_of(".[");
        if (separator == std::string_view::npos)
            return false;
        path = path.substr(0, separator);
    }
}

bool JSONBloomPathMatcher::matchesAnyAncestor(std::string_view path, const std::vector<String> & paths)
{
    String descendant_prefix(path);
    descendant_prefix += '.';
    auto it = std::lower_bound(paths.begin(), paths.end(), descendant_prefix);
    if (it != paths.end() && it->starts_with(descendant_prefix))
        return true;

    descendant_prefix.back() = '[';
    it = std::lower_bound(paths.begin(), paths.end(), descendant_prefix);
    return it != paths.end() && it->starts_with(descendant_prefix);
}

bool JSONBloomPathMatcher::matchesAnyRegexp(std::string_view path, const Regexps & regexps)
{
    return std::ranges::any_of(regexps, [&](const auto & regexp) { return re2::RE2::PartialMatch(path, regexp); });
}

void JSONBloomPathMatcher::compileRegexps(const std::vector<String> & regexp_strings, Regexps & regexps)
{
    auto unique_regexp_strings = regexp_strings;
    std::sort(unique_regexp_strings.begin(), unique_regexp_strings.end());
    unique_regexp_strings.erase(std::unique(unique_regexp_strings.begin(), unique_regexp_strings.end()), unique_regexp_strings.end());
    for (const auto & regexp_string : unique_regexp_strings)
    {
        regexps.emplace_back(regexp_string);
        if (!regexps.back().ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid `jsonbf_v1` path regexp '{}': {}", regexp_string, regexps.back().error());
    }
}

bool JSONBloomPathMatcher::shouldIndex(std::string_view path) const
{
    path = path.substr(0, path.find('\0'));
    if (matchesAnyPathOrSubtree(path, skip_paths) || matchesAnyRegexp(path, skip_regexps))
        return false;

    return !hasIncludeFilter() || matchesAnyPathOrSubtree(path, include_paths) || matchesAnyRegexp(path, include_regexps);
}

bool JSONBloomPathMatcher::shouldVisit(std::string_view path) const
{
    path = path.substr(0, path.find('\0'));
    if (matchesAnyPathOrSubtree(path, skip_paths))
        return false;

    /// An arbitrary include or skip regexp can match a descendant differently from its ancestor.
    return !hasIncludeFilter() || matchesAnyPathOrSubtree(path, include_paths) || matchesAnyAncestor(path, include_paths)
        || !include_regexps.empty();
}

/// Tokens of one index granule. Each token is keyed by its path id, so one table deduplicates the tokens of all paths
/// and a path costs only an id and its interned name. JSON with many distinct paths per granule would otherwise pay
/// for separate hash tables per path on every granule.
struct JSONBloomFilterTokens
{
    /// Why a path is present without value tokens. The reader recomputes the presence hashes from these records.
    enum class PresenceKind : UInt8
    {
        RuntimeType = 0,
        Complex = 1,
        Unsupported = 2,
    };

    /// Token hash in the low half, path id in the high half.
    HashSet<UInt128, UInt128TrivialHash> values;
    /// Path id and scope path id in the low half, role, kind, and runtime type id in the high half.
    HashSet<UInt128, UInt128Hash> presence;
    HashMap<std::string_view, UInt32> path_ids;
    HashMap<std::string_view, UInt32> type_ids;
    Arena arena;
    std::vector<std::string_view> paths;
    std::vector<std::string_view> types;

    static UInt32 intern(std::string_view value, HashMap<std::string_view, UInt32> & ids, std::vector<std::string_view> & values, Arena & arena)
    {
        HashMap<std::string_view, UInt32>::LookupResult it = nullptr;
        bool inserted = false;
        ids.emplace(ArenaKeyHolder{value, arena}, it, inserted);
        if (inserted)
        {
            it->getMapped() = static_cast<UInt32>(values.size());
            values.push_back(it->getKey());
        }
        return it->getMapped();
    }

    UInt32 getPathId(std::string_view path) { return intern(path, path_ids, paths, arena); }

    void addValue(UInt32 path_id, UInt64 hash) { values.insert(UInt128(hash) | (UInt128(path_id) << 64)); }

    /// `scope_path` is the path the presence hashes are computed with, which differs from the logical path below
    /// `Map` keys. `encoded_type` is set for `PresenceKind::RuntimeType`.
    void addPresence(UInt32 path_id, std::string_view scope_path, UInt8 role, PresenceKind kind, std::string_view encoded_type = {})
    {
        const UInt64 type_id = kind == PresenceKind::RuntimeType ? intern(encoded_type, type_ids, types, arena) : 0;
        presence.insert(
            UInt128(UInt64(path_id) | (UInt64(getPathId(scope_path)) << 32))
            | (UInt128(UInt64(role) | (UInt64(kind) << 8) | (type_id << 16)) << 64));
    }
};

namespace
{

struct DecodedJSONDataType
{
    DataTypePtr type;
    SerializationPtr serialization;
    std::string_view name;
};

DecodedJSONDataType
decodeJSONDataType(ReadBuffer & buffer, UnorderedMapWithMemoryTracking<String, SerializationPtr> & serializations_cache)
{
    char type_index = 0;
    if (!buffer.peek(type_index))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse binary JSON value: no type index found");

    const auto binary_type_index = static_cast<BinaryTypeIndex>(type_index);
    const auto & simple_types_cache = getSimpleDataTypesCache();
    if (simple_types_cache.hasElement(binary_type_index))
    {
        ++buffer.position();
        const auto & element = simple_types_cache.getElement(binary_type_index);
        return {element.type, element.serialization, element.name};
    }

    auto type = decodeDataType(buffer);
    auto [it, inserted] = serializations_cache.try_emplace(type->getName());
    if (inserted)
        it->second = type->getDefaultSerialization();
    return {std::move(type), it->second, it->first};
}

enum class JSONBloomRole : UInt8
{
    Scalar = 1,
    ArrayElement = 2,
    MapValue = 3,
};

enum class JSONBloomDomain : UInt8
{
    /// Append only: values are persisted inside the token hashes of `jsonbf_v1` index granules.
    Typed = 1,
    UnsupportedDynamicType = 3,
    DynamicTypePresence = 4,
    DynamicComplexPresence = 5,
};

void updateTokenHash(XXH3_state_t & hash, std::string_view value)
{
    UInt64 size = value.size();
    transformEndianness<std::endian::little>(size);
    XXH_INLINE_XXH3_64bits_update(&hash, &size, sizeof(size));
    XXH_INLINE_XXH3_64bits_update(&hash, value.data(), value.size());
}

bool usesNumericToken(std::string_view type)
{
    return type == "Bool" || type == "Int8" || type == "Int16" || type == "Int32" || type == "Int64"
        || type == "UInt8" || type == "UInt16" || type == "UInt32" || type == "UInt64" || type == "Float32" || type == "Float64";
}

UInt64 hashToken(std::string_view path, JSONBloomRole role, JSONBloomDomain domain, std::string_view type, std::string_view value)
{
    /// Native numeric equality uses the existing `Int64` token layout.
    if (domain == JSONBloomDomain::Typed && usesNumericToken(type))
        type = "Int64";
    XXH3_state_t hash;
    XXH_INLINE_XXH3_64bits_reset(&hash);
    static constexpr std::string_view namespace_name = "jsonbf_v1";
    updateTokenHash(hash, namespace_name);
    XXH_INLINE_XXH3_64bits_update(&hash, &role, sizeof(role));
    XXH_INLINE_XXH3_64bits_update(&hash, &domain, sizeof(domain));
    updateTokenHash(hash, path);
    updateTokenHash(hash, type);
    updateTokenHash(hash, value);
    return XXH_INLINE_XXH3_64bits_digest(&hash);
}

UInt64 unsupportedDynamicTypeHash(std::string_view path, JSONBloomRole role)
{
    return hashToken(path, role, JSONBloomDomain::UnsupportedDynamicType, {}, {});
}

UInt64 dynamicTypePresenceHash(std::string_view path, JSONBloomRole role, std::string_view type_name)
{
    return hashToken(path, role, JSONBloomDomain::DynamicTypePresence, type_name, {});
}

UInt64 dynamicComplexPresenceHash(std::string_view path, JSONBloomRole role)
{
    return hashToken(path, role, JSONBloomDomain::DynamicComplexPresence, {}, {});
}

DataTypePtr removeJSONBloomWrappers(DataTypePtr type)
{
    while (true)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            type = nullable->getNestedType();
            continue;
        }
        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            type = low_cardinality->getDictionaryType();
            continue;
        }
        return type;
    }
}

bool hasJSONPathDescendants(DataTypePtr type)
{
    type = removeJSONBloomWrappers(std::move(type));
    if (typeid_cast<const DataTypeObject *>(type.get()) || typeid_cast<const DataTypeTuple *>(type.get()))
        return true;
    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
        return hasJSONPathDescendants(array->getNestedType());
    if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
        return hasJSONPathDescendants(map->getValueType());
    return false;
}

struct UnwrappedColumn
{
    DataTypePtr type;
    const IColumn * column;
    ColumnPtr owned_column;
};

std::optional<UnwrappedColumn> unwrapColumn(DataTypePtr type, const IColumn & source, size_t row)
{
    const IColumn * column = &source;
    ColumnPtr owned_column;

    while (true)
    {
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            const auto * nullable_column = typeid_cast<const ColumnNullable *>(column);
            if (!nullable_column || nullable_column->isNullAt(row))
                return std::nullopt;
            type = nullable_type->getNestedType();
            column = &nullable_column->getNestedColumn();
            continue;
        }

        if (typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            owned_column = column->convertToFullColumnIfLowCardinality();
            column = owned_column.get();
            type = removeLowCardinality(type);
            continue;
        }

        return UnwrappedColumn{std::move(type), column, std::move(owned_column)};
    }
}

bool canHashRawValue(const IDataType & type)
{
    const WhichDataType which(type);
    if constexpr (std::endian::native == std::endian::little)
        return which.isStringOrFixedString() || which.isNativeNumber();
    else
        return which.isStringOrFixedString();
}

template <typename T>
UInt64 hashNumericValue(UInt64 seed, T number)
{
    UInt64 bits = 0;
    if constexpr (std::is_signed_v<T> && std::is_integral_v<T>)
        bits = static_cast<UInt64>(number);
    else if constexpr (std::is_unsigned_v<T>)
    {
        bits = number;
        /// Separate wide positive values from negative `Int64` values with the same bits.
        if (bits >= (UInt64(1) << 63))
            seed ^= 0x9e3779b97f4a7c15ULL;
    }
    else
    {
        const Float64 magnitude = std::abs(number);
        if (magnitude < 0x1p64 && static_cast<Float64>(static_cast<UInt64>(magnitude)) == magnitude)
        {
            bits = static_cast<UInt64>(magnitude);
            const bool negative = number < 0;
            /// Integral values use the `Int64` representation; a separate domain covers values
            /// outside its range without rounding large integers through `Float64`.
            if (negative ? bits > (UInt64(1) << 63) : bits >= (UInt64(1) << 63))
                seed ^= 0x9e3779b97f4a7c15ULL;
            if (negative)
                bits = 0 - bits;
        }
        else
        {
            bits = std::bit_cast<UInt64>(number);
            seed ^= 0xd1b54a32d192ed03ULL;
        }
    }
    transformEndianness<std::endian::little>(bits);
    return XXH_INLINE_XXH3_64bits_withSeed(&bits, sizeof(bits), seed);
}

UInt64 hashTypedValue(
    UInt64 seed,
    const ISerialization & serialization,
    WhichDataType which,
    bool raw_value,
    const IColumn & column,
    size_t row,
    WriteBufferFromOwnString & value,
    const FormatSettings & format_settings)
{
    if (which.isNativeInt())
        return hashNumericValue(seed, column.getInt(row));
    if (which.isNativeUInt())
        return hashNumericValue(seed, column.getUInt(row));
    if (which.isNativeFloat())
        return hashNumericValue(seed, column.getFloat64(row));
    if (which.isFloat() && column.getFloat64(row) == 0)
    {
        const UInt64 zero = 0;
        return XXH_INLINE_XXH3_64bits_withSeed(&zero, column.getDataAt(row).size(), seed);
    }
    if (raw_value)
    {
        const auto data = column.getDataAt(row);
        return XXH_INLINE_XXH3_64bits_withSeed(data.data(), data.size(), seed);
    }
    value.restart();
    serialization.serializeBinary(column, row, value, format_settings);
    const auto data = value.stringView();
    return XXH_INLINE_XXH3_64bits_withSeed(data.data(), data.size(), seed);
}

/// Read the same scalar payload as its serialization, without materializing a column.
UInt64 hashSharedScalar(UInt64 seed, const IDataType & type, ReadBufferFromMemory & buffer, const FormatSettings & settings)
{
    auto read_number = [&]<typename T>()
    {
        T number;
        readBinaryLittleEndian(number, buffer);
        if constexpr (std::is_floating_point_v<T>)
            return hashNumericValue(seed, static_cast<Float64>(number));
        else
            return hashNumericValue(seed, number);
    };

    switch (type.getTypeId())
    {
        case TypeIndex::Int8: return read_number.template operator()<Int8>();
        case TypeIndex::Int16: return read_number.template operator()<Int16>();
        case TypeIndex::Int32: return read_number.template operator()<Int32>();
        case TypeIndex::Int64: return read_number.template operator()<Int64>();
        case TypeIndex::UInt8: return read_number.template operator()<UInt8>();
        case TypeIndex::UInt16: return read_number.template operator()<UInt16>();
        case TypeIndex::UInt32: return read_number.template operator()<UInt32>();
        case TypeIndex::UInt64: return read_number.template operator()<UInt64>();
        case TypeIndex::Float32: return read_number.template operator()<Float32>();
        case TypeIndex::Float64: return read_number.template operator()<Float64>();
        default: break;
    }

    UInt64 size = 0;
    if (WhichDataType(type).isString())
    {
        readVarUInt(size, buffer);
        SerializationString::checkStringSize(size, settings);
    }
    else
        size = assert_cast<const DataTypeFixedString &>(type).getN();

    /// Match `readStrict`, including its exception for truncated payloads. Trailing bytes are ignored.
    if (size > buffer.available())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data. Bytes read: {}. Bytes expected: {}.", buffer.available(), std::to_string(size));
    const char * data = buffer.position();
    buffer.position() += size;
    return XXH_INLINE_XXH3_64bits_withSeed(data, size, seed);
}

String appendPath(std::string_view prefix, std::string_view suffix)
{
    if (prefix.empty())
        return String(suffix);
    if (suffix.empty())
        return String(prefix);
    return Nested::concatenateName(String(prefix), String(suffix));
}

std::string_view appendMapKey(
    std::string_view path,
    const ISerialization & serialization,
    std::string_view type_name,
    const IColumn & key_column,
    size_t row,
    WriteBufferFromOwnString & encoded_key,
    WriteBufferFromOwnString & result,
    const FormatSettings & format_settings)
{
    encoded_key.restart();
    serialization.serializeBinary(key_column, row, encoded_key, format_settings);

    result.restart();
    writeString(path, result);
    writeChar('\0', result);
    writeChar('M', result);
    writeBinaryLittleEndian(static_cast<UInt64>(type_name.size()), result);
    writeString(type_name, result);
    const auto key = encoded_key.stringView();
    writeBinaryLittleEndian(static_cast<UInt64>(key.size()), result);
    writeString(key, result);
    return result.stringView();
}

String appendMapKey(std::string_view path, const DataTypePtr & key_type, const IColumn & key_column, size_t row)
{
    WriteBufferFromOwnString encoded_key;
    WriteBufferFromOwnString result;
    return String(appendMapKey(path, *key_type->getDefaultSerialization(), key_type->getName(), key_column, row, encoded_key, result, {}));
}

class JSONBloomExtractor
{
public:
    JSONBloomExtractor(JSONBloomFilterTokens & tokens_, const JSONBloomPathMatcher & path_matcher_)
        : tokens(tokens_)
        , path_matcher(path_matcher_)
    {
    }

    void emitObject(const ColumnObject & column, const DataTypeObject & type, size_t start_row, size_t num_rows)
    {
        emitObject({}, {}, JSONBloomRole::Scalar, column, type, start_row, num_rows);
    }

private:
    struct ScalarPlan
    {
        std::optional<UInt32> path_id;
        UInt64 seed = 0;
        bool has_dynamic_presence = false;
    };

    struct TypeInfo
    {
        DataTypePtr type;
        SerializationPtr serialization;
        String name;
        String encoded_type;
        WhichDataType which;
        bool has_json_path_descendants = false;
        bool has_dynamic_structure = false;
        bool is_dynamic_complex = false;
        bool raw_value = false;
        /// For arrays, the info of the element type, which is found by name otherwise.
        mutable const TypeInfo * array_element_info = nullptr;
        mutable UnorderedMapWithMemoryTracking<
            String,
            std::array<ScalarPlan, 3>,
            StringHashForHeterogeneousLookup,
            std::equal_to<>> scalar_plans;
    };

    struct PathInfo
    {
        String relative_path;
        String logical_path;
        String hash_path;
        DataTypePtr type;
        ColumnPtr owned_column;
        const IColumn * column = nullptr;
        const TypeInfo * type_info = nullptr;
        std::vector<const TypeInfo *> dynamic_type_infos;
        bool is_dynamic = false;
        bool should_visit = false;
        bool should_index = false;
        bool has_json_path_descendants = false;
        bool remove_low_cardinality = false;
    };

    struct ObjectPlan
    {
        String hash_prefix;
        String logical_prefix;
        VectorWithMemoryTracking<PathInfo> paths;
        const DataTypeObject * type = nullptr;
    };

    struct SharedPathPlan
    {
        String logical_path;
        String hash_path;
        bool should_visit = false;
        bool should_index = false;
        /// The runtime type of the previous value of this path. A path almost always keeps one type, and the binary
        /// type encoding is prefix-free, so a matching prefix skips decoding the type and the lookups keyed by it.
        String last_encoded_type;
        const TypeInfo * last_type_info = nullptr;
        SerializationPtr last_serialization;
        ScalarPlan * last_scalar_plan = nullptr;
    };

    using SharedPathPlans = UnorderedMapWithMemoryTracking<
        String,
        SharedPathPlan,
        StringHashForHeterogeneousLookup,
        std::equal_to<>>;

    /// Objects under the same prefixes (e.g. the elements of an array of objects) have the same shared paths,
    /// so the plans are kept for the whole call rather than rebuilt for every object. A plan caches a
    /// `ScalarPlan`, which depends on the role, so the role is a part of the key.
    SharedPathPlans & getSharedPathPlans(JSONBloomRole role, std::string_view logical_prefix, std::string_view hash_prefix)
    {
        const size_t role_index = static_cast<size_t>(role) - 1;
        if (logical_prefix == hash_prefix)
        {
            auto & plans_by_prefix = shared_path_plans_by_prefix[role_index];
            auto it = plans_by_prefix.find(logical_prefix);
            if (it == plans_by_prefix.end())
                it = plans_by_prefix.try_emplace(String(logical_prefix)).first;
            return it->second;
        }
        return shared_path_plans_by_prefixes[role_index][{String(logical_prefix), String(hash_prefix)}];
    }

    const TypeInfo & getTypeInfo(
        const DataTypePtr & type,
        SerializationPtr serialization = {},
        std::string_view known_name = {})
    {
        String name;
        if (known_name.empty())
        {
            name = type->getName();
            known_name = name;
        }
        auto it = type_infos.find(known_name);
        if (it != type_infos.end())
            return it->second;

        it = type_infos.try_emplace(String(known_name)).first;

        auto & info = it->second;
        info.type = type;
        info.name = it->first;
        info.which = WhichDataType(type);
        info.has_json_path_descendants = hasJSONPathDescendants(type);
        info.has_dynamic_structure = type->hasDynamicStructure();
        info.is_dynamic_complex = typeid_cast<const DataTypeObject *>(type.get()) || typeid_cast<const DataTypeArray *>(type.get())
            || typeid_cast<const DataTypeMap *>(type.get()) || typeid_cast<const DataTypeTuple *>(type.get()) || info.has_dynamic_structure;
        info.raw_value = canHashRawValue(*type);
        if (!isDynamic(type) && !info.which.isNothing() && !info.which.isVariant() && !info.is_dynamic_complex)
        {
            info.serialization = serialization ? std::move(serialization) : type->getDefaultSerialization();
            info.encoded_type = encodeDataType(type);
        }
        return info;
    }

    void emitObject(
        std::string_view hash_prefix,
        std::string_view logical_prefix,
        JSONBloomRole role,
        const ColumnObject & column_object,
        const DataTypeObject & type_object,
        size_t start_row,
        size_t num_rows)
    {
        auto prepare_paths = [&]
        {
            const auto & typed_path_types = type_object.getTypedPaths();
            const auto & typed_path_columns = column_object.getTypedPaths();
            const auto & dynamic_path_columns = column_object.getDynamicPaths();
            VectorWithMemoryTracking<PathInfo> paths;
            paths.reserve(typed_path_types.size() + dynamic_path_columns.size());

            auto initialize_dynamic_type_infos = [&](PathInfo & entry)
            {
                const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*entry.column);
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*dynamic_column.getVariantInfo().variant_type);
                entry.dynamic_type_infos.reserve(variant_type.getVariants().size());
                for (const auto & variant : variant_type.getVariants())
                    entry.dynamic_type_infos.push_back(&getTypeInfo(variant));
            };

            for (const auto & [path, type] : typed_path_types)
            {
                const auto & column = typed_path_columns.at(path);
                const auto * map_type = typeid_cast<const DataTypeMap *>(type.get());
                /// Keep the dictionary for repeated string map keys; other nested types still use range expansion.
                const bool dictionary_map = map_type && map_type->getKeyType()->lowCardinality()
                    && isString(removeLowCardinality(map_type->getKeyType()))
                    && isString(removeLowCardinality(map_type->getValueType()));
                const auto full_type = dictionary_map ? type : recursiveRemoveLowCardinality(type);
                const auto value_type = removeNullableOrLowCardinalityNullable(full_type);
                const bool is_dynamic = DB::isDynamic(value_type);
                auto logical_path = appendPath(logical_prefix, path);
                auto hash_path = appendPath(hash_prefix, path);
                const bool should_visit = path_matcher.shouldVisit(logical_path);
                const bool should_index = path_matcher.shouldIndex(logical_path);
                const auto & type_info = getTypeInfo(value_type);
                paths.push_back(
                    {path,
                     std::move(logical_path),
                     std::move(hash_path),
                     full_type,
                     column,
                     column.get(),
                     &type_info,
                     {},
                     is_dynamic,
                     should_visit,
                     should_index,
                     type_info.has_json_path_descendants,
                     full_type != type});
                if (is_dynamic)
                    initialize_dynamic_type_infos(paths.back());
            }

            for (const auto & [path, column] : dynamic_path_columns)
            {
                auto logical_path = appendPath(logical_prefix, path);
                auto hash_path = appendPath(hash_prefix, path);
                const bool should_visit = path_matcher.shouldVisit(logical_path);
                const bool should_index = path_matcher.shouldIndex(logical_path);
                paths.push_back(
                    {path,
                     std::move(logical_path),
                     std::move(hash_path),
                     nullptr,
                     column,
                     column.get(),
                     nullptr,
                     {},
                     true,
                     should_visit,
                     should_index,
                     false});
                initialize_dynamic_type_infos(paths.back());
            }

            return paths;
        };

        ObjectPlan temporary_plan;
        /// Shared values and converted ranges use temporary columns that can be reused or destroyed between calls.
        auto & plan = temporary_column_depth ? temporary_plan : object_plans[&column_object];
        if (plan.type != &type_object || plan.logical_prefix != logical_prefix)
        {
            plan.hash_prefix = hash_prefix;
            plan.logical_prefix = logical_prefix;
            plan.paths = prepare_paths();
            plan.type = &type_object;
        }
        else if (plan.hash_prefix != hash_prefix)
        {
            for (auto & path : plan.paths)
            {
                path.hash_path.assign(hash_prefix);
                if (!hash_prefix.empty() && !path.relative_path.empty())
                    path.hash_path += '.';
                path.hash_path += path.relative_path;
            }
            plan.hash_prefix = hash_prefix;
        }
        auto & paths = plan.paths;

        const auto & shared_data_offsets = column_object.getSharedDataOffsets();
        const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
        auto & shared_path_plans = getSharedPathPlans(role, logical_prefix, hash_prefix);

        chassert(start_row <= shared_data_offsets.size());
        const size_t end_row = start_row + std::min(num_rows, shared_data_offsets.size() - start_row);
        /// A typed path of the indexed column has the default value where the path is absent. These values are
        /// not indexed, like absent paths in shared data, and conditions do not use the index for default values.
        const bool skip_typed_defaults = !temporary_column_depth && role == JSONBloomRole::Scalar && logical_prefix.empty() && hash_prefix.empty();
        for (auto & path : paths)
        {
            if (!path.should_visit)
                continue;
            if (!path.is_dynamic)
            {
                if (path.remove_low_cardinality)
                {
                    const auto full_column = recursiveRemoveLowCardinality(path.column->cut(start_row, end_row - start_row));
                    ++temporary_column_depth;
                    emitRange(path.hash_path, path.logical_path, role, path.type, *full_column, 0, end_row - start_row,
                        false, *path.type_info, path.should_index, skip_typed_defaults);
                    --temporary_column_depth;
                }
                else
                    emitRange(path.hash_path, path.logical_path, role, path.type, *path.column, start_row, end_row,
                        false, *path.type_info, path.should_index, skip_typed_defaults);
                continue;
            }
            const auto & dynamic = assert_cast<const ColumnDynamic &>(*path.column);
            const auto & variant = dynamic.getVariantColumn();
            const auto discriminator = variant.getGlobalDiscriminatorOfOneNoneEmptyVariantNoNulls();
            if (discriminator && *discriminator != dynamic.getSharedVariantDiscriminator())
            {
                const auto & info = *path.dynamic_type_infos[*discriminator];
                emitRange(path.hash_path, path.logical_path, role, info.type, variant.getVariantByGlobalDiscriminator(*discriminator),
                    start_row, end_row, true, info, path.should_index);
                continue;
            }
            /// Rows of each variant occupy a contiguous range in its subcolumn.
            std::vector<std::pair<size_t, size_t>> ranges(path.dynamic_type_infos.size(), {std::numeric_limits<size_t>::max(), 0});
            for (size_t row = start_row; row != end_row; ++row)
            {
                const auto row_discriminator = variant.globalDiscriminatorAt(row);
                if (row_discriminator == ColumnVariant::NULL_DISCRIMINATOR)
                    continue;
                auto & [begin, end] = ranges[row_discriminator];
                begin = std::min(begin, variant.offsetAt(row));
                end = variant.offsetAt(row) + 1;
            }
            for (size_t variant_index = 0; variant_index != ranges.size(); ++variant_index)
            {
                const auto [begin, end] = ranges[variant_index];
                if (end == 0)
                    continue;
                if (variant_index == dynamic.getSharedVariantDiscriminator())
                {
                    for (size_t row = begin; row != end; ++row)
                        emitSharedValue(path.hash_path, path.logical_path, role, path.should_index, dynamic.getSharedVariant().getDataAt(row));
                }
                else
                {
                    const auto & info = *path.dynamic_type_infos[variant_index];
                    emitRange(path.hash_path, path.logical_path, role, info.type,
                        variant.getVariantByGlobalDiscriminator(variant_index), begin, end, true, info, path.should_index);
                }
            }
        }
        for (size_t row = start_row; row != end_row; ++row)
        {
            const size_t start = shared_data_offsets[static_cast<ssize_t>(row) - 1];
            const size_t end = shared_data_offsets[static_cast<ssize_t>(row)];
            for (size_t shared_index = start; shared_index != end; ++shared_index)
            {
                const auto path = shared_data_paths->getDataAt(shared_index);
                auto plan_it = shared_path_plans.find(path);
                if (plan_it == shared_path_plans.end())
                {
                    SharedPathPlan shared_plan;
                    shared_plan.logical_path = appendPath(logical_prefix, path);
                    shared_plan.hash_path = appendPath(hash_prefix, path);
                    shared_plan.should_visit = path_matcher.shouldVisit(shared_plan.logical_path);
                    shared_plan.should_index = path_matcher.shouldIndex(shared_plan.logical_path);
                    plan_it = shared_path_plans.try_emplace(String(path), std::move(shared_plan)).first;
                }
                auto & shared_plan = plan_it->second;
                if (!shared_plan.should_visit)
                    continue;
                emitSharedValue(
                    shared_plan.hash_path,
                    shared_plan.logical_path,
                    role,
                    shared_plan.should_index,
                    shared_data_values->getDataAt(shared_index),
                    &shared_plan);
            }
        }
    }

    void emitSharedValue(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        bool should_index,
        std::string_view value_data,
        SharedPathPlan * shared_plan = nullptr)
    {
        ReadBufferFromMemory buffer(value_data);
        const TypeInfo * type_info_ptr = nullptr;
        SerializationPtr serialization;
        if (shared_plan && shared_plan->last_type_info && value_data.starts_with(shared_plan->last_encoded_type))
        {
            buffer.position() += shared_plan->last_encoded_type.size();
            type_info_ptr = shared_plan->last_type_info;
            serialization = shared_plan->last_serialization;
        }
        else
        {
            auto decoded = decodeJSONDataType(buffer, serializations_cache);
            type_info_ptr = &getTypeInfo(decoded.type, decoded.serialization, decoded.name);
            serialization = std::move(decoded.serialization);
            if (shared_plan)
            {
                shared_plan->last_encoded_type.assign(value_data.begin(), value_data.begin() + (buffer.position() - value_data.data()));
                shared_plan->last_type_info = type_info_ptr;
                shared_plan->last_serialization = serialization;
                shared_plan->last_scalar_plan = nullptr;
            }
        }
        const auto & type_info = *type_info_ptr;
        const auto & type = type_info.type;
        if (type_info.which.isNothing())
            return;

        if (type_info.which.isNativeNumber() || type_info.which.isStringOrFixedString())
        {
            ScalarPlan keyed_plan;
            ScalarPlan * plan = nullptr;
            if (should_index)
            {
                plan = shared_plan ? shared_plan->last_scalar_plan : nullptr;
                if (!plan)
                {
                    plan = &prepareScalar(hash_path, logical_path, role, true, type_info, keyed_plan);
                    /// A keyed plan lives on the stack; only plans stored in `scalar_plans` can be reused.
                    if (shared_plan && hash_path == logical_path)
                        shared_plan->last_scalar_plan = plan;
                }
            }
            const auto hash = hashSharedScalar(plan ? plan->seed : 0, *type, buffer, format_settings);
            if (plan)
                tokens.addValue(*plan->path_id, hash);
            return;
        }

        auto & available_columns = shared_columns_cache[type_info.name];
        auto column = available_columns.empty() ? type->createColumn() : std::move(available_columns.back());
        if (!available_columns.empty())
            available_columns.pop_back();
        serialization->deserializeBinary(*column, buffer, format_settings);
        ++temporary_column_depth;
        if (type_info.raw_value)
        {
            if (should_index)
                emitScalar(hash_path, logical_path, role, *column, 0, true, type_info);
        }
        else
            emitValue(hash_path, logical_path, role, type, *column, 0, true, type_info, should_index);
        --temporary_column_depth;
        column->popBack(1);
        available_columns.push_back(std::move(column));
    }

    void emitDynamic(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        const ColumnDynamic & dynamic_column,
        size_t row,
        bool should_index)
    {
        if (dynamic_column.isNullAt(row))
            return;

        const auto & variant_column = dynamic_column.getVariantColumn();
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        const size_t variant_row = variant_column.offsetAt(row);

        if (discriminator == dynamic_column.getSharedVariantDiscriminator())
        {
            emitSharedValue(hash_path, logical_path, role, should_index, dynamic_column.getSharedVariant().getDataAt(variant_row));
            return;
        }

        const auto & variant_type = assert_cast<const DataTypeVariant &>(*dynamic_column.getVariantInfo().variant_type);
        const auto & type = variant_type.getVariant(discriminator);
        const auto & type_info = getTypeInfo(type);
        emitValue(
            hash_path,
            logical_path,
            role,
            type,
            variant_column.getVariantByGlobalDiscriminator(discriminator),
            variant_row,
            true,
            type_info,
            should_index);
    }

    void emitArray(
        std::string_view hash_path,
        std::string_view logical_path,
        const DataTypeArray & array_type,
        const TypeInfo & array_info,
        const ColumnArray & array_column,
        size_t row,
        bool is_dynamic,
        bool should_index)
    {
        const auto & nested_type = array_type.getNestedType();
        const auto & nested_column = array_column.getData();
        const auto & offsets = array_column.getOffsets();
        const size_t begin = offsets[static_cast<ssize_t>(row) - 1];
        const size_t end = offsets[row];
        if (!array_info.array_element_info)
            array_info.array_element_info = &getTypeInfo(removeJSONBloomWrappers(nested_type));
        const auto & nested_type_info = *array_info.array_element_info;

        for (size_t element = begin; element != end; ++element)
            emitValue(
                hash_path,
                logical_path,
                JSONBloomRole::ArrayElement,
                nested_type,
                nested_column,
                element,
                is_dynamic || isDynamic(nested_type),
                nested_type_info,
                should_index);
    }

    void emitMapRange(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        const DataTypeMap & map_type,
        const ColumnMap & map_column,
        size_t begin_row,
        size_t end_row,
        bool is_dynamic,
        bool should_index)
    {
        if (is_dynamic)
        {
            addPresence(logical_path, hash_path, role, JSONBloomFilterTokens::PresenceKind::Unsupported);
            return;
        }

        const auto key_type = removeJSONBloomWrappers(map_type.getKeyType());
        const auto & value_type = map_type.getValueType();
        const auto & tuple = map_column.getNestedData();
        const auto & keys = tuple.getColumn(0);
        const auto & values = tuple.getColumn(1);
        const auto & offsets = map_column.getNestedColumn().getOffsets();
        const size_t begin = offsets[static_cast<ssize_t>(begin_row) - 1];
        const size_t end = offsets[end_row - 1];
        if (begin == end)
            return;
        const auto & value_type_info = getTypeInfo(removeJSONBloomWrappers(value_type));
        const auto key_type_name = key_type->getName();
        const auto key_serialization = key_type->getDefaultSerialization();
        WriteBufferFromOwnString encoded_key;
        WriteBufferFromOwnString key_path;

        if (should_index && isString(removeLowCardinality(value_type)))
        {
            const UInt32 path_id = tokens.getPathId(logical_path);
            const auto * lc_keys = map_type.getKeyType()->lowCardinality() && isString(key_type)
                ? &assert_cast<const ColumnLowCardinality &>(keys) : nullptr;
            const auto full_keys = lc_keys ? lc_keys->getDictionary().getNestedColumn() : keys.convertToFullColumnIfLowCardinality();
            VectorWithMemoryTracking<std::optional<UInt64>> seeds(lc_keys ? full_keys->size() : 0);
            /// Maps like feature flags repeat the same pairs in many rows. With dictionaries for both keys and values,
            /// a pair of dictionary indexes identifies a token, so skip the pairs whose tokens are already added.
            const auto * lc_values = lc_keys ? typeid_cast<const ColumnLowCardinality *>(&values) : nullptr;
            seen_map_pairs.clear();
            for (size_t element = begin; element != end; ++element)
            {
                const size_t key_index = lc_keys ? lc_keys->getIndexAt(element) : element;
                if (lc_values)
                {
                    const UInt64 pair = (UInt64(key_index) << 32) | lc_values->getIndexAt(element);
                    HashSet<UInt64>::LookupResult it;
                    bool inserted;
                    seen_map_pairs.emplace(pair, it, inserted);
                    if (!inserted)
                        continue;
                }
                std::optional<UInt64> uncached_seed;
                auto & seed = lc_keys ? seeds[key_index] : uncached_seed;
                if (!seed)
                {
                    const auto path = appendMapKey(
                        hash_path, *key_serialization, key_type_name, *full_keys, key_index, encoded_key, key_path, format_settings);
                    seed = hashToken(path, JSONBloomRole::MapValue, JSONBloomDomain::Typed, value_type_info.name, {});
                }
                tokens.addValue(
                    path_id,
                    hashTypedValue(
                        *seed, *value_type_info.serialization, value_type_info.which, value_type_info.raw_value,
                        values, element, value_buffer, format_settings));
            }
            return;
        }

        const auto full_keys = keys.convertToFullColumnIfLowCardinality();
        for (size_t element = begin; element != end; ++element)
            emitValue(
                appendMapKey(hash_path, *key_serialization, key_type_name, *full_keys, element, encoded_key, key_path, format_settings),
                logical_path,
                JSONBloomRole::MapValue,
                value_type,
                values,
                element,
                false,
                value_type_info,
                should_index);
    }

    void emitTuple(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        const DataTypeTuple & tuple_type,
        const ColumnTuple & tuple_column,
        size_t row,
        bool is_dynamic)
    {
        const auto & element_types = tuple_type.getElements();
        const auto & element_names = tuple_type.getElementNames();
        const auto & columns = tuple_column.getColumns();
        for (size_t i = 0; i != element_types.size(); ++i)
        {
            auto element_logical_path = appendPath(logical_path, element_names[i]);
            if (!path_matcher.shouldVisit(element_logical_path))
                continue;
            const auto & element_type_info = getTypeInfo(removeJSONBloomWrappers(element_types[i]));
            emitValue(
                appendPath(hash_path, element_names[i]),
                element_logical_path,
                role,
                element_types[i],
                *columns[i],
                row,
                is_dynamic,
                element_type_info,
                path_matcher.shouldIndex(element_logical_path));
        }
    }

    void emitRange(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        const DataTypePtr & type,
        const IColumn & column,
        size_t begin,
        size_t end,
        bool is_dynamic,
        const TypeInfo & info,
        bool index_path,
        bool skip_defaults = false)
    {
        if (begin == end || (!index_path && !info.has_json_path_descendants))
            return;
        if (info.serialization)
        {
            const auto * nullable = typeid_cast<const ColumnNullable *>(&column);
            const auto & values = nullable ? nullable->getNestedColumn() : column;
            const UInt32 path_id = tokens.getPathId(logical_path);
            const UInt64 seed = hashToken(hash_path, role, JSONBloomDomain::Typed, info.name, {});
            /// A typed path has a value in every row: the default one where the path is absent. Most typed paths
            /// are absent in most rows, and present values often repeat in adjacent rows, so skip a string equal
            /// to the previous one: its token is already added.
            const auto * strings = info.raw_value ? typeid_cast<const ColumnString *>(&values) : nullptr;
            std::optional<std::string_view> previous_string;
            bool has_value = false;
            /// In a `Nullable` typed path, the default of the nested column is a real value.
            skip_defaults = skip_defaults && !nullable;
            for (size_t row = begin; row != end; ++row)
            {
                if (nullable && nullable->isNullAt(row))
                    continue;
                has_value = true;
                if (strings)
                {
                    const auto data = strings->getDataAt(row);
                    const std::string_view value(data.data(), data.size());
                    if ((skip_defaults && value.empty()) || previous_string == value)
                        continue;
                    previous_string = value;
                }
                else if (skip_defaults && values.isDefaultAt(row))
                    continue;
                tokens.addValue(
                    path_id, hashTypedValue(seed, *info.serialization, info.which, info.raw_value, values, row, value_buffer, format_settings));
            }
            if (is_dynamic && has_value)
            {
                tokens.addPresence(
                    path_id, hash_path, static_cast<UInt8>(role), JSONBloomFilterTokens::PresenceKind::RuntimeType, info.encoded_type);
            }
            return;
        }
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            const auto & array = assert_cast<const ColumnArray &>(column);
            if (index_path && is_dynamic)
                addPresence(logical_path, hash_path, role, JSONBloomFilterTokens::PresenceKind::Complex);
            const auto & nested_type = array_type->getNestedType();
            const auto & nested_info = getTypeInfo(removeJSONBloomWrappers(nested_type));
            emitRange(hash_path, logical_path, JSONBloomRole::ArrayElement, nested_type, array.getData(),
                array.getOffsets()[static_cast<ssize_t>(begin) - 1], array.getOffsets()[end - 1],
                is_dynamic || isDynamic(nested_type), nested_info, index_path);
            return;
        }
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        {
            if (is_dynamic && !index_path)
                return;
            if (is_dynamic)
                addPresence(logical_path, hash_path, role, JSONBloomFilterTokens::PresenceKind::Complex);
            emitMapRange(hash_path, logical_path, role, *map_type, assert_cast<const ColumnMap &>(column),
                begin, end, is_dynamic, index_path);
            return;
        }
        for (size_t row = begin; row != end; ++row)
            emitValue(hash_path, logical_path, role, type, column, row, is_dynamic, info, index_path);
    }

    ScalarPlan & prepareScalar(
        std::string_view path,
        std::string_view logical_path,
        JSONBloomRole role,
        bool is_dynamic,
        const TypeInfo & type_info,
        ScalarPlan & keyed_plan)
    {
        /// Reuse preparation for shared paths. Keyed map scopes can differ on every row.
        auto * plan = &keyed_plan;
        if (path == logical_path)
        {
            auto plan_it = type_info.scalar_plans.find(path);
            if (plan_it == type_info.scalar_plans.end())
                plan_it = type_info.scalar_plans.try_emplace(String(path)).first;
            plan = &plan_it->second[static_cast<size_t>(role) - 1];
        }
        if (!plan->path_id)
        {
            plan->path_id = tokens.getPathId(logical_path);
            plan->seed = hashToken(path, role, JSONBloomDomain::Typed, type_info.name, {});
        }
        if (is_dynamic && !plan->has_dynamic_presence)
        {
            tokens.addPresence(
                *plan->path_id, path, static_cast<UInt8>(role), JSONBloomFilterTokens::PresenceKind::RuntimeType, type_info.encoded_type);
            plan->has_dynamic_presence = true;
        }

        return *plan;
    }

    void emitScalar(
        std::string_view path,
        std::string_view logical_path,
        JSONBloomRole role,
        const IColumn & column,
        size_t row,
        bool is_dynamic,
        const TypeInfo & type_info)
    {
        ScalarPlan keyed_plan;
        auto & plan = prepareScalar(path, logical_path, role, is_dynamic, type_info, keyed_plan);
        tokens.addValue(
            *plan.path_id,
            hashTypedValue(plan.seed, *type_info.serialization, type_info.which, type_info.raw_value, column, row, value_buffer, format_settings));
    }

    void emitValue(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        DataTypePtr type,
        const IColumn & source_column,
        size_t row,
        bool is_dynamic,
        const TypeInfo & supplied_type_info,
        bool index_path)
    {
        if (isDynamic(type))
        {
            emitDynamic(hash_path, logical_path, role, assert_cast<const ColumnDynamic &>(source_column), row, index_path);
            return;
        }

        auto unwrapped = unwrapColumn(std::move(type), source_column, row);
        if (!unwrapped)
            return;

        type = unwrapped->type;
        const IColumn & column = *unwrapped->column;
        const auto & type_info = supplied_type_info.type.get() == type.get() ? supplied_type_info : getTypeInfo(type);

        if (!index_path && !type_info.has_json_path_descendants)
            return;

        if (index_path && is_dynamic && type_info.is_dynamic_complex)
            addPresence(logical_path, hash_path, role, JSONBloomFilterTokens::PresenceKind::Complex);

        if (const auto * object_type = typeid_cast<const DataTypeObject *>(type.get()))
        {
            emitObject(hash_path, logical_path, role, assert_cast<const ColumnObject &>(column), *object_type, row, 1);
            return;
        }

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            emitArray(hash_path, logical_path, *array_type, type_info, assert_cast<const ColumnArray &>(column), row, is_dynamic, index_path);
            return;
        }

        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        {
            if (is_dynamic && !index_path)
                return;
            emitMapRange(hash_path, logical_path, role, *map_type, assert_cast<const ColumnMap &>(column), row, row + 1, is_dynamic, index_path);
            return;
        }

        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            emitTuple(hash_path, logical_path, role, *tuple_type, assert_cast<const ColumnTuple &>(column), row, is_dynamic);
            return;
        }

        if (!index_path)
            return;

        if (type_info.which.isNothing())
            return;
        if (type_info.which.isVariant() || type_info.has_dynamic_structure)
        {
            addPresence(logical_path, hash_path, role, JSONBloomFilterTokens::PresenceKind::Unsupported);
            return;
        }

        emitScalar(hash_path, logical_path, role, column, row, is_dynamic, type_info);
    }

    void addPresence(std::string_view logical_path, std::string_view hash_path, JSONBloomRole role, JSONBloomFilterTokens::PresenceKind kind)
    {
        tokens.addPresence(tokens.getPathId(logical_path), hash_path, static_cast<UInt8>(role), kind);
    }

    JSONBloomFilterTokens & tokens;
    const JSONBloomPathMatcher & path_matcher;
    UnorderedMapWithMemoryTracking<String, SerializationPtr> serializations_cache;
    UnorderedMapWithMemoryTracking<String, VectorWithMemoryTracking<MutableColumnPtr>> shared_columns_cache;
    UnorderedMapWithMemoryTracking<
        String,
        TypeInfo,
        StringHashForHeterogeneousLookup,
        std::equal_to<>> type_infos;
    UnorderedMapWithMemoryTracking<const ColumnObject *, ObjectPlan> object_plans;
    HashSet<UInt64> seen_map_pairs;
    std::array<UnorderedMapWithMemoryTracking<String, SharedPathPlans, StringHashForHeterogeneousLookup, std::equal_to<>>, 3> shared_path_plans_by_prefix;
    std::array<std::map<std::pair<String, String>, SharedPathPlans>, 3> shared_path_plans_by_prefixes;
    size_t temporary_column_depth = 0;
    WriteBufferFromOwnString value_buffer;
    const FormatSettings format_settings;
};

constexpr size_t MAX_INLINE_JSON_BLOOM_FILTER_BYTES = 64;

BloomFilterHashPair jsonBloomHashPair(UInt64 hash)
{
    /// `hashTypedValue` already hashes the token; only the second probe step needs mixing.
    return {hash, intHash64(hash)};
}

struct JSONPathMatch
{
    String path;
    String logical_path;
    DataTypePtr type;
    DataTypePtr cast_type;
    JSONBloomRole role = JSONBloomRole::Scalar;
    bool indexes_missing_values = false;
    bool typed_dynamic = false;
};

struct ArrayJSONBridge
{
    size_t path_end;
    size_t array_depth;
};

DataTypePtr resolveJSONStructuralParent(
    DataTypePtr parent_type,
    const String & path,
    size_t prefix_end,
    size_t parent_end,
    const std::vector<ArrayJSONBridge> & array_json_bridges)
{
    size_t resolved_end = prefix_end;
    for (const auto & bridge : array_json_bridges)
    {
        if (bridge.path_end < prefix_end || bridge.path_end > parent_end)
            continue;

        if (bridge.path_end != resolved_end)
            parent_type = parent_type->tryGetSubcolumnType(path.substr(resolved_end + 1, bridge.path_end - resolved_end - 1));

        for (size_t level = 0; parent_type && level != bridge.array_depth; ++level)
        {
            parent_type = removeJSONBloomWrappers(parent_type);
            const auto * array_type = typeid_cast<const DataTypeArray *>(parent_type.get());
            parent_type = array_type ? array_type->getNestedType() : nullptr;
        }

        if (!parent_type || !isObject(removeJSONBloomWrappers(parent_type)))
            return nullptr;
        resolved_end = bridge.path_end;
    }

    if (resolved_end != parent_end)
        parent_type = parent_type->tryGetSubcolumnType(path.substr(resolved_end + 1, parent_end - resolved_end - 1));
    return parent_type;
}

bool isStructuralJSONSubcolumn(
    const DataTypeObject & object_type, const String & path, const std::vector<ArrayJSONBridge> & array_json_bridges)
{
    const auto delimiter = path.rfind('.');
    if (delimiter == String::npos || object_type.getTypedPaths().contains(path))
        return false;

    const auto last_component = path.substr(delimiter + 1);
    const bool is_array_size = last_component.starts_with("size") && last_component.size() > 4
        && std::ranges::all_of(last_component.substr(4), [](char c) { return c >= '0' && c <= '9'; });

    for (size_t prefix_end = delimiter; prefix_end != String::npos; prefix_end = path.rfind('.', prefix_end - 1))
    {
        const auto typed_path = object_type.getTypedPaths().find(path.substr(0, prefix_end));
        if (typed_path == object_type.getTypedPaths().end())
            continue;

        auto contains_dynamic_type = [](DataTypePtr type)
        {
            while (type)
            {
                type = removeJSONBloomWrappers(std::move(type));
                if (isDynamic(type) || isVariant(type))
                    return true;
                const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
                type = array_type ? array_type->getNestedType() : nullptr;
            }
            return false;
        };

        DataTypePtr parent_type;
        for (size_t parent_end = prefix_end;; parent_end = path.find('.', parent_end + 1))
        {
            parent_type = resolveJSONStructuralParent(typed_path->second, path, prefix_end, parent_end, array_json_bridges);
            const auto unwrapped_parent_type = removeJSONBloomWrappers(parent_type);
            if (typeid_cast<const DataTypeMap *>(unwrapped_parent_type.get()) && parent_end != delimiter)
                return true;
            if (const auto * nested_object = typeid_cast<const DataTypeObject *>(unwrapped_parent_type.get()))
            {
                std::vector<ArrayJSONBridge> nested_bridges;
                for (const auto & bridge : array_json_bridges)
                {
                    if (bridge.path_end > parent_end)
                        nested_bridges.push_back({bridge.path_end - parent_end - 1, bridge.array_depth});
                }
                return isStructuralJSONSubcolumn(*nested_object, path.substr(parent_end + 1), nested_bridges);
            }
            if (contains_dynamic_type(parent_type))
                return true;
            if (parent_end == delimiter)
                break;
        }

        while (parent_type)
        {
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(parent_type.get()))
            {
                if (last_component == "null")
                    return true;
                parent_type = nullable_type->getNestedType();
                continue;
            }
            if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(parent_type.get()))
            {
                parent_type = low_cardinality_type->getDictionaryType();
                continue;
            }
            if (const auto * array_type = typeid_cast<const DataTypeArray *>(parent_type.get()))
            {
                if (is_array_size)
                    return true;
                parent_type = array_type->getNestedType();
                continue;
            }
            if (typeid_cast<const DataTypeMap *>(parent_type.get()))
                return is_array_size || last_component == "keys" || last_component == "values" || last_component.starts_with("key_");
            /// Below a typed leaf, only a named `Tuple` element continues the JSON path. Any other subcolumn,
            /// such as `size` of a `String`, is structural.
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(parent_type.get());
            return !tuple_type || !tuple_type->hasExplicitNames() || !tuple_type->tryGetPositionByName(last_component);
        }
    }

    return false;
}

std::optional<JSONPathMatch> tryMatchJSONSubcolumn(std::string_view column_path, const Block & header)
{
    for (const auto & [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(column_path))
    {
        const String column_name_string(column_name);
        if (!header.has(column_name_string) || !isObject(header.getByName(column_name_string).type))
            continue;
        const auto & object_type = assert_cast<const DataTypeObject &>(*header.getByName(column_name_string).type);
        if (subcolumn_name.empty() || subcolumn_name.starts_with("^") || subcolumn_name.starts_with("@"))
            return std::nullopt;

        const auto subcolumn_type = object_type.tryGetSubcolumnType(String(subcolumn_name));
        if (!subcolumn_type)
            return std::nullopt;

        String path(subcolumn_name);
        std::vector<ArrayJSONBridge> array_json_bridges;
        bool typed_dynamic = false;
        for (size_t type_hint = path.find(".:`"); type_hint != String::npos; type_hint = path.find(".:`", type_hint))
        {
            const size_t type_end = path.find('`', type_hint + 3);
            if (type_end == String::npos)
                return std::nullopt;

            if (type_end + 1 == path.size())
            {
                typed_dynamic = true;
                path.resize(type_hint);
            }
            else
            {
                if (path[type_end + 1] != '.')
                    return std::nullopt;

                auto hinted_type = DataTypeFactory::instance().get(path.substr(type_hint + 3, type_end - type_hint - 3));
                size_t array_depth = 0;
                while (const auto * array_type = typeid_cast<const DataTypeArray *>(hinted_type.get()))
                {
                    hinted_type = array_type->getNestedType();
                    ++array_depth;
                }
                if (array_depth == 0 || !isObject(hinted_type))
                    return std::nullopt;
                array_json_bridges.push_back({type_hint, array_depth});
                path.erase(type_hint, type_end - type_hint + 1);
            }
        }

        if (path.empty())
            return std::nullopt;

        if (isStructuralJSONSubcolumn(object_type, path, array_json_bridges))
            return std::nullopt;

        /// Defaults of typed paths are not indexed, like absent paths in shared data.
        bool indexes_missing_values = false;
        if (!object_type.getTypedPaths().contains(path) && !subcolumn_type->hasDynamicStructure())
        {
            indexes_missing_values = std::ranges::any_of(
                object_type.getTypedPaths(),
                [&](const auto & typed_path)
                {
                    return path.starts_with(typed_path.first) && path.size() > typed_path.first.size()
                        && path[typed_path.first.size()] == '.';
                });
        }

        /// Runtime `Map` values have only a scalar unsupported-type marker, not keyed tokens.
        const auto * map_type = typeid_cast<const DataTypeMap *>(removeJSONBloomWrappers(subcolumn_type).get());
        if (typed_dynamic && map_type)
            return std::nullopt;

        String logical_path = path;
        return JSONPathMatch{
            std::move(path), std::move(logical_path), subcolumn_type, nullptr, JSONBloomRole::Scalar, indexes_missing_values, typed_dynamic};
    }

    return std::nullopt;
}

std::optional<JSONPathMatch> tryMatchDirectJSONPath(
    const RPNBuilderTreeNode & node, const Block & header, const NameSet & columns_shadowing_map_subcolumns)
{
    if (!node.getDAGNode() || columns_shadowing_map_subcolumns.contains(node.getColumnName()))
        return std::nullopt;

    if (const auto parsed_map_subcolumn = tryParseMapSubcolumnName(node.getColumnName(), columns_shadowing_map_subcolumns))
    {
        auto match = tryMatchJSONSubcolumn(parsed_map_subcolumn->first, header);
        const auto * map_type = match ? typeid_cast<const DataTypeMap *>(removeJSONBloomWrappers(match->type).get()) : nullptr;
        if (map_type)
        {
            const auto key_type = removeJSONBloomWrappers(map_type->getKeyType());
            auto key_column = key_type->createColumn();
            ReadBufferFromString buffer(parsed_map_subcolumn->second);
            key_type->getDefaultSerialization()->deserializeWholeText(*key_column, buffer, {});
            match->path = appendMapKey(match->path, key_type, *key_column, 0);
            match->type = map_type->getValueType();
            match->role = JSONBloomRole::MapValue;
            match->indexes_missing_values = false;
            return match;
        }
    }

    return tryMatchJSONSubcolumn(node.getColumnName(), header);
}

std::optional<JSONPathMatch> tryMatchJSONPath(
    const RPNBuilderTreeNode & node, const Block & header, const NameSet & columns_shadowing_map_subcolumns)
{
    if (!node.isFunction())
        return tryMatchDirectJSONPath(node, header, columns_shadowing_map_subcolumns);

    const auto function = node.toFunctionNode();
    if ((function.getFunctionName() == "CAST" || function.getFunctionName() == "_CAST") && function.getArgumentsSize() == 2)
    {
        auto match = tryMatchJSONPath(function.getArgumentAt(0), header, columns_shadowing_map_subcolumns);
        const auto * dag_node = node.getDAGNode();
        if (!match || !dag_node || match->cast_type)
            return std::nullopt;
        match->cast_type = removeJSONBloomWrappers(dag_node->result_type);
        if (typeid_cast<const DataTypeObject *>(match->cast_type.get()) || typeid_cast<const DataTypeArray *>(match->cast_type.get())
            || typeid_cast<const DataTypeMap *>(match->cast_type.get()) || typeid_cast<const DataTypeTuple *>(match->cast_type.get())
            || match->cast_type->hasDynamicStructure())
            return std::nullopt;
        return match;
    }

    if (function.getFunctionName() == "tupleElement" && function.getArgumentsSize() == 2)
    {
        auto match = tryMatchJSONPath(function.getArgumentAt(0), header, columns_shadowing_map_subcolumns);
        const auto * dag_node = node.getDAGNode();
        Field element;
        DataTypePtr element_type;
        if (!match || !dag_node || !function.getArgumentAt(1).tryGetConstant(element, element_type)
            || element.getType() != Field::Types::String)
            return std::nullopt;

        const String & element_name = element.safeGet<String>();
        const auto parent_type = removeJSONBloomWrappers(match->type);
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(parent_type.get());
        if ((!tuple_type || !tuple_type->hasExplicitNames() || !tuple_type->tryGetPositionByName(element_name)) && !isObject(parent_type))
            return std::nullopt;

        match->path = appendPath(match->path, element_name);
        match->logical_path = appendPath(match->logical_path, element_name);
        match->type = dag_node->result_type;
        match->cast_type = nullptr;
        return match;
    }

    if (function.getFunctionName() != "arrayElement" || function.getArgumentsSize() != 2)
        return std::nullopt;

    auto map_node = function.getArgumentAt(0);
    auto map_match = tryMatchDirectJSONPath(map_node, header, columns_shadowing_map_subcolumns);
    if (!map_match)
        return std::nullopt;

    const auto * map_type = typeid_cast<const DataTypeMap *>(removeJSONBloomWrappers(map_node.getDAGNode()->result_type).get());
    if (!map_type)
        return std::nullopt;

    Field key;
    DataTypePtr key_source_type;
    if (!function.getArgumentAt(1).tryGetConstant(key, key_source_type))
        return std::nullopt;

    const auto key_type = removeJSONBloomWrappers(map_type->getKeyType());
    auto key_column = key_type->createColumn();
    if (!key_column->tryInsert(key))
        return std::nullopt;

    map_match->path = appendMapKey(map_match->path, key_type, *key_column, 0);
    map_match->type = map_type->getValueType();
    map_match->cast_type = nullptr;
    map_match->role = JSONBloomRole::MapValue;
    map_match->indexes_missing_values = false;
    return map_match;
}

bool isJSONBloomPathFilterSafe(
    const DataTypePtr & key_type,
    const Field & value,
    const DataTypePtr & value_type,
    const FormatSettings & format_settings,
    bool indexes_missing_values)
{
    /// The generic field conversion does not support decimal-to-number conversions. Compare the
    /// numeric default directly instead of introducing a conversion exception during index analysis.
    if (Field::isDecimal(value.getType()) && isNativeNumber(*key_type))
        return indexes_missing_values || !accurateEquals(value, key_type->getDefault());
    return isJSONPathFilterSafe(key_type, value, value_type, format_settings, indexes_missing_values);
}

/// The indexed path that must be present in a granule for `node`, a JSON subcolumn optionally wrapped in `CAST`, to be
/// non-NULL or differ from its type default. It matches the checks that a `bloom_filter` index over `JSONAllPaths` uses.
std::optional<String> tryMatchJSONPresencePath(
    const RPNBuilderTreeNode & node,
    const Block & header,
    const NameSet & columns_shadowing_map_subcolumns,
    const JSONBloomPathMatcher & path_matcher)
{
    String column_name;
    if (!node.isFunction())
        column_name = node.getColumnName();
    else
    {
        const auto function = node.toFunctionNode();
        if ((function.getFunctionName() != "CAST" && function.getFunctionName() != "_CAST") || function.getArgumentsSize() != 2
            || function.getArgumentAt(0).isFunction())
            return std::nullopt;
        /// A missing `Dynamic` path casts to NULL or the type's default, but a NULL of another type makes a cast to a
        /// non-`Nullable` type throw, and skipping the granule would hide that exception.
        const auto * argument = function.getArgumentAt(0).getDAGNode();
        if (!node.getDAGNode() || !argument
            || (!isDynamic(removeJSONBloomWrappers(argument->result_type)) && !canContainNull(*node.getDAGNode()->result_type)))
            return std::nullopt;
        column_name = function.getArgumentAt(0).getColumnName();
    }

    if (columns_shadowing_map_subcolumns.contains(column_name))
        return std::nullopt;
    auto match = tryMatchJSONSubcolumn(column_name, header);
    if (!match || match->indexes_missing_values || match->role != JSONBloomRole::Scalar || !path_matcher.shouldIndex(match->logical_path))
        return std::nullopt;
    return std::move(match->logical_path);
}

/// The name of the indexed `JSON` column when `node` is `JSONAllPaths(json)`.
std::optional<String> tryMatchJSONAllPaths(const RPNBuilderTreeNode & node, const Block & header)
{
    if (!node.isFunction())
        return std::nullopt;
    const auto function = node.toFunctionNode();
    if (function.getFunctionName() != "JSONAllPaths" || function.getArgumentsSize() != 1 || function.getArgumentAt(0).isFunction())
        return std::nullopt;
    String column_name = function.getArgumentAt(0).getColumnName();
    if (!header.has(column_name) || !isObject(header.getByName(column_name).type))
        return std::nullopt;
    return column_name;
}

/// The indexed path for an element of `JSONAllPaths(json)`. Returns nothing for typed paths, which `JSONAllPaths`
/// can list in every row, and for paths the index does not track.
std::optional<String> tryMatchJSONAllPathsElement(
    const String & column_name, const Field & element, const Block & header, const JSONBloomPathMatcher & path_matcher)
{
    /// Backticks introduce type hints in subcolumn names, so such literal paths stay unindexed.
    if (element.getType() != Field::Types::String || element.safeGet<String>().contains('`'))
        return std::nullopt;
    auto match = tryMatchJSONSubcolumn(column_name + "." + element.safeGet<String>(), header);
    if (!match || match->typed_dynamic || match->indexes_missing_values || match->role != JSONBloomRole::Scalar
        || !isDynamic(match->type) || !path_matcher.shouldIndex(match->logical_path))
        return std::nullopt;
    return std::move(match->logical_path);
}

bool appendTypedProbe(
    std::vector<JSONBloomFilterProbe> & hashes,
    std::string_view path,
    JSONBloomRole role,
    const Field & value,
    const DataTypePtr & source_type,
    DataTypePtr target_type,
    const FormatSettings & format_settings,
    bool require_presence = false)
{
    target_type = removeJSONBloomWrappers(std::move(target_type));
    Field converted;
    if (Field::isDecimal(value.getType()) && WhichDataType(target_type).isNativeInteger())
    {
        /// Convert through a wide integer so neither fractional values nor out-of-range values
        /// can become a matching token. `Int256` covers every decimal's whole part.
        const Field integer = applyVisitor(FieldVisitorConvertToNumber<Int256>(), value);
        if (!accurateEquals(value, integer))
            return false;
        if (isBool(target_type))
        {
            converted = tryConvertFieldToType(integer, DataTypeUInt64(), nullptr, format_settings, /* strict= */ true);
            converted = tryConvertFieldToType(converted, *target_type, nullptr, format_settings, /* strict= */ true);
        }
        else
            converted = tryConvertFieldToType(integer, *target_type, nullptr, format_settings, /* strict= */ true);
    }
    else
        converted = tryConvertFieldToType(value, *target_type, source_type.get(), format_settings, /* strict= */ true);
    if (converted.isNull())
        return false;

    auto column = target_type->createColumn();
    column->insert(converted);
    const auto serialization = target_type->getDefaultSerialization();
    const String type_name = target_type->getName();
    WriteBufferFromOwnString value_buffer;
    hashes.push_back({hashTypedValue(
        hashToken(path, role, JSONBloomDomain::Typed, type_name, {}),
        *serialization, WhichDataType(target_type), canHashRawValue(*target_type), *column, 0, value_buffer, {})});
    /// Keep type-specific pruning when a numeric token is shared by several runtime types.
    if (require_presence && usesNumericToken(type_name))
        hashes.back().required_presence = dynamicTypePresenceHash(path, role, type_name);
    return true;
}

bool comparisonUsesExactConversion(const IDataType & left, const IDataType & right)
{
    /// A failed strict conversion proves inequality only inside the numeric comparison domain.
    /// Other type pairs need a presence probe because execution can match or throw.
    const auto is_number
        = [](const IDataType & type) { return isBool(type.getPtr()) || isNativeNumber(type) || WhichDataType(type).isDecimal(); };
    if ((WhichDataType(left).isDecimal() && WhichDataType(right).isNativeFloat())
        || (WhichDataType(right).isDecimal() && WhichDataType(left).isNativeFloat()))
        return false;
    if (WhichDataType(left).isNativeInteger() && WhichDataType(right).isDecimal())
    {
        /// Execution scales the integer into the decimal's storage type. Keep types whose values
        /// can overflow that representation, even when the constant has no matching token.
        const auto common_type = tryGetLeastSupertype(DataTypes{left.getPtr(), right.getPtr()});
        return common_type && common_type->getTypeId() == right.getTypeId();
    }
    if (WhichDataType(left).isDecimal() && WhichDataType(right).isDecimal() && getDecimalScale(right) > getDecimalScale(left))
    {
        /// Rescaling the column must fit for its entire storage range, including the signed minimum.
        const UInt256 runtime_limit = UInt256(1) << (8 * left.getSizeOfValueInMemory() - 1);
        const UInt256 comparison_limit = UInt256(1) << (8 * std::max(left.getSizeOfValueInMemory(), right.getSizeOfValueInMemory()) - 1);
        const UInt256 multiplier = DecimalUtils::scaleMultiplier<Int256>(getDecimalScale(right) - getDecimalScale(left));
        return runtime_limit <= comparison_limit / multiplier;
    }
    return left.equals(right) || (is_number(left) && is_number(right));
}

void appendDynamicProbe(
    std::vector<JSONBloomFilterProbe> & hashes,
    std::string_view path,
    JSONBloomRole role,
    const Field & value,
    const DataTypePtr & value_type,
    const DataTypePtr & runtime_type,
    const FormatSettings & format_settings)
{
    const WhichDataType which(runtime_type);
    const bool comparable = isNativeNumber(*runtime_type) || which.isDecimal() || which.isStringOrFixedString()
        || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64() || which.isUUID() || which.isIPv4() || which.isIPv6();
    if (comparable && comparisonUsesExactConversion(*runtime_type, *removeJSONBloomWrappers(value_type)))
    {
        /// A failed decimal conversion can mean that execution overflows, rather than proving inequality.
        if (!appendTypedProbe(hashes, path, role, value, value_type, runtime_type, format_settings, true) && which.isDecimal())
            hashes.push_back({dynamicTypePresenceHash(path, role, runtime_type->getName()), true});
    }
    else
    {
        if (comparable && WhichDataType(removeJSONBloomWrappers(value_type)).isStringOrFixedString()
            && appendTypedProbe(hashes, path, role, value, value_type, runtime_type, format_settings, true))
            return;
        /// A constant string is compared after conversion to the runtime type. A failed conversion
        /// must retain the type: execution may throw, and this type may be absent in other granules.
        hashes.push_back({dynamicTypePresenceHash(path, role, runtime_type->getName()), true});
    }
}

std::vector<JSONBloomFilterProbe> makeDynamicCastProbes(
    std::string_view path,
    JSONBloomRole role,
    DataTypePtr source_type,
    DataTypePtr cast_type,
    const Field & value,
    const DataTypePtr & value_type,
    const FormatSettings &)
{
    source_type = removeJSONBloomWrappers(std::move(source_type));
    cast_type = removeJSONBloomWrappers(std::move(cast_type));
    if (!isDynamic(source_type))
        return {};

    auto dynamic = std::make_shared<JSONBloomFilterDynamicProbe>(String(path), static_cast<UInt8>(role), value, value_type, cast_type);
    return {{unsupportedDynamicTypeHash(path, role), false, {}, std::move(dynamic)},
            {dynamicComplexPresenceHash(path, role), true},
            {unsupportedDynamicTypeHash(path, role), true}};
}

std::vector<JSONBloomFilterProbe> makeValueProbes(
    std::string_view path,
    JSONBloomRole role,
    DataTypePtr target_type,
    const Field & value,
    const DataTypePtr & source_type,
    const FormatSettings & format_settings,
    bool require_presence = false)
{
    std::vector<JSONBloomFilterProbe> hashes;
    target_type = removeJSONBloomWrappers(std::move(target_type));

    if (isDynamic(target_type))
    {
        auto dynamic = std::make_shared<JSONBloomFilterDynamicProbe>(String(path), static_cast<UInt8>(role), value, source_type, nullptr);
        return {{unsupportedDynamicTypeHash(path, role), false, {}, std::move(dynamic)},
                {dynamicComplexPresenceHash(path, role), true},
                {unsupportedDynamicTypeHash(path, role), true}};
    }

    if (target_type->hasDynamicStructure() || typeid_cast<const DataTypeArray *>(target_type.get())
        || typeid_cast<const DataTypeMap *>(target_type.get()) || typeid_cast<const DataTypeTuple *>(target_type.get())
        || typeid_cast<const DataTypeVariant *>(target_type.get()))
        return hashes;

    const auto unwrapped_source_type = removeJSONBloomWrappers(source_type);
    /// Execution compares a string with a non-string constant by conversion that can throw, even though the constant
    /// formats as a string. Skipping the granules would hide that exception.
    if (isStringOrFixedString(target_type) && !isStringOrFixedString(unwrapped_source_type))
        return hashes;
    if ((WhichDataType(*target_type).isDecimal()
            && !comparisonUsesExactConversion(*target_type, *unwrapped_source_type)
            && !WhichDataType(*unwrapped_source_type).isStringOrFixedString())
        || (WhichDataType(*unwrapped_source_type).isDecimal()
            && (WhichDataType(*target_type).isNativeFloat()
                || (WhichDataType(*target_type).isNativeInteger() && !comparisonUsesExactConversion(*target_type, *unwrapped_source_type)))))
        return hashes;

    appendTypedProbe(hashes, path, role, value, source_type, target_type, format_settings, require_presence);

    std::ranges::sort(hashes);
    hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
    return hashes;
}

std::vector<JSONBloomFilterProbe> makeArrayElementProbes(
    std::string_view path,
    JSONBloomRole role,
    DataTypePtr target_type,
    const Field & value,
    DataTypePtr source_type,
    const FormatSettings & format_settings)
{
    target_type = removeJSONBloomWrappers(std::move(target_type));
    if (const auto * target_array_type = typeid_cast<const DataTypeArray *>(target_type.get()))
    {
        source_type = removeJSONBloomWrappers(std::move(source_type));
        const auto * source_array_type = typeid_cast<const DataTypeArray *>(source_type.get());
        if (!source_array_type || value.getType() != Field::Types::Array || value.safeGet<Array>().empty())
            return {};

        std::vector<JSONBloomFilterProbe> hashes;
        for (const auto & element : value.safeGet<Array>())
        {
            auto element_hashes = makeArrayElementProbes(
                path, role, target_array_type->getNestedType(), element, source_array_type->getNestedType(), format_settings);
            if (element_hashes.empty())
                return {};
            hashes.insert(hashes.end(), element_hashes.begin(), element_hashes.end());
        }
        std::ranges::sort(hashes);
        hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
        return hashes;
    }

    if (!isDynamic(target_type))
        return makeValueProbes(path, role, target_type, value, source_type, format_settings);

    source_type = removeJSONBloomWrappers(std::move(source_type));
    if (WhichDataType(source_type).isNothing() || source_type->hasDynamicStructure()
        || typeid_cast<const DataTypeArray *>(source_type.get()) || typeid_cast<const DataTypeMap *>(source_type.get())
        || typeid_cast<const DataTypeTuple *>(source_type.get()))
        return {};

    std::vector<JSONBloomFilterProbe> hashes;
    /// `Array(Dynamic)` membership compares the literal's runtime variant directly.
    /// Other element variants cannot match and do not throw, so they need no presence probes.
    appendTypedProbe(hashes, path, role, value, source_type, source_type, format_settings, true);
    return hashes;
}

}

void MergeTreeIndexGranuleJSONBloomFilter::prepareDynamicProbe(
    const String & path, const JSONBloomFilterProbe & probe, const FormatSettings & format_settings)
{
    if (!probe.dynamic)
        return;
    const auto filter_it = paths.find(path);
    if (filter_it == paths.end())
        return;
    auto & filter = filter_it->second;
    if (!filter.dynamic_types_changed && filter.dynamic_probes.contains(probe.dynamic))
        return;
    auto & prepared = filter.dynamic_probes[probe.dynamic];
    prepared.clear();
    const auto types_it = std::ranges::find(filter.dynamic_types, probe.hash, &decltype(filter.dynamic_types)::value_type::first);
    if (types_it == filter.dynamic_types.end())
        return;

    /// This cache belongs to the reader's granule and is filled before publishing it for evaluation.
    auto & compiled = compiled_dynamic_probes[probe.dynamic];
    const auto & dynamic = *probe.dynamic;
    const auto role = static_cast<JSONBloomRole>(dynamic.role);
    for (const auto & encoded_type : types_it->second)
    {
        auto [it, inserted] = compiled.try_emplace(encoded_type);
        if (inserted)
        {
            ReadBufferFromString type_buffer(encoded_type);
            const auto runtime_type = decodeDataType(type_buffer);
            /// Type equality ignores timezones and custom names such as `Bool`, which affect casts and comparisons.
            const bool changes_type = dynamic.cast_type && runtime_type->getName() != dynamic.cast_type->getName();
            DataTypePtr common_type;
            if (changes_type && isNativeNumber(*runtime_type) && isNativeNumber(*dynamic.cast_type) && !isBool(dynamic.cast_type))
                common_type = tryGetLeastSupertype(DataTypes{runtime_type, dynamic.cast_type});
            const bool widening_numeric_cast = common_type && common_type->equals(*dynamic.cast_type);
            if (widening_numeric_cast && WhichDataType(removeJSONBloomWrappers(dynamic.value_type)).isStringOrFixedString())
            {
                /// Parse strings in the comparison's type before probing the original runtime type.
                /// In particular, a wider type can parse constants that the original type cannot.
                const auto converted = tryConvertFieldToType(
                    dynamic.value, *dynamic.cast_type, dynamic.value_type.get(), format_settings, /* strict= */ true);
                if (converted.isNull())
                    it->second.push_back({dynamicTypePresenceHash(dynamic.path, role, runtime_type->getName()), true});
                else
                    appendDynamicProbe(it->second, dynamic.path, role, converted, dynamic.cast_type, runtime_type, format_settings);
            }
            else if (changes_type
                && (!widening_numeric_cast
                    || !comparisonUsesExactConversion(*dynamic.cast_type, *removeJSONBloomWrappers(dynamic.value_type))))
                it->second.push_back({dynamicTypePresenceHash(dynamic.path, role, runtime_type->getName()), true});
            else
                appendDynamicProbe(it->second, dynamic.path, role, dynamic.value, dynamic.value_type, runtime_type, format_settings);
            /// The type list already proves presence. Removing redundant guards lets equal numeric
            /// values share a single probe across runtime types.
            for (auto & typed_probe : it->second)
                typed_probe.required_presence.reset();
        }
        prepared.insert(prepared.end(), it->second.begin(), it->second.end());
    }
    std::ranges::sort(prepared);
    prepared.erase(std::unique(prepared.begin(), prepared.end()), prepared.end());
}

struct MergeTreeIndexGranuleJSONBloomFilter::BuiltPaths
{
    /// Presence records sharing a scope path and role. They replace the hashes that the reader recomputes.
    /// `path` indexes `scope_paths`, where 0 is the logical path; `types_begin` and `types_end` index `scope_types`.
    struct Scope
    {
        UInt32 path;
        UInt8 role;
        UInt8 flags;
        size_t types_begin;
        size_t types_end;
    };

    /// A path with at least one token or presence record, in name order. The ranges index `values` and `scopes`.
    struct Path
    {
        size_t name_end;
        size_t values_begin;
        size_t values_end;
        size_t scopes_begin;
        size_t scopes_end;
    };

    String names;
    std::vector<Path> paths;
    std::vector<UInt64> values;
    std::vector<Scope> scopes;
    std::vector<UInt32> scope_types;
    std::vector<String> type_names;
    std::vector<String> scope_paths{String{}};
};

MergeTreeIndexGranuleJSONBloomFilter::MergeTreeIndexGranuleJSONBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, std::shared_ptr<const JSONBloomPathMatcher> path_matcher_)
    : bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
    , path_matcher(std::move(path_matcher_))
{
}


MergeTreeIndexGranuleJSONBloomFilter::MergeTreeIndexGranuleJSONBloomFilter(
    size_t bits_per_row_,
    size_t hash_functions_,
    const JSONBloomFilterTokens & tokens,
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher_)
    : MergeTreeIndexGranuleJSONBloomFilter(bits_per_row_, hash_functions_, std::move(path_matcher_))
{
    has_rows = true;
    const size_t num_paths = tokens.paths.size();

    /// Group the value tokens by path with a counting sort: two linear passes without per-path allocations.
    std::vector<size_t> value_offsets(num_paths + 1);
    for (const auto & cell : tokens.values)
        ++value_offsets[static_cast<UInt64>(cell.getKey() >> 64) + 1];
    std::partial_sum(value_offsets.begin(), value_offsets.end(), value_offsets.begin());

    built = std::make_unique<BuiltPaths>();
    built->values.resize(value_offsets.back());
    auto value_positions = value_offsets;
    for (const auto & cell : tokens.values)
        built->values[value_positions[static_cast<UInt64>(cell.getKey() >> 64)]++] = static_cast<UInt64>(cell.getKey());

    struct PresenceRecord
    {
        UInt32 path_id;
        UInt32 scope_id;
        UInt8 role;
        UInt8 kind;
        UInt32 type_id;
        auto operator<=>(const PresenceRecord &) const = default;
    };
    std::vector<PresenceRecord> presence;
    presence.reserve(tokens.presence.size());
    for (const auto & cell : tokens.presence)
    {
        const auto low = static_cast<UInt64>(cell.getKey());
        const auto high = static_cast<UInt64>(cell.getKey() >> 64);
        presence.push_back({static_cast<UInt32>(low), static_cast<UInt32>(low >> 32), static_cast<UInt8>(high),
                            static_cast<UInt8>(high >> 8), static_cast<UInt32>(high >> 16)});
    }
    std::ranges::sort(presence);

    /// Paths are written in name order, so each name can be stored as a suffix of the previous one.
    std::vector<UInt32> order;
    std::vector<size_t> presence_begin(num_paths + 1, presence.size());
    for (size_t i = presence.size(); i != 0; --i)
        presence_begin[presence[i - 1].path_id] = i - 1;
    for (size_t path_id = num_paths; path_id != 0; --path_id)
        presence_begin[path_id - 1] = std::min(presence_begin[path_id - 1], presence_begin[path_id]);
    for (UInt32 path_id = 0; path_id != num_paths; ++path_id)
        if (value_offsets[path_id] != value_offsets[path_id + 1] || presence_begin[path_id] != presence_begin[path_id + 1])
            order.push_back(path_id);
    std::ranges::sort(order, [&](UInt32 left, UInt32 right) { return tokens.paths[left] < tokens.paths[right]; });

    built->type_names.assign(tokens.types.begin(), tokens.types.end());
    using enum JSONBloomFilterTokens::PresenceKind;
    for (const UInt32 path_id : order)
    {
        const size_t scopes_begin = built->scopes.size();
        for (size_t i = presence_begin[path_id]; i != presence_begin[path_id + 1]; ++i)
        {
            const auto & record = presence[i];
            if (i == presence_begin[path_id] || record.scope_id != presence[i - 1].scope_id || record.role != presence[i - 1].role)
            {
                UInt32 scope_path = 0;
                if (record.scope_id != path_id)
                {
                    scope_path = static_cast<UInt32>(built->scope_paths.size());
                    built->scope_paths.emplace_back(tokens.paths[record.scope_id]);
                }
                built->scopes.push_back({scope_path, record.role, 0, built->scope_types.size(), built->scope_types.size()});
            }
            auto & scope = built->scopes.back();
            if (record.kind == static_cast<UInt8>(RuntimeType))
            {
                built->scope_types.push_back(record.type_id);
                scope.types_end = built->scope_types.size();
            }
            else
                scope.flags |= record.kind == static_cast<UInt8>(Complex) ? SCOPE_HAS_COMPLEX : SCOPE_HAS_UNSUPPORTED;
        }
        built->names.append(tokens.paths[path_id]);
        built->paths.push_back(
            {built->names.size(), value_offsets[path_id], value_offsets[path_id + 1], scopes_begin, built->scopes.size()});
    }
}

MergeTreeIndexGranuleJSONBloomFilter::~MergeTreeIndexGranuleJSONBloomFilter() = default;

void MergeTreeIndexGranuleJSONBloomFilter::addScope(
    PathFilter & filter, std::string_view logical_path, std::string_view scope_path, UInt8 role_value, UInt8 flags, std::vector<String> types)
{
    const std::string_view path = scope_path.empty() ? logical_path : scope_path;
    const auto role = static_cast<JSONBloomRole>(role_value);
    if (flags & SCOPE_HAS_COMPLEX)
        filter.presence.push_back(dynamicComplexPresenceHash(path, role));
    if (flags & SCOPE_HAS_UNSUPPORTED)
        filter.presence.push_back(unsupportedDynamicTypeHash(path, role));
    if (types.empty())
        return;
    for (const auto & type : types)
    {
        auto [it, inserted] = runtime_type_names.try_emplace(type);
        if (inserted)
        {
            ReadBufferFromString type_buffer(type);
            it->second = decodeDataType(type_buffer)->getName();
        }
        filter.presence.push_back(dynamicTypePresenceHash(path, role, it->second));
    }
    filter.next_dynamic_types.emplace_back(unsupportedDynamicTypeHash(path, role), std::move(types));
}

void MergeTreeIndexGranuleJSONBloomFilter::finishPath(PathFilter & filter)
{
    std::ranges::sort(filter.presence);
    filter.dynamic_types_changed = filter.dynamic_types != filter.next_dynamic_types;
    filter.dynamic_types.swap(filter.next_dynamic_types);
    filter.next_dynamic_types.clear();
}

void MergeTreeIndexGranuleJSONBloomFilter::materialize()
{
    if (!built)
        return;

    size_t name_begin = 0;
    for (const auto & path : built->paths)
    {
        const auto name = std::string_view(built->names).substr(name_begin, path.name_end - name_begin);
        name_begin = path.name_end;
        auto & filter = paths[String(name)];
        for (size_t i = path.scopes_begin; i != path.scopes_end; ++i)
        {
            const auto & scope = built->scopes[i];
            std::vector<String> types;
            for (size_t j = scope.types_begin; j != scope.types_end; ++j)
                types.push_back(built->type_names[built->scope_types[j]]);
            addScope(filter, name, built->scope_paths[scope.path], scope.role, scope.flags, std::move(types));
        }
        finishPath(filter);
        if (path.values_begin != path.values_end)
        {
            const size_t num_values = path.values_end - path.values_begin;
            filter.values = std::make_shared<BloomFilter>((bits_per_row * num_values + 7) / 8, hash_functions, 0);
            std::vector<BloomFilterHashPair> pairs;
            pairs.reserve(num_values);
            for (size_t i = path.values_begin; i != path.values_end; ++i)
                pairs.push_back(jsonBloomHashPair(built->values[i]));
            filter.values->addHashPairs(pairs.data(), pairs.size());
        }
    }
    built.reset();
}

void MergeTreeIndexGranuleJSONBloomFilter::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "`jsonbf_v1` requires serialization with multiple streams");
}

void MergeTreeIndexGranuleJSONBloomFilter::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "`jsonbf_v1` requires deserialization with multiple streams");
}

size_t MergeTreeIndexGranuleJSONBloomFilter::memoryUsageBytes() const
{
    size_t bytes = 0;
    if (built)
    {
        bytes += built->names.capacity() + built->paths.capacity() * sizeof(BuiltPaths::Path)
            + built->values.capacity() * sizeof(UInt64) + built->scopes.capacity() * sizeof(BuiltPaths::Scope)
            + built->scope_types.capacity() * sizeof(UInt32);
        for (const auto & strings : {&built->type_names, &built->scope_paths})
            for (const auto & string : *strings)
                bytes += sizeof(String) + string.capacity();
    }
    for (const auto & [path, filter] : paths)
    {
        bytes += path.capacity() + filter.presence.capacity() * sizeof(UInt64) + (filter.values ? filter.values->memoryUsageBytes() : 0);
        bytes += filter.dynamic_types.capacity() * sizeof(decltype(filter.dynamic_types)::value_type);
        for (const auto & [scope, types] : filter.dynamic_types)
        {
            bytes += types.capacity() * sizeof(String);
            for (const auto & type : types)
                bytes += type.capacity();
        }
        for (const auto & [probe, prepared] : filter.dynamic_probes)
            bytes += sizeof(decltype(filter.dynamic_probes)::value_type) + prepared.capacity() * sizeof(JSONBloomFilterProbe);
    }
    for (const auto & [probe, compiled] : compiled_dynamic_probes)
        for (const auto & [type, prepared] : compiled)
            bytes += sizeof(std::remove_cvref_t<decltype(compiled)>::value_type) + type.capacity() + prepared.capacity() * sizeof(JSONBloomFilterProbe);
    return bytes;
}

bool MergeTreeIndexGranuleJSONBloomFilter::PathFilter::matches(const JSONBloomFilterProbe & probe, bool pending_matches) const
{
    if (probe.dynamic)
        return std::ranges::any_of(dynamic_probes.at(probe.dynamic),
            [&](const auto & typed_probe) { return matches(typed_probe, pending_matches); });
    if (probe.is_presence)
        return std::ranges::binary_search(presence, probe.hash);
    return (!probe.required_presence || std::ranges::binary_search(presence, *probe.required_presence))
        && (pending ? pending_matches : (values && values->findHashPair(jsonBloomHashPair(probe.hash))));
}

bool MergeTreeIndexGranuleJSONBloomFilter::matches(const String & path, const JSONBloomFilterProbe & probe, bool pending_matches) const
{
    const auto it = paths.find(path);
    return it != paths.end() && it->second.matches(probe, pending_matches);
}

void MergeTreeIndexGranuleJSONBloomFilter::serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const
{
    if (!built)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`jsonbf_v1` can serialize only granules built by its aggregator");

    auto & directory = streams.at(MergeTreeIndexSubstream::Type::Regular)->compressed_hashing;
    auto & values_stream = *streams.at(MergeTreeIndexSubstream::Type::JSONBloomFilterValues);
    /// One filter is reused for all paths, so building a granule allocates no filter per path.
    std::optional<BloomFilter> filter;
    std::vector<BloomFilterHashPair> pairs;
    writeVarUInt(built->paths.size(), directory);
    size_t name_begin = 0;
    std::string_view previous_name;
    for (const auto & path : built->paths)
    {
        /// Paths are sorted, so a name is stored as the length it shares with the previous name and the rest.
        const auto name = std::string_view(built->names).substr(name_begin, path.name_end - name_begin);
        name_begin = path.name_end;
        const size_t shared = std::ranges::mismatch(name, previous_name).in1 - name.begin();
        writeVarUInt(shared, directory);
        writeStringBinary(name.substr(shared), directory);
        previous_name = name;

        /// Presence hashes and runtime type scopes are recomputed by the reader from these records.
        writeVarUInt(path.scopes_end - path.scopes_begin, directory);
        for (size_t i = path.scopes_begin; i != path.scopes_end; ++i)
        {
            const auto & scope = built->scopes[i];
            writeStringBinary(built->scope_paths[scope.path], directory);
            writeBinary(scope.role, directory);
            writeBinary(scope.flags, directory);
            writeVarUInt(scope.types_end - scope.types_begin, directory);
            for (size_t j = scope.types_begin; j != scope.types_end; ++j)
                writeStringBinary(built->type_names[built->scope_types[j]], directory);
        }

        const size_t num_values = path.values_end - path.values_begin;
        const size_t size = num_values ? (bits_per_row * num_values + 7) / 8 : 0;
        writeVarUInt(size, directory);
        if (!size)
            continue;

        if (filter)
        {
            filter->resize(size);
            std::fill(filter->getFilter().begin(), filter->getFilter().end(), 0);
        }
        else
            filter.emplace(size, hash_functions, 0);
        pairs.clear();
        for (size_t i = path.values_begin; i != path.values_end; ++i)
            pairs.push_back(jsonBloomHashPair(built->values[i]));
        filter->addHashPairs(pairs.data(), pairs.size());

        WriteBuffer * out = &directory;
        if (size > MAX_INLINE_JSON_BLOOM_FILTER_BYTES)
        {
            /// Each large filter starts a compressed block; small filters share the directory's block.
            values_stream.compressed_hashing.next();
            auto mark = values_stream.getCurrentMark();
            writeVarUInt(mark.offset_in_compressed_file, directory);
            writeVarUInt(mark.offset_in_decompressed_block, directory);
            out = &values_stream.compressed_hashing;
        }
        const auto & words = filter->getFilter();
        if constexpr (std::endian::native == std::endian::little)
            out->write(reinterpret_cast<const char *>(words.data()), size);
        else
            for (size_t i = 0; i < size; ++i)
                writeBinary(static_cast<UInt8>(words[i / 8] >> (8 * (i % 8))), *out);
    }
    values_stream.compressed_hashing.next();
}

void MergeTreeIndexGranuleJSONBloomFilter::deserializeBinaryWithMultipleStreams(
    MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    /// The part metadata has an unknown version, so its granules cannot be parsed. No granule of the part reads the
    /// streams, so their positions stay consistent, and evaluation treats every path as unknown.
    if (unsupported)
    {
        has_rows = true;
        return;
    }
    if (state.version != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown `jsonbf_v1` index version {}", state.version);
    auto & directory = *streams.at(MergeTreeIndexSubstream::Type::Regular)->getDataBuffer();
    auto & values_stream = *streams.at(MergeTreeIndexSubstream::Type::JSONBloomFilterValues);
    const auto * condition = typeid_cast<const MergeTreeIndexConditionJSONBloomFilter *>(state.condition);
    const bool need_dynamic_types = !condition || condition->needsDynamicTypes();
    for (auto & [path, filter] : paths)
        filter.present = false;
    has_rows = true;
    size_t path_count = 0;
    readVarUInt(path_count, directory);
    struct PendingFilter
    {
        PathFilter * filter;
        size_t size;
        MarkInCompressedFile mark;
    };
    std::vector<PendingFilter> pending_filters;
    auto read_filter = [&](PathFilter & filter, size_t size, ReadBuffer & in)
    {
        if (filter.values)
            filter.values->resize(size);
        else
            filter.values = std::make_shared<BloomFilter>(size, hash_functions, 0);
        auto & words = filter.values->getFilter();
        if constexpr (std::endian::native == std::endian::little)
            in.readStrict(reinterpret_cast<char *>(words.data()), size);
        else
        {
            std::fill(words.begin(), words.end(), 0);
            for (size_t j = 0; j < size; ++j)
            {
                UInt8 byte = 0;
                readBinary(byte, in);
                words[j / 8] |= UInt64(byte) << (8 * (j % 8));
            }
        }
        filter.pending = false;
    };
    String path;
    String scope_path;
    std::vector<String> types;
    for (size_t i = 0; i < path_count; ++i)
    {
        size_t shared = 0;
        readVarUInt(shared, directory);
        if (shared > path.size())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid path prefix length in `jsonbf_v1`");
        path.resize(shared);
        size_t suffix_size = 0;
        readVarUInt(suffix_size, directory);
        SerializationString::checkStringSize(suffix_size, {});
        path.resize(shared + suffix_size);
        directory.readStrict(path.data() + shared, suffix_size);

        bool needed = !condition || condition->usesPath(path);
        PathFilter * filter = needed ? &paths[path] : nullptr;
        if (filter)
        {
            filter->present = true;
            filter->pending = false;
            filter->presence.clear();
        }

        size_t scope_count = 0;
        readVarUInt(scope_count, directory);
        for (size_t j = 0; j < scope_count; ++j)
        {
            readStringBinary(scope_path, directory);
            UInt8 role = 0;
            UInt8 flags = 0;
            readBinary(role, directory);
            readBinary(flags, directory);
            size_t type_count = 0;
            readVarUInt(type_count, directory);
            types.resize(type_count);
            for (auto & type : types)
                readStringBinary(type, directory);
            if (filter)
                addScope(*filter, path, scope_path, role, flags, types);
        }
        if (filter)
            finishPath(*filter);

        size_t size = 0;
        readVarUInt(size, directory);
        if (size > MAX_INLINE_JSON_BLOOM_FILTER_BYTES)
        {
            MarkInCompressedFile mark{};
            readVarUInt(mark.offset_in_compressed_file, directory);
            readVarUInt(mark.offset_in_decompressed_block, directory);
            if (filter)
            {
                filter->pending = true;
                pending_filters.push_back({filter, size, mark});
            }
        }
        else if (size)
        {
            if (filter)
                read_filter(*filter, size, directory);
            else
                directory.ignore(size);
        }
        else if (filter)
            filter->values.reset();
    }
    std::erase_if(paths, [](const auto & entry) { return !entry.second.present; });
    if (condition && need_dynamic_types)
        condition->prepareDynamicProbes(*this);
    for (const auto & pending : pending_filters)
    {
        /// Bound the result with unread filters matching or rejecting. Stop once either bound decides
        /// whether to retain the granule; unread filters stay unknown for partial disjunction results.
        if (condition && (!condition->mayBeTrueOnGranule(*this, true) || condition->mayBeTrueOnGranule(*this, false)))
            break;
        values_stream.seekToMark(pending.mark);
        read_filter(*pending.filter, pending.size, *values_stream.getDataBuffer());
    }
}

bool MergeTreeIndexConditionJSONBloomFilter::usesPath(const String & path) const
{
    return std::ranges::any_of(rpn, [&](const auto & element)
    {
        return element.path == path || std::ranges::find(element.exists_paths, path) != element.exists_paths.end();
    });
}

void MergeTreeIndexConditionJSONBloomFilter::prepareDynamicProbes(MergeTreeIndexGranuleJSONBloomFilter & granule) const
{
    for (const auto & element : rpn)
    {
        for (const auto & probe : element.hashes)
            granule.prepareDynamicProbe(element.path, probe, comparison_format_settings);
        for (const auto & alternative : element.alternatives)
            for (const auto & probe : alternative)
                granule.prepareDynamicProbe(element.path, probe, comparison_format_settings);
    }
}

MergeTreeIndexAggregatorJSONBloomFilter::MergeTreeIndexAggregatorJSONBloomFilter(
    size_t bits_per_row_,
    size_t hash_functions_,
    String column_name_,
    DataTypePtr column_type_,
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher_)
    : bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
    , column_name(std::move(column_name_))
    , column_type(std::move(column_type_))
    , path_matcher(std::move(path_matcher_))
    , tokens(std::make_unique<JSONBloomFilterTokens>())
{
}

MergeTreeIndexAggregatorJSONBloomFilter::~MergeTreeIndexAggregatorJSONBloomFilter() = default;

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorJSONBloomFilter::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleJSONBloomFilter>(bits_per_row, hash_functions, *tokens, path_matcher);
    /// Adjacent granules usually have a similar number of values, so presize the table to avoid rehashing as it grows.
    const size_t values_hint = tokens->values.size();
    tokens = std::make_unique<JSONBloomFilterTokens>();
    tokens->values.reserve(values_hint);
    total_rows = 0;
    return granule;
}

void MergeTreeIndexAggregatorJSONBloomFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    const size_t rows = std::min(limit, block.rows() - *pos);
    const auto & column = block.getByName(column_name).column;
    const auto * object_type = typeid_cast<const DataTypeObject *>(column_type.get());
    const auto * object_column = typeid_cast<const ColumnObject *>(column.get());
    if (!object_type || !object_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`jsonbf_v1` expected a `JSON` column");

    JSONBloomExtractor extractor(*tokens, *path_matcher);
    extractor.emitObject(*object_column, *object_type, *pos, rows);

    *pos += rows;
    total_rows += rows;
}

MergeTreeIndexConditionJSONBloomFilter::MergeTreeIndexConditionJSONBloomFilter(
    const ActionsDAG::Node * predicate,
    ContextPtr context,
    const Block & header_,
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher_,
    NameSet columns_shadowing_map_subcolumns_)
    : header(header_)
    , path_matcher(std::move(path_matcher_))
    , comparison_format_settings(getJSONComparisonFormatSettings(context))
    , columns_shadowing_map_subcolumns(std::move(columns_shadowing_map_subcolumns_))
{
    if (!predicate)
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        predicate, context, [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
    const auto is_dynamic = [](const auto & probe) { return bool(probe.dynamic); };
    has_dynamic_probes = std::ranges::any_of(rpn, [&](const auto & element)
    {
        return std::ranges::any_of(element.hashes, is_dynamic)
            || std::ranges::any_of(element.alternatives, [&](const auto & alternative)
            {
                return std::ranges::any_of(alternative, is_dynamic);
            });
    });
}

bool MergeTreeIndexConditionJSONBloomFilter::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        rpn, {RPNElement::FUNCTION_ANY, RPNElement::FUNCTION_ALL, RPNElement::FUNCTION_EXISTS, RPNElement::ALWAYS_FALSE});
}

bool MergeTreeIndexConditionJSONBloomFilter::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn) const
{
    auto * bloom_granule = typeid_cast<MergeTreeIndexGranuleJSONBloomFilter *>(granule.get());
    if (!bloom_granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`jsonbf_v1` received an incompatible granule");
    /// Granules built in memory, such as those evaluated by `EXPLAIN WHATIF`, have not been through deserialization.
    if (bloom_granule->isBuilt())
    {
        bloom_granule->materialize();
        if (has_dynamic_probes)
            prepareDynamicProbes(*bloom_granule);
    }

    return evaluateGranule(*bloom_granule, update_partial_result_disjunction_fn, true);
}

bool MergeTreeIndexConditionJSONBloomFilter::mayBeTrueOnGranule(
    const MergeTreeIndexGranuleJSONBloomFilter & granule, bool pending_matches) const
{
    return evaluateGranule(granule, {}, pending_matches);
}

bool MergeTreeIndexConditionJSONBloomFilter::evaluateGranule(
    const MergeTreeIndexGranuleJSONBloomFilter & granule,
    const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn,
    bool pending_matches) const
{
    const auto & part_path_matcher = granule.getPathMatcher();
    /// A part whose index this server cannot read, or that does not index the path, can hold any value for it.
    const auto is_unknown = [&](const String & path) { return granule.isUnsupported() || !part_path_matcher.shouldIndex(path); };
    PODArrayWithStackMemory<BoolMask, 64> stack;
    size_t element_index = 0;
    for (const auto & element : rpn)
    {
        bool element_is_unknown = element.function == RPNElement::FUNCTION_UNKNOWN;
        switch (element.function)
        {
            case RPNElement::FUNCTION_UNKNOWN: stack.emplace_back(true, true); break;
            case RPNElement::FUNCTION_EXISTS: {
                /// A path the part does not index can be present in any granule.
                const auto may_exist = [&](const String & path) { return is_unknown(path) || granule.hasPath(path); };
                element_is_unknown = element.exists_all ? std::ranges::all_of(element.exists_paths, is_unknown)
                                                        : std::ranges::any_of(element.exists_paths, is_unknown);
                stack.emplace_back(
                    element.exists_all ? std::ranges::all_of(element.exists_paths, may_exist)
                                       : std::ranges::any_of(element.exists_paths, may_exist),
                    true);
                break;
            }
            case RPNElement::FUNCTION_ANY: {
                if (is_unknown(element.path))
                {
                    element_is_unknown = true;
                    stack.emplace_back(true, true);
                    break;
                }
                const bool matches = std::ranges::any_of(
                    element.hashes, [&](const auto & probe) { return granule.matches(element.path, probe, pending_matches); });
                stack.emplace_back(matches, true);
                break;
            }
            case RPNElement::FUNCTION_ALL:
                if (is_unknown(element.path))
                {
                    element_is_unknown = true;
                    stack.emplace_back(true, true);
                    break;
                }
                if (!element.alternatives.empty())
                {
                    stack.emplace_back(
                        std::ranges::all_of(
                            element.alternatives,
                            [&](const auto & alternative)
                            {
                                return !alternative.empty()
                                    && std::ranges::any_of(
                                        alternative, [&](const auto & probe) { return granule.matches(element.path, probe, pending_matches); });
                            }),
                        true);
                }
                else
                {
                    stack.emplace_back(
                        !element.hashes.empty()
                            && std::ranges::all_of(
                                element.hashes, [&](const auto & probe) { return granule.matches(element.path, probe, pending_matches); }),
                        true);
                }
                break;
            case RPNElement::FUNCTION_NOT: stack.back() = !stack.back(); break;
            case RPNElement::FUNCTION_AND: {
                const auto right = stack.back();
                stack.pop_back();
                stack.back() = stack.back() & right;
                break;
            }
            case RPNElement::FUNCTION_OR: {
                const auto right = stack.back();
                stack.pop_back();
                stack.back() = stack.back() | right;
                break;
            }
            case RPNElement::ALWAYS_FALSE: stack.emplace_back(false, true); break;
            case RPNElement::ALWAYS_TRUE: stack.emplace_back(true, false); break;
        }

        if (update_partial_result_disjunction_fn)
        {
            update_partial_result_disjunction_fn(element_index, stack.back().can_be_true, element_is_unknown);
            ++element_index;
        }
    }

    if (stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected RPN stack size for `jsonbf_v1`");
    return stack.front().can_be_true;
}

bool MergeTreeIndexConditionJSONBloomFilter::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    Field constant;
    DataTypePtr constant_type;
    if (node.tryGetConstant(constant, constant_type))
    {
        if (constant.getType() == Field::Types::UInt64)
        {
            out.function = constant.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        if (constant.getType() == Field::Types::Int64)
        {
            out.function = constant.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }

    if (!node.isFunction())
        return false;

    const auto function = node.toFunctionNode();
    const String function_name = function.getFunctionName();

    /// Path presence covers every predicate that a `bloom_filter` index over `JSONAllPaths` can use.
    const auto set_exists = [&](std::vector<String> paths, bool all)
    {
        if (paths.empty())
            return false;
        out.function = RPNElement::FUNCTION_EXISTS;
        out.exists_paths = std::move(paths);
        out.exists_all = all;
        return true;
    };

    if (function_name == "isNotNull" && function.getArgumentsSize() == 1)
    {
        /// A missing path reads as NULL, and a present path has a directory entry even for complex values.
        const auto argument = function.getArgumentAt(0);
        if (!canContainNull(*argument.getDAGNode()->result_type))
            return false;
        auto path = tryMatchJSONPresencePath(argument, header, columns_shadowing_map_subcolumns, *path_matcher);
        return path && set_exists({std::move(*path)}, false);
    }
    if (function.getArgumentsSize() != 2)
        return false;

    /// `indexOf(JSONAllPaths(json), 'path')` inside a comparison that implies the path is listed.
    for (size_t i = 0; i != 2; ++i)
    {
        const auto argument = function.getArgumentAt(i);
        if (!argument.isFunction())
            continue;
        const auto index_of = argument.toFunctionNode();
        if (index_of.getFunctionName() != "indexOf" || index_of.getArgumentsSize() != 2)
            continue;
        const auto column_name = tryMatchJSONAllPaths(index_of.getArgumentAt(0), header);
        if (!column_name)
            continue;
        if (!indexOfCanUseBloomFilter(&node) || !index_of.getArgumentAt(1).tryGetConstant(constant, constant_type))
            return false;
        auto path = tryMatchJSONAllPathsElement(*column_name, constant, header, *path_matcher);
        return path && set_exists({std::move(*path)}, false);
    }

    /// `has`, `hasAny` and `hasAll` over `JSONAllPaths(json)`.
    if (function_name == "has" || function_name == "hasAny" || function_name == "hasAll")
    {
        if (const auto column_name = tryMatchJSONAllPaths(function.getArgumentAt(0), header))
        {
            if (!function.getArgumentAt(1).tryGetConstant(constant, constant_type))
                return false;
            if (function_name != "has" && constant.getType() != Field::Types::Array)
                return false;
            const Array elements = function_name == "has" ? Array{constant} : constant.safeGet<Array>();
            std::vector<String> paths;
            for (const auto & element : elements)
            {
                auto path = tryMatchJSONAllPathsElement(*column_name, element, header, *path_matcher);
                /// An unmatched element can be listed in any granule, which only `hasAll` can ignore.
                if (!path && function_name != "hasAll")
                    return false;
                if (path)
                    paths.push_back(std::move(*path));
            }
            return set_exists(std::move(paths), function_name == "hasAll");
        }
    }

    if (functionIsInOrGlobalInOperator(function_name))
    {
        if (function_name != "in" && function_name != "globalIn")
            return false;

        auto key_node = function.getArgumentAt(0);
        const auto all_paths_column = tryMatchJSONAllPaths(key_node, header);
        auto path = tryMatchJSONPath(key_node, header, columns_shadowing_map_subcolumns);
        auto presence_path = tryMatchJSONPresencePath(key_node, header, columns_shadowing_map_subcolumns, *path_matcher);
        if (!all_paths_column && !path && !presence_path)
            return false;

        auto future_set = function.getArgumentAt(1).tryGetPreparedSet();
        if (!future_set)
            return false;
        auto prepared_set = future_set->buildOrderedSetInplace(function.getArgumentAt(1).getTreeContext().getQueryContext());
        if (!prepared_set || !prepared_set->hasExplicitSetElements())
            return false;
        const auto set_columns = prepared_set->getSetElements();
        const auto set_types = prepared_set->getElementsTypes();

        if (all_paths_column)
        {
            /// Each set element is a whole path list, so a granule needs at least one path from a non-empty list.
            if (set_columns.size() != 1)
                return false;
            std::vector<String> paths;
            for (size_t row = 0; row != set_columns.front()->size(); ++row)
            {
                Field elements;
                set_columns.front()->get(row, elements);
                if (elements.getType() != Field::Types::Array || elements.safeGet<Array>().empty())
                    return false;
                for (const auto & element : elements.safeGet<Array>())
                {
                    auto element_path = tryMatchJSONAllPathsElement(*all_paths_column, element, header, *path_matcher);
                    if (!element_path)
                        return false;
                    paths.push_back(std::move(*element_path));
                }
            }
            return set_exists(std::move(paths), false);
        }

        if (path && path_matcher->shouldIndex(path->logical_path) && !path->cast_type && !isDynamic(removeJSONBloomWrappers(path->type))
            && set_columns.size() == 1)
        {
            out.path = path->logical_path;
            bool safe = true;
            for (size_t row = 0; safe && row != set_columns.front()->size(); ++row)
            {
                Field value;
                set_columns.front()->get(row, value);
                safe = isJSONBloomPathFilterSafe(
                    key_node.getDAGNode()->result_type, value, set_types.front(), comparison_format_settings, path->indexes_missing_values);
                if (safe)
                {
                    auto probes = makeValueProbes(
                        path->path, path->role, path->type, value, set_types.front(), comparison_format_settings, path->typed_dynamic);
                    out.hashes.insert(out.hashes.end(), probes.begin(), probes.end());
                }
            }
            if (safe && !out.hashes.empty())
            {
                out.function = RPNElement::FUNCTION_ANY;
                return true;
            }
            out.path.clear();
            out.hashes.clear();
        }

        /// Fall back to path presence when a missing path cannot satisfy the condition.
        if (!presence_path)
            return false;
        const auto & key_type = key_node.getDAGNode()->result_type;
        if (!canContainNull(*key_type))
        {
            ColumnsWithTypeAndName default_columns{{key_type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst(), key_type, ""}};
            const auto result = prepared_set->execute(default_columns, false);
            if (assert_cast<const ColumnUInt8 &>(*result).getData()[0])
                return false;
        }
        return set_exists({std::move(*presence_path)}, false);
    }

    auto lhs_node = function.getArgumentAt(0);
    auto rhs_node = function.getArgumentAt(1);
    const RPNBuilderTreeNode * key_node = &lhs_node;
    const RPNBuilderTreeNode * value_node = &rhs_node;
    if (!value_node->tryGetConstant(constant, constant_type))
    {
        if (function_name != "equals" || !key_node->tryGetConstant(constant, constant_type))
            return false;
        std::swap(key_node, value_node);
    }

    if (function_name == "equals")
    {
        /// `arrayJoin(JSONAllPaths(json)) = 'path'` needs the path in the granule.
        if (const auto array_join_argument = key_node->getArrayJoinArgument())
        {
            const auto column_name = tryMatchJSONAllPaths(*array_join_argument, header);
            auto path = column_name ? tryMatchJSONAllPathsElement(*column_name, constant, header, *path_matcher) : std::nullopt;
            return path && set_exists({std::move(*path)}, false);
        }

        auto path = tryMatchJSONPath(*key_node, header, columns_shadowing_map_subcolumns);
        if (path && path_matcher->shouldIndex(path->logical_path)
            && isJSONBloomPathFilterSafe(
                key_node->getDAGNode()->result_type, constant, constant_type, comparison_format_settings, path->indexes_missing_values))
        {
            if (path->cast_type)
                out.hashes = makeDynamicCastProbes(
                    path->path, path->role, path->type, path->cast_type, constant, constant_type, comparison_format_settings);
            else
                out.hashes = makeValueProbes(
                    path->path, path->role, path->type, constant, constant_type, comparison_format_settings, path->typed_dynamic);
            if (!out.hashes.empty())
            {
                out.path = path->logical_path;
                out.function = RPNElement::FUNCTION_ANY;
                return true;
            }
        }

        /// Fall back to path presence when a missing path cannot satisfy the condition.
        auto presence_path = tryMatchJSONPresencePath(*key_node, header, columns_shadowing_map_subcolumns, *path_matcher);
        if (!presence_path
            || !isJSONBloomPathFilterSafe(key_node->getDAGNode()->result_type, constant, constant_type, comparison_format_settings, false))
            return false;
        return set_exists({std::move(*presence_path)}, false);
    }

    auto path = tryMatchJSONPath(*key_node, header, columns_shadowing_map_subcolumns);
    if (!path || !path_matcher->shouldIndex(path->logical_path))
        return false;
    out.path = path->logical_path;

    if (function_name == "has" || function_name == "hasAny" || function_name == "hasAll")
    {
        if (path->cast_type)
            return false;
        const auto * array_type = typeid_cast<const DataTypeArray *>(removeJSONBloomWrappers(path->type).get());
        if (!array_type)
            return false;

        path->role = JSONBloomRole::ArrayElement;
        if (function_name == "has")
        {
            out.hashes = makeArrayElementProbes(
                path->path, path->role, array_type->getNestedType(), constant, constant_type, comparison_format_settings);
            if (out.hashes.empty())
                return false;
            out.function = typeid_cast<const DataTypeArray *>(removeJSONBloomWrappers(array_type->getNestedType()).get())
                ? RPNElement::FUNCTION_ALL
                : RPNElement::FUNCTION_ANY;
            return true;
        }

        if (constant.getType() != Field::Types::Array || constant.safeGet<Array>().empty())
            return false;

        const auto * constant_array_type = typeid_cast<const DataTypeArray *>(constant_type.get());
        if (!constant_array_type)
            return false;

        for (const auto & value : constant.safeGet<Array>())
        {
            auto probes = makeArrayElementProbes(
                path->path,
                path->role,
                array_type->getNestedType(),
                value,
                constant_array_type->getNestedType(),
                comparison_format_settings);
            if (probes.empty())
                return false;

            if (function_name == "hasAny")
                out.hashes.insert(out.hashes.end(), probes.begin(), probes.end());
            else
                out.alternatives.emplace_back(std::move(probes));
        }

        out.function = function_name == "hasAny" ? RPNElement::FUNCTION_ANY : RPNElement::FUNCTION_ALL;
        return function_name == "hasAny" ? !out.hashes.empty() : !out.alternatives.empty();
    }

    return false;
}

MergeTreeIndexJSONBloomFilter::MergeTreeIndexJSONBloomFilter(
    StorageMetadataPtr metadata_snapshot_,
    const IndexDescription & index_,
    size_t bits_per_row_,
    size_t hash_functions_,
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
    , path_matcher(std::move(path_matcher_))
{
}

NameSet MergeTreeIndexJSONBloomFilter::getColumnsShadowingJSONSubcolumns() const
{
    /// Another column, or a subcolumn of one, can have the name of a subcolumn of the indexed column, such as a
    /// column named `json.x`. A predicate on that name reads the other column, so it must not use this index.
    auto result = getColumnsShadowingMapSubcolumns();
    const auto & json_column = index.column_names.front();
    const auto prefix = json_column + ".";
    for (const auto & column : metadata_snapshot->getColumns().get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns()))
        if (column.name.starts_with(prefix) && column.getNameInStorage() != json_column)
            result.insert(column.name);
    return result;
}

MergeTreeIndexGranulePtr MergeTreeIndexJSONBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleJSONBloomFilter>(bits_per_row, hash_functions, path_matcher);
}

MergeTreeIndexGranulePtr MergeTreeIndexJSONBloomFilter::createIndexGranule(const MergeTreeIndexPartMetadataPtr & part_metadata) const
{
    const auto metadata = std::dynamic_pointer_cast<const MergeTreeIndexJSONBloomFilterPartMetadata>(part_metadata);
    if (!metadata)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Missing `jsonbf_v1` part metadata");
    auto granule = std::make_shared<MergeTreeIndexGranuleJSONBloomFilter>(metadata->bits_per_row, metadata->hash_functions, metadata->path_matcher);
    if (!metadata->supported)
        granule->markUnsupported();
    return granule;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexJSONBloomFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorJSONBloomFilter>(
        bits_per_row, hash_functions, index.column_names.front(), index.data_types.front(), path_matcher);
}

MergeTreeIndexConditionPtr MergeTreeIndexJSONBloomFilter::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionJSONBloomFilter>(
        predicate, context, index.sample_block, path_matcher, getColumnsShadowingJSONSubcolumns());
}

namespace
{

constexpr UInt64 JSON_BLOOM_PART_METADATA_VERSION = 1;

void writeStrings(const std::vector<String> & values, WriteBuffer & out)
{
    writeVarUInt(values.size(), out);
    for (const auto & value : values)
        writeStringBinary(value, out);
}

std::vector<String> readStrings(ReadBuffer & in)
{
    UInt64 size = 0;
    readVarUInt(size, in);
    std::vector<String> values(size);
    for (auto & value : values)
        readStringBinary(value, in);
    return values;
}

}

MergeTreeIndexSubstreams MergeTreeIndexJSONBloomFilter::getSubstreams() const
{
    return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"},
            {MergeTreeIndexSubstream::Type::JSONBloomFilterValues, ".values", ".idx2"}};
}

MergeTreeIndexFormat MergeTreeIndexJSONBloomFilter::getPhysicalFormat(
    const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2", &storage))
        return {2, getSubstreams()};
    return {0, {}};
}

MergeTreeIndexSubstreams MergeTreeIndexJSONBloomFilter::getAllSubstreamsInPart(
    const MergeTreeDataPartChecksums & checksums, const std::string & relative_path_prefix, const IDataPartStorage * storage) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2", storage))
        return getSubstreams();
    return {};
}

void MergeTreeIndexJSONBloomFilter::serializePartMetadata(MergeTreeIndexOutputStreams & streams) const
{
    auto & out = streams.at(MergeTreeIndexSubstream::Type::Regular)->compressed_hashing;
    writeVarUInt(JSON_BLOOM_PART_METADATA_VERSION, out);
    writeVarUInt(bits_per_row, out);
    writeVarUInt(hash_functions, out);
    writeStrings(path_matcher->getIncludePaths(), out);
    writeStrings(path_matcher->getIncludePathRegexps(), out);
    writeStrings(path_matcher->getSkipPaths(), out);
    writeStrings(path_matcher->getSkipPathRegexps(), out);
}

MergeTreeIndexPartMetadataPtr MergeTreeIndexJSONBloomFilter::deserializePartMetadata(MergeTreeIndexInputStreams & streams) const
{
    auto & in = *streams.at(MergeTreeIndexSubstream::Type::Regular)->getDataBuffer();
    UInt64 metadata_version = 0;
    UInt64 part_bits_per_row = 0;
    UInt64 part_hash_functions = 0;
    readVarUInt(metadata_version, in);
    if (metadata_version != JSON_BLOOM_PART_METADATA_VERSION)
    {
        /// Written by a newer server. The index is ignored for this part instead of failing its queries.
        auto metadata = std::make_shared<MergeTreeIndexJSONBloomFilterPartMetadata>(bits_per_row, hash_functions, path_matcher);
        metadata->supported = false;
        return metadata;
    }
    readVarUInt(part_bits_per_row, in);
    readVarUInt(part_hash_functions, in);
    if (part_bits_per_row == 0 || part_hash_functions == 0
        || part_hash_functions > std::size(BloomFilterHash::bf_hash_seed))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid `jsonbf_v1` part metadata");

    auto include_paths = readStrings(in);
    auto include_path_regexps = readStrings(in);
    auto skip_paths = readStrings(in);
    auto skip_path_regexps = readStrings(in);
    return std::make_shared<MergeTreeIndexJSONBloomFilterPartMetadata>(
        part_bits_per_row,
        part_hash_functions,
        std::make_shared<JSONBloomPathMatcher>(std::move(include_paths), include_path_regexps, std::move(skip_paths), skip_path_regexps));
}

namespace
{

std::unordered_map<String, ASTPtr> parseJSONBloomOptions(const ASTPtr & arguments)
{
    std::unordered_map<String, ASTPtr> options;
    if (!arguments)
        return options;

    for (const auto & argument : arguments->children)
    {
        const auto * equals = argument->as<ASTFunction>();
        if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` arguments must be named");

        const auto * name = equals->arguments->children[0]->as<ASTIdentifier>();
        if (!name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` argument name must be an identifier");
        if (!options.emplace(name->name(), equals->arguments->children[1]).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` argument `{}` is specified more than once", name->name());
    }
    return options;
}

std::vector<String> extractStringArrayOption(std::unordered_map<String, ASTPtr> & options, std::string_view name)
{
    const auto it = options.find(String(name));
    if (it == options.end())
        return {};

    const Field value = getFieldFromIndexArgumentAST(it->second);
    if (value.getType() != Field::Types::Array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` argument `{}` must be an array of strings", name);

    std::vector<String> result;
    result.reserve(value.safeGet<Array>().size());
    for (const auto & element : value.safeGet<Array>())
    {
        if (element.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` argument `{}` must be an array of strings", name);
        result.push_back(element.safeGet<String>());
    }

    options.erase(it);
    return result;
}

struct JSONBloomOptions
{
    Float64 false_positive_rate;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
};

JSONBloomOptions getJSONBloomOptions(const IndexDescription & index)
{
    auto options = parseJSONBloomOptions(index.arguments);
    Float64 false_positive_rate = 0.025;

    if (auto it = options.find("false_positive_rate"); it != options.end())
    {
        const Field value = getFieldFromIndexArgumentAST(it->second);
        if (value.getType() != Field::Types::Float64 || value.safeGet<Float64>() < 0 || value.safeGet<Float64>() > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` argument `false_positive_rate` must be a `Float64` between 0 and 1");
        false_positive_rate = value.safeGet<Float64>();
        options.erase(it);
    }

    auto include_paths = extractStringArrayOption(options, "include_paths");
    if (std::ranges::any_of(include_paths, [](const auto & path) { return path.empty(); }))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` argument `include_paths` cannot contain an empty path");
    auto include_paths_regexp = extractStringArrayOption(options, "include_paths_regexp");
    auto skip_paths = extractStringArrayOption(options, "skip_paths");
    if (std::ranges::any_of(skip_paths, [](const auto & path) { return path.empty(); }))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` argument `skip_paths` cannot contain an empty path");
    auto skip_paths_regexp = extractStringArrayOption(options, "skip_paths_regexp");

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected `jsonbf_v1` argument `{}`", options.begin()->first);
    return {
        false_positive_rate,
        std::make_shared<JSONBloomPathMatcher>(std::move(include_paths), include_paths_regexp, std::move(skip_paths), skip_paths_regexp)};
}

}

MergeTreeIndexPtr
jsonBloomFilterIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings &)
{
    auto options = getJSONBloomOptions(index);
    const auto [bits_per_row, hash_functions] = BloomFilterHash::calculationBestPractices(options.false_positive_rate);
    return std::make_shared<MergeTreeIndexJSONBloomFilter>(
        std::move(metadata_snapshot), index, bits_per_row, hash_functions, std::move(options.path_matcher));
}

void jsonBloomFilterIndexValidator(const IndexDescription & index, bool, const MergeTreeSettings &)
{
    getJSONBloomOptions(index);
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "`jsonbf_v1` must be created on one direct `JSON` column");
    if (!index.isSimpleSingleColumnIndex())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` must be created on a direct `JSON` column");
    if (!isObject(index.data_types.front()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "`jsonbf_v1` must be created on a direct `JSON` column, got `{}`",
            index.data_types.front()->getName());
}


}
