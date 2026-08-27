#include <Storages/MergeTree/MergeTreeIndexJSONBloomFilter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/re2.h>
#include <Common/transformEndianness.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesCache.h>
#include <DataTypes/NestedUtils.h>
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

#include <iterator>
#include <list>
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
    extern const int CORRUPTED_DATA;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int LOGICAL_ERROR;
}

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
            paths.begin(),
            paths.end(),
            path,
            [](const String & lhs, std::string_view rhs) { return lhs.compare(rhs) < 0; });
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
    unique_regexp_strings.erase(
        std::unique(unique_regexp_strings.begin(), unique_regexp_strings.end()),
        unique_regexp_strings.end());
    for (const auto & regexp_string : unique_regexp_strings)
    {
        regexps.emplace_back(regexp_string);
        if (!regexps.back().ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid `jsonbf_v1` path regexp '{}': {}",
                regexp_string,
                regexps.back().error());
    }
}

bool JSONBloomPathMatcher::shouldIndex(std::string_view path) const
{
    path = path.substr(0, path.find('\0'));
    if (matchesAnyPathOrSubtree(path, skip_paths) || matchesAnyRegexp(path, skip_regexps))
        return false;

    return !hasIncludeFilter()
        || matchesAnyPathOrSubtree(path, include_paths)
        || matchesAnyRegexp(path, include_regexps);
}

bool JSONBloomPathMatcher::shouldVisit(std::string_view path) const
{
    path = path.substr(0, path.find('\0'));
    if (matchesAnyPathOrSubtree(path, skip_paths))
        return false;

    /// An arbitrary include or skip regexp can match a descendant differently from its ancestor.
    return !hasIncludeFilter()
        || matchesAnyPathOrSubtree(path, include_paths)
        || matchesAnyAncestor(path, include_paths)
        || !include_regexps.empty();
}

namespace
{

std::pair<DataTypePtr, SerializationPtr> decodeJSONDataType(
    ReadBuffer & buffer,
    UnorderedMapWithMemoryTracking<String, SerializationPtr> & serializations_cache)
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
        return {element.type, element.serialization};
    }

    auto type = decodeDataType(buffer);
    auto [it, inserted] = serializations_cache.try_emplace(type->getName());
    if (inserted)
        it->second = type->getDefaultSerialization();
    return {std::move(type), it->second};
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
    AlwaysPresent = 2,
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

UInt64 hashToken(
    std::string_view path,
    JSONBloomRole role,
    JSONBloomDomain domain,
    std::string_view type,
    std::string_view value)
{
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

UInt64 alwaysPresentHash()
{
    return hashToken({}, JSONBloomRole::Scalar, JSONBloomDomain::AlwaysPresent, {}, {});
}

UInt64 unsupportedDynamicTypeHash(std::string_view path, JSONBloomRole role)
{
    return hashToken(path, role, JSONBloomDomain::UnsupportedDynamicType, {}, {});
}

UInt64 dynamicTypePresenceHash(std::string_view path, JSONBloomRole role, const IDataType & type)
{
    return hashToken(path, role, JSONBloomDomain::DynamicTypePresence, type.getName(), {});
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

UInt64 hashTypedValue(
    std::string_view path,
    JSONBloomRole role,
    const DataTypePtr & type,
    const IColumn & column,
    size_t row)
{
    WriteBufferFromOwnString value;
    if (WhichDataType(type).isFloat() && column.getFloat64(row) == 0)
    {
        const auto zero = type->createColumnConst(1, Field(0.0))->convertToFullColumnIfConst();
        type->getDefaultSerialization()->serializeBinary(*zero, 0, value, {});
    }
    else
        type->getDefaultSerialization()->serializeBinary(column, row, value, {});
    return hashToken(path, role, JSONBloomDomain::Typed, type->getName(), value.str());
}

const std::vector<DataTypePtr> & getDynamicScalarTypes();

bool isKnownDynamicScalar(const IDataType & type)
{
    return std::ranges::any_of(
        getDynamicScalarTypes(),
        [&](const auto & known_type) { return known_type->equals(type); });
}

String appendPath(std::string_view prefix, std::string_view suffix)
{
    if (prefix.empty())
        return String(suffix);
    if (suffix.empty())
        return String(prefix);
    return Nested::concatenateName(String(prefix), String(suffix));
}

String appendMapKey(
    std::string_view path,
    const DataTypePtr & key_type,
    const IColumn & key_column,
    size_t row)
{
    WriteBufferFromOwnString encoded_key;
    key_type->getDefaultSerialization()->serializeBinary(key_column, row, encoded_key, {});

    WriteBufferFromOwnString result;
    writeString(path, result);
    writeChar('\0', result);
    writeChar('M', result);
    const String & type_name = key_type->getName();
    writeBinaryLittleEndian(static_cast<UInt64>(type_name.size()), result);
    writeString(type_name, result);
    const String & key = encoded_key.str();
    writeBinaryLittleEndian(static_cast<UInt64>(key.size()), result);
    writeString(key, result);
    return result.str();
}

class JSONBloomExtractor
{
public:
    JSONBloomExtractor(HashSet<UInt64> & hashes_, const JSONBloomPathMatcher & path_matcher_)
        : hashes(hashes_)
        , path_matcher(path_matcher_)
    {
    }

    void emitObject(const ColumnObject & column, const DataTypeObject & type, size_t start_row, size_t num_rows)
    {
        emitObject({}, {}, JSONBloomRole::Scalar, column, type, start_row, num_rows);
    }

private:
    void emitObject(
        std::string_view hash_prefix,
        std::string_view logical_prefix,
        JSONBloomRole role,
        const ColumnObject & column_object,
        const DataTypeObject & type_object,
        size_t start_row,
        size_t num_rows)
    {
        struct PathInfo
        {
            std::string_view path;
            DataTypePtr type;
            ColumnPtr owned_column;
            const IColumn * column = nullptr;
            bool is_dynamic = false;
        };

        const auto & typed_path_types = type_object.getTypedPaths();
        const auto & typed_path_columns = column_object.getTypedPaths();
        const auto & dynamic_path_columns = column_object.getDynamicPaths();
        VectorWithMemoryTracking<PathInfo> paths;
        paths.reserve(typed_path_types.size() + dynamic_path_columns.size());

        for (const auto & [path, type] : typed_path_types)
        {
            const auto & column = typed_path_columns.at(path);
            const auto full_type = recursiveRemoveLowCardinality(type);
            const auto full_column = recursiveRemoveLowCardinality(column);
            const auto value_type = removeNullableOrLowCardinalityNullable(full_type);
            const bool is_dynamic = DB::isDynamic(value_type);
            paths.push_back({
                path,
                full_type,
                full_column,
                full_column.get(),
                is_dynamic});
        }

        for (const auto & [path, column] : dynamic_path_columns)
            paths.push_back({path, nullptr, column, column.get(), true});

        UnorderedMapWithMemoryTracking<String, SerializationPtr> object_serializations_cache;
        UnorderedMapWithMemoryTracking<String, MutableColumnPtr> shared_columns_cache;

        const auto & shared_data_offsets = column_object.getSharedDataOffsets();
        const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
        const FormatSettings format_settings;

        auto emit_shared = [&](std::string_view path, std::string_view value_data)
        {
            const auto logical_path = appendPath(logical_prefix, path);
            if (!path_matcher.shouldVisit(logical_path))
                return;

            ReadBufferFromMemory buffer(value_data);
            auto [type, serialization] = decodeJSONDataType(buffer, object_serializations_cache);
            if (isNothing(type))
                return;

            auto [column_it, inserted] = shared_columns_cache.try_emplace(type->getName());
            if (inserted)
                column_it->second = type->createColumn();

            auto & temporary_column = *column_it->second;
            serialization->deserializeBinary(temporary_column, buffer, format_settings);
            emitValue(appendPath(hash_prefix, path), logical_path, role, type, temporary_column, 0, true);
            temporary_column.popBack(1);
        };

        auto emit_path = [&](PathInfo & entry, size_t row)
        {
            const auto logical_path = appendPath(logical_prefix, entry.path);
            if (!path_matcher.shouldVisit(logical_path) || entry.column->isNullAt(row))
                return;

            if (!entry.is_dynamic)
            {
                emitValue(
                    appendPath(hash_prefix, entry.path),
                    logical_path,
                    role,
                    entry.type,
                    *entry.column,
                    row,
                    false);
                return;
            }

            const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*entry.column);
            const auto & variant_column = dynamic_column.getVariantColumn();
            const auto discriminator = variant_column.globalDiscriminatorAt(row);
            const size_t variant_row = variant_column.offsetAt(row);

            if (discriminator == dynamic_column.getSharedVariantDiscriminator())
            {
                emit_shared(entry.path, dynamic_column.getSharedVariant().getDataAt(variant_row));
                return;
            }

            const auto * type
                = assert_cast<const DataTypeVariant &>(*dynamic_column.getVariantInfo().variant_type).getVariant(discriminator).get();
            emitValue(
                appendPath(hash_prefix, entry.path),
                logical_path,
                role,
                type->getPtr(),
                variant_column.getVariantByGlobalDiscriminator(discriminator),
                variant_row,
                true);
        };

        chassert(start_row <= shared_data_offsets.size());
        const size_t end_row = start_row + std::min(num_rows, shared_data_offsets.size() - start_row);
        for (size_t row = start_row; row != end_row; ++row)
        {
            for (auto & path : paths)
                emit_path(path, row);

            const size_t start = shared_data_offsets[static_cast<ssize_t>(row) - 1];
            const size_t end = shared_data_offsets[static_cast<ssize_t>(row)];
            for (size_t shared_index = start; shared_index != end; ++shared_index)
                emit_shared(shared_data_paths->getDataAt(shared_index), shared_data_values->getDataAt(shared_index));
        }
    }

    void emitDynamic(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        const ColumnDynamic & dynamic_column,
        size_t row)
    {
        if (dynamic_column.isNullAt(row))
            return;

        const auto & variant_column = dynamic_column.getVariantColumn();
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        const size_t variant_row = variant_column.offsetAt(row);

        if (discriminator == dynamic_column.getSharedVariantDiscriminator())
        {
            ReadBufferFromMemory buffer(dynamic_column.getSharedVariant().getDataAt(variant_row));
            auto [type, serialization] = decodeJSONDataType(buffer, serializations_cache);

            auto column = type->createColumn();
            serialization->deserializeBinary(*column, buffer, {});
            emitValue(hash_path, logical_path, role, type, *column, 0, true);
            return;
        }

        const auto & variant_type = assert_cast<const DataTypeVariant &>(*dynamic_column.getVariantInfo().variant_type);
        const auto & type = variant_type.getVariant(discriminator);
        emitValue(
            hash_path,
            logical_path,
            role,
            type,
            variant_column.getVariantByGlobalDiscriminator(discriminator),
            variant_row,
            true);
    }

    void emitArray(
        std::string_view hash_path,
        std::string_view logical_path,
        const DataTypeArray & array_type,
        const ColumnArray & array_column,
        size_t row,
        bool is_dynamic)
    {
        const auto & nested_type = array_type.getNestedType();
        const auto & nested_column = array_column.getData();
        const auto & offsets = array_column.getOffsets();
        const size_t begin = offsets[static_cast<ssize_t>(row) - 1];
        const size_t end = offsets[row];

        for (size_t element = begin; element != end; ++element)
            emitValue(
                hash_path,
                logical_path,
                JSONBloomRole::ArrayElement,
                nested_type,
                nested_column,
                element,
                is_dynamic || isDynamic(nested_type));
    }

    void emitMap(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        const DataTypeMap & map_type,
        const ColumnMap & map_column,
        size_t row,
        bool is_dynamic)
    {
        if (is_dynamic)
        {
            hashes.insert(unsupportedDynamicTypeHash(hash_path, role));
            return;
        }

        const auto key_type = removeJSONBloomWrappers(map_type.getKeyType());
        const auto & value_type = map_type.getValueType();
        const auto & tuple = map_column.getNestedData();
        const auto & keys = tuple.getColumn(0);
        const auto full_keys = keys.convertToFullColumnIfLowCardinality();
        const auto & values = tuple.getColumn(1);
        const auto & offsets = map_column.getNestedColumn().getOffsets();
        const size_t begin = offsets[static_cast<ssize_t>(row) - 1];
        const size_t end = offsets[row];

        for (size_t element = begin; element != end; ++element)
            emitValue(
                appendMapKey(hash_path, key_type, *full_keys, element),
                logical_path,
                JSONBloomRole::MapValue,
                value_type,
                values,
                element,
                false);
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
            emitValue(
                appendPath(hash_path, element_names[i]),
                appendPath(logical_path, element_names[i]),
                role,
                element_types[i],
                *columns[i],
                row,
                is_dynamic);
    }

    void emitScalar(
        std::string_view path,
        JSONBloomRole role,
        const DataTypePtr & type,
        const IColumn & column,
        size_t row,
        bool is_dynamic)
    {
        if (is_dynamic)
            hashes.insert(dynamicTypePresenceHash(path, role, *type));

        if (is_dynamic && !isKnownDynamicScalar(*type))
            hashes.insert(unsupportedDynamicTypeHash(path, role));

        hashes.insert(hashTypedValue(path, role, type, column, row));
    }

    void emitValue(
        std::string_view hash_path,
        std::string_view logical_path,
        JSONBloomRole role,
        DataTypePtr type,
        const IColumn & source_column,
        size_t row,
        bool is_dynamic)
    {
        if (!path_matcher.shouldVisit(logical_path))
            return;

        const bool index_path = path_matcher.shouldIndex(logical_path);

        if (isDynamic(type))
        {
            emitDynamic(hash_path, logical_path, role, assert_cast<const ColumnDynamic &>(source_column), row);
            return;
        }

        auto unwrapped = unwrapColumn(std::move(type), source_column, row);
        if (!unwrapped)
            return;

        type = unwrapped->type;
        const IColumn & column = *unwrapped->column;

        if (!index_path && !hasJSONPathDescendants(type))
            return;

        if (index_path
            && is_dynamic
            && (typeid_cast<const DataTypeObject *>(type.get())
                || typeid_cast<const DataTypeArray *>(type.get())
                || typeid_cast<const DataTypeMap *>(type.get())
                || typeid_cast<const DataTypeTuple *>(type.get())
                || type->hasDynamicStructure()))
            hashes.insert(dynamicComplexPresenceHash(hash_path, role));

        if (const auto * object_type = typeid_cast<const DataTypeObject *>(type.get()))
        {
            emitObject(hash_path, logical_path, role, assert_cast<const ColumnObject &>(column), *object_type, row, 1);
            return;
        }

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            emitArray(hash_path, logical_path, *array_type, assert_cast<const ColumnArray &>(column), row, is_dynamic);
            return;
        }

        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        {
            if (is_dynamic && !index_path)
                return;
            emitMap(hash_path, logical_path, role, *map_type, assert_cast<const ColumnMap &>(column), row, is_dynamic);
            return;
        }

        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            emitTuple(hash_path, logical_path, role, *tuple_type, assert_cast<const ColumnTuple &>(column), row, is_dynamic);
            return;
        }

        if (!index_path)
            return;

        const WhichDataType which(type);
        if (which.isNothing())
            return;
        if (which.isVariant() || type->hasDynamicStructure())
        {
            hashes.insert(unsupportedDynamicTypeHash(hash_path, role));
            return;
        }

        emitScalar(hash_path, role, type, column, row, is_dynamic);
    }

    HashSet<UInt64> & hashes;
    const JSONBloomPathMatcher & path_matcher;
    UnorderedMapWithMemoryTracking<String, SerializationPtr> serializations_cache;
};

bool hashMatchesFilter(const BloomFilterPtr & bloom_filter, UInt64 hash, size_t hash_functions)
{
    return std::all_of(
        BloomFilterHash::bf_hash_seed,
        BloomFilterHash::bf_hash_seed + hash_functions,
        [&](UInt64 seed) { return bloom_filter->findHashWithSeed(hash, seed); });
}

struct JSONPathMatch
{
    String path;
    String logical_path;
    DataTypePtr type;
    DataTypePtr cast_type;
    JSONBloomRole role = JSONBloomRole::Scalar;
    bool indexes_missing_values = false;
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
    const DataTypeObject & object_type,
    const String & path,
    const std::vector<ArrayJSONBridge> & array_json_bridges)
{
    const auto delimiter = path.rfind('.');
    if (delimiter == String::npos || object_type.getTypedPaths().contains(path))
        return false;

    const auto last_component = path.substr(delimiter + 1);
    const bool is_array_size = last_component.starts_with("size")
        && last_component.size() > 4
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
                return is_array_size
                    || last_component == "keys"
                    || last_component == "values"
                    || last_component.starts_with("key_");
            break;
        }
    }

    return false;
}

std::optional<JSONPathMatch> tryMatchDirectJSONPath(const RPNBuilderTreeNode & node, const Block & header)
{
    const auto * dag_node = node.getDAGNode();
    if (!dag_node)
        return std::nullopt;

    if (auto parsed_map_subcolumn = tryParseMapSubcolumnName(node.getColumnName()))
    {
        const auto & [map_column_name, serialized_key] = *parsed_map_subcolumn;
        for (const auto & [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(map_column_name))
        {
            const String column_name_string(column_name);
            if (!header.has(column_name_string) || subcolumn_name.empty())
                continue;

            const auto * object_type = typeid_cast<const DataTypeObject *>(header.getByName(column_name_string).type.get());
            if (!object_type)
                continue;

            const auto subcolumn_type = object_type->tryGetSubcolumnType(String(subcolumn_name));
            const auto unwrapped_subcolumn_type = subcolumn_type ? removeJSONBloomWrappers(subcolumn_type) : nullptr;
            const auto * map_type = typeid_cast<const DataTypeMap *>(unwrapped_subcolumn_type.get());
            if (!map_type)
                continue;
            const auto key_type = removeJSONBloomWrappers(map_type->getKeyType());
            auto key_column = key_type->createColumn();
            ReadBufferFromString buffer(serialized_key);
            key_type->getDefaultSerialization()->deserializeWholeText(*key_column, buffer, {});
            return JSONPathMatch{
                appendMapKey(subcolumn_name, key_type, *key_column, 0),
                String(subcolumn_name),
                map_type->getValueType(),
                nullptr,
                JSONBloomRole::MapValue};
        }

    }

    for (const auto & [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(node.getColumnName()))
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
        for (size_t type_hint = path.find(".:`"); type_hint != String::npos; type_hint = path.find(".:`", type_hint))
        {
            const size_t type_end = path.find('`', type_hint + 3);
            if (type_end == String::npos)
                return std::nullopt;

            if (type_end + 1 == path.size())
                path.resize(type_hint);
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

        bool indexes_missing_values = object_type.getTypedPaths().contains(path);
        if (!indexes_missing_values && !subcolumn_type->hasDynamicStructure())
        {
            indexes_missing_values = std::ranges::any_of(object_type.getTypedPaths(), [&](const auto & typed_path)
            {
                return path.starts_with(typed_path.first)
                    && path.size() > typed_path.first.size()
                    && path[typed_path.first.size()] == '.';
            });
        }

        String logical_path = path;
        return JSONPathMatch{
            std::move(path),
            std::move(logical_path),
            subcolumn_type,
            nullptr,
            JSONBloomRole::Scalar,
            indexes_missing_values};
    }

    return std::nullopt;
}

std::optional<JSONPathMatch> tryMatchJSONPath(const RPNBuilderTreeNode & node, const Block & header)
{
    if (!node.isFunction())
        return tryMatchDirectJSONPath(node, header);

    const auto function = node.toFunctionNode();
    if ((function.getFunctionName() == "CAST" || function.getFunctionName() == "_CAST")
        && function.getArgumentsSize() == 2)
    {
        auto match = tryMatchJSONPath(function.getArgumentAt(0), header);
        const auto * dag_node = node.getDAGNode();
        if (!match || !dag_node || match->cast_type)
            return std::nullopt;
        match->cast_type = removeJSONBloomWrappers(dag_node->result_type);
        if (typeid_cast<const DataTypeObject *>(match->cast_type.get())
            || typeid_cast<const DataTypeArray *>(match->cast_type.get())
            || typeid_cast<const DataTypeMap *>(match->cast_type.get())
            || typeid_cast<const DataTypeTuple *>(match->cast_type.get())
            || match->cast_type->hasDynamicStructure())
            return std::nullopt;
        return match;
    }

    if (function.getFunctionName() == "tupleElement" && function.getArgumentsSize() == 2)
    {
        auto match = tryMatchJSONPath(function.getArgumentAt(0), header);
        const auto * dag_node = node.getDAGNode();
        Field element;
        DataTypePtr element_type;
        if (!match || !dag_node || !function.getArgumentAt(1).tryGetConstant(element, element_type)
            || element.getType() != Field::Types::String)
            return std::nullopt;

        const String & element_name = element.safeGet<String>();
        const auto parent_type = removeJSONBloomWrappers(match->type);
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(parent_type.get());
        if ((!tuple_type || !tuple_type->hasExplicitNames() || !tuple_type->tryGetPositionByName(element_name))
            && !isObject(parent_type))
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
    auto map_match = tryMatchDirectJSONPath(map_node, header);
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

const std::vector<DataTypePtr> & getDynamicScalarTypes()
{
    static const std::vector<DataTypePtr> types = {
        DataTypeFactory::instance().get("Bool"),
        DataTypeFactory::instance().get("Int64"),
        DataTypeFactory::instance().get("UInt64"),
        DataTypeFactory::instance().get("Float64"),
        DataTypeFactory::instance().get("String"),
        DataTypeFactory::instance().get("Date"),
        DataTypeFactory::instance().get("DateTime"),
        DataTypeFactory::instance().get("DateTime64(9)"),
    };
    return types;
}

bool appendTypedProbe(
    std::vector<UInt64> & hashes,
    std::string_view path,
    JSONBloomRole role,
    const Field & value,
    const DataTypePtr & source_type,
    DataTypePtr target_type,
    const FormatSettings & format_settings)
{
    target_type = removeJSONBloomWrappers(std::move(target_type));
    const auto converted = tryConvertFieldToType(value, *target_type, source_type.get(), format_settings, /* strict= */ true);
    if (converted.isNull())
        return false;

    auto column = target_type->createColumn();
    column->insert(converted);
    hashes.push_back(hashTypedValue(path, role, target_type, *column, 0));
    return true;
}

bool comparisonUsesExactConversion(const IDataType & left, const IDataType & right)
{
    /// A failed strict conversion proves inequality only inside the numeric comparison domain.
    /// Other type pairs need a presence probe because execution can match or throw.
    const auto is_number = [](const IDataType & type)
    {
        return isBool(type.getPtr()) || isNativeNumber(type) || WhichDataType(type).isDecimal();
    };
    if ((WhichDataType(left).isDecimal() && WhichDataType(right).isNativeFloat())
        || (WhichDataType(right).isDecimal() && WhichDataType(left).isNativeFloat()))
        return false;
    return left.equals(right)
        || (is_number(left) && is_number(right));
}

std::vector<UInt64> makeDynamicCastProbes(
    std::string_view path,
    JSONBloomRole role,
    DataTypePtr source_type,
    DataTypePtr cast_type,
    const Field & value,
    const DataTypePtr & value_type,
    const FormatSettings & format_settings)
{
    source_type = removeJSONBloomWrappers(std::move(source_type));
    cast_type = removeJSONBloomWrappers(std::move(cast_type));
    if (!isDynamic(source_type) || !isKnownDynamicScalar(*cast_type))
        return {};

    std::vector<UInt64> hashes;
    for (const auto & runtime_type : getDynamicScalarTypes())
    {
        if (runtime_type->equals(*cast_type))
        {
            if (comparisonUsesExactConversion(*runtime_type, *removeJSONBloomWrappers(value_type)))
                appendTypedProbe(hashes, path, role, value, value_type, runtime_type, format_settings);
            else
                hashes.push_back(dynamicTypePresenceHash(path, role, *runtime_type));
        }
        else
            hashes.push_back(dynamicTypePresenceHash(path, role, *runtime_type));
    }
    hashes.push_back(dynamicComplexPresenceHash(path, role));
    hashes.push_back(unsupportedDynamicTypeHash(path, role));

    return hashes;
}

std::vector<UInt64> makeValueProbes(
    std::string_view path,
    JSONBloomRole role,
    DataTypePtr target_type,
    const Field & value,
    const DataTypePtr & source_type,
    const FormatSettings & format_settings)
{
    std::vector<UInt64> hashes;
    target_type = removeJSONBloomWrappers(std::move(target_type));

    if (isDynamic(target_type))
    {
        const auto unwrapped_source_type = removeJSONBloomWrappers(source_type);
        for (const auto & dynamic_type : getDynamicScalarTypes())
        {
            if (comparisonUsesExactConversion(*dynamic_type, *unwrapped_source_type))
                appendTypedProbe(hashes, path, role, value, source_type, dynamic_type, format_settings);
            else
                hashes.push_back(dynamicTypePresenceHash(path, role, *dynamic_type));
        }
        hashes.push_back(dynamicComplexPresenceHash(path, role));
        hashes.push_back(unsupportedDynamicTypeHash(path, role));
        return hashes;
    }

    if (target_type->hasDynamicStructure()
        || typeid_cast<const DataTypeArray *>(target_type.get())
        || typeid_cast<const DataTypeMap *>(target_type.get())
        || typeid_cast<const DataTypeTuple *>(target_type.get())
        || typeid_cast<const DataTypeVariant *>(target_type.get()))
        return hashes;

    const auto unwrapped_source_type = removeJSONBloomWrappers(source_type);
    if (!comparisonUsesExactConversion(*target_type, *unwrapped_source_type)
        && !WhichDataType(unwrapped_source_type).isStringOrFixedString())
        return hashes;

    appendTypedProbe(hashes, path, role, value, source_type, target_type, format_settings);

    std::ranges::sort(hashes);
    hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
    return hashes;
}

std::vector<UInt64> makeArrayElementProbes(
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

        std::vector<UInt64> hashes;
        for (const auto & element : value.safeGet<Array>())
        {
            auto element_hashes = makeArrayElementProbes(
                path,
                role,
                target_array_type->getNestedType(),
                element,
                source_array_type->getNestedType(),
                format_settings);
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
    if (WhichDataType(source_type).isNothing()
        || source_type->hasDynamicStructure()
        || typeid_cast<const DataTypeArray *>(source_type.get())
        || typeid_cast<const DataTypeMap *>(source_type.get())
        || typeid_cast<const DataTypeTuple *>(source_type.get()))
        return {};

    std::vector<UInt64> hashes;
    /// `Array(Dynamic)` membership compares the literal's runtime variant directly.
    /// Other element variants cannot match and do not throw, so they need no presence probes.
    appendTypedProbe(hashes, path, role, value, source_type, source_type, format_settings);
    return hashes;
}

}

MergeTreeIndexGranuleJSONBloomFilter::MergeTreeIndexGranuleJSONBloomFilter(
    size_t bits_per_row_,
    size_t hash_functions_,
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher_)
    : MergeTreeIndexGranuleBloomFilter(bits_per_row_, hash_functions_, 1)
    , hash_functions(hash_functions_)
    , path_matcher(std::move(path_matcher_))
{
}

void MergeTreeIndexGranuleJSONBloomFilter::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown `jsonbf_v1` index version {}", version);
    MergeTreeIndexGranuleBloomFilter::deserializeBinary(istr, 1);
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
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorJSONBloomFilter::getGranuleAndReset()
{
    std::vector<HashSet<UInt64>> column_hashes;
    column_hashes.emplace_back(std::move(hashes));
    auto granule = std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, column_hashes);
    hashes = HashSet<UInt64>{};
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

    /// `MergeTreeIndexGranuleBloomFilter` cannot serialize a filter with zero hashes.
    hashes.insert(alwaysPresentHash());
    JSONBloomExtractor extractor(hashes, *path_matcher);
    extractor.emitObject(*object_column, *object_type, *pos, rows);

    *pos += rows;
    total_rows += rows;
}

MergeTreeIndexConditionJSONBloomFilter::MergeTreeIndexConditionJSONBloomFilter(
    const ActionsDAG::Node * predicate,
    ContextPtr context,
    const Block & header_,
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher_)
    : header(header_)
    , path_matcher(std::move(path_matcher_))
    , comparison_format_settings(getJSONComparisonFormatSettings(context))
{
    if (!predicate)
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        predicate,
        context,
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

bool MergeTreeIndexConditionJSONBloomFilter::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        rpn,
        {RPNElement::FUNCTION_ANY, RPNElement::FUNCTION_ALL, RPNElement::ALWAYS_FALSE});
}

bool MergeTreeIndexConditionJSONBloomFilter::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr granule,
    const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn) const
{
    const auto * bloom_granule = typeid_cast<const MergeTreeIndexGranuleJSONBloomFilter *>(granule.get());
    if (!bloom_granule || bloom_granule->getFilters().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`jsonbf_v1` received an incompatible granule");

    const auto & filter = bloom_granule->getFilters().front();
    const auto & part_path_matcher = bloom_granule->getPathMatcher();
    const size_t part_hash_functions = bloom_granule->getHashFunctions();
    std::vector<BoolMask> stack;
    size_t element_index = 0;
    for (const auto & element : rpn)
    {
        bool element_is_unknown = element.function == RPNElement::FUNCTION_UNKNOWN;
        switch (element.function)
        {
            case RPNElement::FUNCTION_UNKNOWN:
                stack.emplace_back(true, true);
                break;
            case RPNElement::FUNCTION_ANY:
            {
                if (!part_path_matcher.shouldIndex(element.path))
                {
                    element_is_unknown = true;
                    stack.emplace_back(true, true);
                    break;
                }
                const bool matches = std::ranges::any_of(element.hashes, [&](UInt64 hash)
                {
                    return hashMatchesFilter(filter, hash, part_hash_functions);
                });
                stack.emplace_back(matches, true);
                break;
            }
            case RPNElement::FUNCTION_ALL:
                if (!part_path_matcher.shouldIndex(element.path))
                {
                    element_is_unknown = true;
                    stack.emplace_back(true, true);
                    break;
                }
                if (!element.alternatives.empty())
                {
                    stack.emplace_back(
                        std::ranges::all_of(element.alternatives, [&](const auto & alternative)
                        {
                            return !alternative.empty() && std::ranges::any_of(alternative, [&](UInt64 hash)
                            {
                                return hashMatchesFilter(filter, hash, part_hash_functions);
                            });
                        }),
                        true);
                }
                else
                {
                    stack.emplace_back(
                        !element.hashes.empty()
                            && std::ranges::all_of(element.hashes, [&](UInt64 hash)
                            {
                                return hashMatchesFilter(filter, hash, part_hash_functions);
                            }),
                        true);
                }
                break;
            case RPNElement::FUNCTION_NOT:
                stack.back() = !stack.back();
                break;
            case RPNElement::FUNCTION_AND:
            {
                const auto right = stack.back();
                stack.pop_back();
                stack.back() = stack.back() & right;
                break;
            }
            case RPNElement::FUNCTION_OR:
            {
                const auto right = stack.back();
                stack.pop_back();
                stack.back() = stack.back() | right;
                break;
            }
            case RPNElement::ALWAYS_FALSE:
                stack.emplace_back(false, true);
                break;
            case RPNElement::ALWAYS_TRUE:
                stack.emplace_back(true, false);
                break;
        }

        if (update_partial_result_disjunction_fn)
        {
            update_partial_result_disjunction_fn(
                element_index,
                stack.back().can_be_true,
                element_is_unknown);
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
    if (function.getArgumentsSize() != 2)
        return false;

    if (functionIsInOrGlobalInOperator(function_name))
    {
        if (function_name != "in" && function_name != "globalIn")
            return false;

        auto key_node = function.getArgumentAt(0);
        auto path = tryMatchJSONPath(key_node, header);
        if (!path || !path_matcher->shouldIndex(path->logical_path) || path->cast_type || isDynamic(removeJSONBloomWrappers(path->type)))
            return false;
        out.path = path->logical_path;

        auto future_set = function.getArgumentAt(1).tryGetPreparedSet();
        if (!future_set)
            return false;
        auto prepared_set = future_set->buildOrderedSetInplace(function.getArgumentAt(1).getTreeContext().getQueryContext());
        if (!prepared_set || !prepared_set->hasExplicitSetElements() || prepared_set->getSetElements().size() != 1)
            return false;

        const auto set_columns = prepared_set->getSetElements();
        const auto set_types = prepared_set->getElementsTypes();
        const auto & set_column = set_columns.front();
        const auto & set_type = set_types.front();
        for (size_t row = 0; row != set_column->size(); ++row)
        {
            Field value;
            set_column->get(row, value);
            if (!isJSONPathFilterSafe(
                    key_node.getDAGNode()->result_type,
                    value,
                    comparison_format_settings,
                    path->indexes_missing_values))
                return false;
            auto probes = makeValueProbes(path->path, path->role, path->type, value, set_type, comparison_format_settings);
            out.hashes.insert(out.hashes.end(), probes.begin(), probes.end());
        }

        if (out.hashes.empty())
            return false;
        out.function = RPNElement::FUNCTION_ANY;
        return true;
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

    auto path = tryMatchJSONPath(*key_node, header);
    if (!path || !path_matcher->shouldIndex(path->logical_path))
        return false;
    out.path = path->logical_path;

    if (function_name == "equals")
    {
        if (!isJSONPathFilterSafe(
                key_node->getDAGNode()->result_type,
                constant,
                comparison_format_settings,
                path->indexes_missing_values))
            return false;
        if (path->cast_type)
        {
            out.hashes = makeDynamicCastProbes(
                path->path,
                path->role,
                path->type,
                path->cast_type,
                constant,
                constant_type,
                comparison_format_settings);
        }
        else
            out.hashes = makeValueProbes(path->path, path->role, path->type, constant, constant_type, comparison_format_settings);
        if (out.hashes.empty())
            return false;
        out.function = RPNElement::FUNCTION_ANY;
        return true;
    }

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

MergeTreeIndexGranulePtr MergeTreeIndexJSONBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleJSONBloomFilter>(bits_per_row, hash_functions, path_matcher);
}

MergeTreeIndexGranulePtr MergeTreeIndexJSONBloomFilter::createIndexGranule(
    const MergeTreeIndexPartMetadataPtr & part_metadata) const
{
    const auto metadata = std::dynamic_pointer_cast<const MergeTreeIndexJSONBloomFilterPartMetadata>(part_metadata);
    if (!metadata)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Missing `jsonbf_v1` part metadata");
    return std::make_shared<MergeTreeIndexGranuleJSONBloomFilter>(
        metadata->bits_per_row,
        metadata->hash_functions,
        metadata->path_matcher);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexJSONBloomFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorJSONBloomFilter>(
        bits_per_row, hash_functions, index.column_names.front(), index.data_types.front(), path_matcher);
}

MergeTreeIndexConditionPtr MergeTreeIndexJSONBloomFilter::createIndexCondition(
    const ActionsDAG::Node * predicate,
    ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionJSONBloomFilter>(predicate, context, index.sample_block, path_matcher);
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
    return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}};
}

MergeTreeIndexFormat MergeTreeIndexJSONBloomFilter::getPhysicalFormat(
    const IMergeTreeDataPart & part,
    const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(part.checksums, relative_path_prefix, ".idx2", &part.getDataPartStorage()))
        return {2, getSubstreams()};
    return {0, {}};
}

MergeTreeIndexSubstreams MergeTreeIndexJSONBloomFilter::getAllSubstreamsInPart(
    const MergeTreeDataPartChecksums & checksums,
    const std::string & relative_path_prefix,
    const IDataPartStorage * storage) const
{
    MergeTreeIndexSubstreams result;
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2", storage))
        result.push_back({MergeTreeIndexSubstream::Type::Regular, "", ".idx2"});
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx", storage))
        result.push_back({MergeTreeIndexSubstream::Type::Regular, "", ".idx"});
    return result;
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

MergeTreeIndexPartMetadataPtr MergeTreeIndexJSONBloomFilter::deserializePartMetadata(
    MergeTreeIndexInputStreams & streams) const
{
    auto & in = *streams.at(MergeTreeIndexSubstream::Type::Regular)->getDataBuffer();
    UInt64 metadata_version = 0;
    UInt64 part_bits_per_row = 0;
    UInt64 part_hash_functions = 0;
    readVarUInt(metadata_version, in);
    readVarUInt(part_bits_per_row, in);
    readVarUInt(part_hash_functions, in);
    if (metadata_version != JSON_BLOOM_PART_METADATA_VERSION
        || part_bits_per_row == 0
        || part_hash_functions == 0
        || part_hash_functions > std::size(BloomFilterHash::bf_hash_seed))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid `jsonbf_v1` part metadata");

    auto include_paths = readStrings(in);
    auto include_path_regexps = readStrings(in);
    auto skip_paths = readStrings(in);
    auto skip_path_regexps = readStrings(in);
    return std::make_shared<MergeTreeIndexJSONBloomFilterPartMetadata>(
        part_bits_per_row,
        part_hash_functions,
        std::make_shared<JSONBloomPathMatcher>(
            std::move(include_paths),
            include_path_regexps,
            std::move(skip_paths),
            skip_path_regexps));
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
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`jsonbf_v1` argument `false_positive_rate` must be a `Float64` between 0 and 1");
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
        std::make_shared<JSONBloomPathMatcher>(
            std::move(include_paths),
            include_paths_regexp,
            std::move(skip_paths),
            skip_paths_regexp)};
}

}

MergeTreeIndexPtr jsonBloomFilterIndexCreator(
    StorageMetadataPtr metadata_snapshot,
    const IndexDescription & index,
    const MergeTreeSettings &)
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
