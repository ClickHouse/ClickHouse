#include <Storages/MergeTree/MergeTreeIndexJSONBloomFilter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/SipHash.h>
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
#include <Functions/FunctionsComparison.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/BloomFilterHash.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/TokenizerFactory.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/JSONValueEnumerator.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int LOGICAL_ERROR;
}

namespace
{

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
    Ngram = 2,
    AlwaysPresent = 3,
    UnsupportedDynamicType = 4,
    DynamicTypePresence = 5,
    DynamicComplexPresence = 6,
};

UInt64 finishTokenHash(SipHash & hash)
{
    return hash.get64();
}

void updateTokenHash(SipHash & hash, std::string_view value)
{
    const UInt64 size = value.size();
    hash.update(size);
    hash.update(value.data(), value.size());
}

UInt64 hashToken(
    std::string_view path,
    JSONBloomRole role,
    JSONBloomDomain domain,
    std::string_view type,
    std::string_view value)
{
    SipHash hash;
    static constexpr std::string_view namespace_name = "jsonbf_v1";
    updateTokenHash(hash, namespace_name);
    hash.update(static_cast<UInt8>(role));
    hash.update(static_cast<UInt8>(domain));
    updateTokenHash(hash, path);
    updateTokenHash(hash, type);
    updateTokenHash(hash, value);
    return finishTokenHash(hash);
}

UInt64 alwaysPresentHash()
{
    return hashToken({}, JSONBloomRole::Scalar, JSONBloomDomain::AlwaysPresent, {}, {});
}

UInt64 unsupportedDynamicTypeHash()
{
    return hashToken({}, JSONBloomRole::Scalar, JSONBloomDomain::UnsupportedDynamicType, {}, {});
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

    String result(path);
    result.push_back('\0');
    result.push_back('M');
    const String & type_name = key_type->getName();
    const UInt64 type_size = type_name.size();
    result.append(reinterpret_cast<const char *>(&type_size), sizeof(type_size));
    result.append(type_name);
    const String & key = encoded_key.str();
    const UInt64 key_size = key.size();
    result.append(reinterpret_cast<const char *>(&key_size), sizeof(key_size));
    result.append(key);
    return result;
}

class JSONBloomExtractor
{
public:
    JSONBloomExtractor(HashSet<UInt64> & hashes_, TokenizerPtr tokenizer_)
        : hashes(hashes_)
        , tokenizer(tokenizer_)
    {
    }

    void beginRow() {}
    void endRow() {}
    void consumeNull(std::string_view, bool) {}

    void consumeValue(
        std::string_view path,
        const IDataType & data_type,
        std::string_view,
        const ISerialization &,
        const IColumn & source_column,
        size_t row,
        bool is_dynamic,
        const FormatSettings &)
    {
        emitValue(path, JSONBloomRole::Scalar, data_type.getPtr(), source_column, row, is_dynamic);
    }

private:
    class NestedConsumer
    {
    public:
        NestedConsumer(JSONBloomExtractor & extractor_, String prefix_, JSONBloomRole role_)
            : extractor(extractor_)
            , prefix(std::move(prefix_))
            , role(role_)
        {
        }

        void beginRow() {}
        void endRow() {}
        void consumeNull(std::string_view, bool) {}

        void consumeValue(
            std::string_view path,
            const IDataType & data_type,
            std::string_view,
            const ISerialization &,
            const IColumn & source_column,
            size_t row,
            bool is_dynamic,
            const FormatSettings &)
        {
            extractor.emitValue(appendPath(prefix, path), role, data_type.getPtr(), source_column, row, is_dynamic);
        }

    private:
        JSONBloomExtractor & extractor;
        String prefix;
        JSONBloomRole role;
    };

    void emitDynamic(
        std::string_view path,
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
            char type_index = 0;
            if (!buffer.peek(type_index))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parse shared `Dynamic` value in `jsonbf_v1`");

            DataTypePtr type;
            SerializationPtr serialization;
            const auto binary_type_index = static_cast<BinaryTypeIndex>(type_index);
            const auto & cache = getSimpleDataTypesCache();
            if (cache.hasElement(binary_type_index))
            {
                ++buffer.position();
                const auto & element = cache.getElement(binary_type_index);
                type = element.type;
                serialization = element.serialization;
            }
            else
            {
                type = decodeDataType(buffer);
                serialization = type->getDefaultSerialization();
            }

            auto column = type->createColumn();
            serialization->deserializeBinary(*column, buffer, {});
            emitValue(path, role, type, *column, 0, true);
            return;
        }

        const auto & variant_type = assert_cast<const DataTypeVariant &>(*dynamic_column.getVariantInfo().variant_type);
        const auto & type = variant_type.getVariant(discriminator);
        emitValue(
            path,
            role,
            type,
            variant_column.getVariantByGlobalDiscriminator(discriminator),
            variant_row,
            true);
    }

    void emitArray(
        std::string_view path,
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
            emitValue(path, JSONBloomRole::ArrayElement, nested_type, nested_column, element, is_dynamic || isDynamic(nested_type));
    }

    void emitMap(
        std::string_view path,
        const DataTypeMap & map_type,
        const ColumnMap & map_column,
        size_t row,
        bool is_dynamic)
    {
        if (is_dynamic || map_type.getValueType()->hasDynamicStructure())
        {
            hashes.insert(unsupportedDynamicTypeHash());
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
            emitValue(appendMapKey(path, key_type, *full_keys, element), JSONBloomRole::MapValue, value_type, values, element, false);
    }

    void emitTuple(
        std::string_view path,
        const DataTypeTuple & tuple_type,
        const ColumnTuple & tuple_column,
        size_t row,
        bool is_dynamic)
    {
        const auto & element_types = tuple_type.getElements();
        const auto & element_names = tuple_type.getElementNames();
        const auto & columns = tuple_column.getColumns();
        for (size_t i = 0; i != element_types.size(); ++i)
            emitValue(appendPath(path, element_names[i]), JSONBloomRole::Scalar, element_types[i], *columns[i], row, is_dynamic);
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
        {
            hashes.insert(unsupportedDynamicTypeHash());
            return;
        }

        hashes.insert(hashTypedValue(path, role, type, column, row));

        if (tokenizer && WhichDataType(type).isStringOrFixedString())
        {
            const auto value = column.getDataAt(row);
            size_t position = 0;
            size_t token_start = 0;
            size_t token_length = 0;
            while (position < value.size()
                && tokenizer->nextInString(value.data(), value.size(), position, token_start, token_length))
            {
                hashes.insert(hashToken(
                    path,
                    role,
                    JSONBloomDomain::Ngram,
                    {},
                    std::string_view(value.data() + token_start, token_length)));
            }
        }
    }

    void emitValue(
        std::string_view path,
        JSONBloomRole role,
        DataTypePtr type,
        const IColumn & source_column,
        size_t row,
        bool is_dynamic)
    {
        if (isDynamic(type))
        {
            emitDynamic(path, role, assert_cast<const ColumnDynamic &>(source_column), row);
            return;
        }

        auto unwrapped = unwrapColumn(std::move(type), source_column, row);
        if (!unwrapped)
            return;

        type = unwrapped->type;
        const IColumn & column = *unwrapped->column;

        if (is_dynamic
            && (typeid_cast<const DataTypeObject *>(type.get())
                || typeid_cast<const DataTypeArray *>(type.get())
                || typeid_cast<const DataTypeMap *>(type.get())
                || typeid_cast<const DataTypeTuple *>(type.get())
                || type->hasDynamicStructure()))
            hashes.insert(dynamicComplexPresenceHash(path, role));

        if (const auto * object_type = typeid_cast<const DataTypeObject *>(type.get()))
        {
            NestedConsumer consumer(*this, String(path), role);
            enumerateJSONValues<false>(assert_cast<const ColumnObject &>(column), *object_type, consumer, row, 1);
            return;
        }

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            emitArray(path, *array_type, assert_cast<const ColumnArray &>(column), row, is_dynamic);
            return;
        }

        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        {
            emitMap(path, *map_type, assert_cast<const ColumnMap &>(column), row, is_dynamic);
            return;
        }

        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            emitTuple(path, *tuple_type, assert_cast<const ColumnTuple &>(column), row, is_dynamic);
            return;
        }

        const WhichDataType which(type);
        if (which.isNothing())
            return;
        if (which.isVariant() || type->hasDynamicStructure())
        {
            hashes.insert(unsupportedDynamicTypeHash());
            return;
        }

        emitScalar(path, role, type, column, row, is_dynamic);
    }

    HashSet<UInt64> & hashes;
    TokenizerPtr tokenizer;
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
    DataTypePtr type;
    DataTypePtr cast_type;
    JSONBloomRole role = JSONBloomRole::Scalar;
};

bool isStructuralJSONSubcolumn(const DataTypeObject & object_type, const String & path)
{
    const auto delimiter = path.rfind('.');
    if (delimiter == String::npos || object_type.getTypedPaths().contains(path))
        return false;

    const auto last_component = path.substr(delimiter + 1);
    if (last_component != "null"
        && last_component != "keys"
        && last_component != "values"
        && last_component != "items"
        && !last_component.starts_with("key_")
        && !(last_component.starts_with("size")
            && last_component.size() > 4
            && std::ranges::all_of(last_component.substr(4), [](char c) { return c >= '0' && c <= '9'; })))
        return false;

    for (size_t prefix_end = delimiter; prefix_end != String::npos; prefix_end = path.rfind('.', prefix_end - 1))
    {
        const auto typed_path = object_type.getTypedPaths().find(path.substr(0, prefix_end));
        if (typed_path != object_type.getTypedPaths().end()
            && typed_path->second->tryGetSubcolumnType(path.substr(prefix_end + 1)))
            return true;
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
            if (!map_type || map_type->getValueType()->hasDynamicStructure())
                return std::nullopt;

            const auto key_type = removeJSONBloomWrappers(map_type->getKeyType());
            auto key_column = key_type->createColumn();
            ReadBufferFromString buffer(serialized_key);
            key_type->getDefaultSerialization()->deserializeWholeText(*key_column, buffer, {});
            return JSONPathMatch{
                appendMapKey(subcolumn_name, key_type, *key_column, 0),
                map_type->getValueType(),
                nullptr,
                JSONBloomRole::MapValue};
        }

        return std::nullopt;
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
        if (const size_t type_hint = path.find(".:`"); type_hint != String::npos)
        {
            const size_t type_end = path.find('`', type_hint + 3);
            if (type_end == String::npos)
                return std::nullopt;

            if (type_end + 1 == path.size())
                path.resize(type_hint);
            else
            {
                if (path[type_end + 1] != '.' || path.find(".:`", type_end + 2) != String::npos)
                    return std::nullopt;

                const auto hinted_type = DataTypeFactory::instance().get(path.substr(type_hint + 3, type_end - type_hint - 3));
                const auto * array_type = typeid_cast<const DataTypeArray *>(hinted_type.get());
                if (!array_type || !isObject(array_type->getNestedType()))
                    return std::nullopt;
                path.erase(type_hint, type_end - type_hint + 1);
            }
        }

        if (path.empty())
            return std::nullopt;

        if (isStructuralJSONSubcolumn(object_type, path))
            return std::nullopt;

        return JSONPathMatch{std::move(path), subcolumn_type, nullptr, JSONBloomRole::Scalar};
    }

    return std::nullopt;
}

std::optional<JSONPathMatch> tryMatchJSONPath(const RPNBuilderTreeNode & node, const Block & header)
{
    if (auto direct = tryMatchDirectJSONPath(node, header))
        return direct;

    if (!node.isFunction())
        return std::nullopt;

    const auto function = node.toFunctionNode();
    if ((function.getFunctionName() == "CAST" || function.getFunctionName() == "_CAST")
        && function.getArgumentsSize() == 2)
    {
        auto match = tryMatchJSONPath(function.getArgumentAt(0), header);
        const auto * dag_node = node.getDAGNode();
        if (!match || !dag_node)
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

    if (function.getFunctionName() != "arrayElement" || function.getArgumentsSize() != 2)
        return std::nullopt;

    auto map_node = function.getArgumentAt(0);
    auto map_match = tryMatchDirectJSONPath(map_node, header);
    if (!map_match)
        return std::nullopt;

    const auto * map_type = typeid_cast<const DataTypeMap *>(removeJSONBloomWrappers(map_node.getDAGNode()->result_type).get());
    if (!map_type || map_type->getValueType()->hasDynamicStructure())
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
        return hashes;
    }

    if (target_type->hasDynamicStructure()
        || typeid_cast<const DataTypeArray *>(target_type.get())
        || typeid_cast<const DataTypeMap *>(target_type.get())
        || typeid_cast<const DataTypeTuple *>(target_type.get()))
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

void appendDynamicStringPresenceAlternatives(
    std::vector<std::vector<UInt64>> & alternatives,
    std::string_view path,
    JSONBloomRole role)
{
    for (const auto & runtime_type : getDynamicScalarTypes())
    {
        if (!WhichDataType(runtime_type).isStringOrFixedString())
            alternatives.push_back({dynamicTypePresenceHash(path, role, *runtime_type)});
    }
    alternatives.push_back({dynamicComplexPresenceHash(path, role)});
}

std::vector<UInt64> makeNgramProbes(
    std::string_view path,
    JSONBloomRole role,
    const ITokenizer & tokenizer,
    std::string_view value,
    const String & function_name)
{
    VectorWithMemoryTracking<String> tokens;
    if (function_name == "like")
        tokenizer.stringLikeToTokens(value.data(), value.size(), tokens);
    else if (function_name == "startsWith")
        tokenizer.substringToTokens(value.data(), value.size(), tokens, true, false);
    else if (function_name == "endsWith")
        tokenizer.substringToTokens(value.data(), value.size(), tokens, false, true);
    else
        tokenizer.substringToTokens(value.data(), value.size(), tokens, false, false);

    tokens = tokenizer.compactTokens(tokens);
    std::vector<UInt64> result;
    result.reserve(tokens.size());
    for (const auto & token : tokens)
        result.push_back(hashToken(path, role, JSONBloomDomain::Ngram, {}, token));
    return result;
}

}

MergeTreeIndexAggregatorJSONBloomFilter::MergeTreeIndexAggregatorJSONBloomFilter(
    size_t bits_per_row_,
    size_t hash_functions_,
    String column_name_,
    DataTypePtr column_type_,
    TokenizerPtr tokenizer_)
    : bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
    , column_name(std::move(column_name_))
    , column_type(std::move(column_type_))
    , tokenizer(tokenizer_)
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
    JSONBloomExtractor extractor(hashes, tokenizer);
    enumerateJSONValues<false>(*object_column, *object_type, extractor, *pos, rows);

    *pos += rows;
    total_rows += rows;
}

MergeTreeIndexConditionJSONBloomFilter::MergeTreeIndexConditionJSONBloomFilter(
    const ActionsDAG::Node * predicate,
    ContextPtr context,
    const Block & header_,
    size_t hash_functions_,
    TokenizerPtr tokenizer_)
    : header(header_)
    , hash_functions(hash_functions_)
    , tokenizer(tokenizer_)
    , comparison_format_settings(ComparisonParams(context).format_settings)
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
    const auto * bloom_granule = typeid_cast<const MergeTreeIndexGranuleBloomFilter *>(granule.get());
    if (!bloom_granule || bloom_granule->getFilters().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`jsonbf_v1` received an incompatible granule");

    const auto & filter = bloom_granule->getFilters().front();
    if (hashMatchesFilter(filter, unsupportedDynamicTypeHash(), hash_functions))
        return true;

    std::vector<BoolMask> stack;
    size_t element_index = 0;
    for (const auto & element : rpn)
    {
        switch (element.function)
        {
            case RPNElement::FUNCTION_UNKNOWN:
                stack.emplace_back(true, true);
                break;
            case RPNElement::FUNCTION_ANY:
            {
                bool matches = false;
                if (!element.alternatives.empty())
                {
                    matches = std::ranges::any_of(element.alternatives, [&](const auto & alternative)
                    {
                        return !alternative.empty() && std::ranges::all_of(alternative, [&](UInt64 hash)
                        {
                            return hashMatchesFilter(filter, hash, hash_functions);
                        });
                    });
                }
                else
                {
                    matches = std::ranges::any_of(element.hashes, [&](UInt64 hash)
                    {
                        return hashMatchesFilter(filter, hash, hash_functions);
                    });
                }
                stack.emplace_back(matches, true);
                break;
            }
            case RPNElement::FUNCTION_ALL:
                if (!element.alternatives.empty())
                {
                    stack.emplace_back(
                        std::ranges::all_of(element.alternatives, [&](const auto & alternative)
                        {
                            return !alternative.empty() && std::ranges::any_of(alternative, [&](UInt64 hash)
                            {
                                return hashMatchesFilter(filter, hash, hash_functions);
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
                                return hashMatchesFilter(filter, hash, hash_functions);
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
                element.function == RPNElement::FUNCTION_UNKNOWN);
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
        if (!path || isDynamic(removeJSONBloomWrappers(path->type)))
            return false;

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
            if (!isJSONPathFilterSafe(key_node.getDAGNode()->result_type, value, comparison_format_settings))
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
    if (!path)
        return false;

    if (function_name == "equals")
    {
        if (!isJSONPathFilterSafe(key_node->getDAGNode()->result_type, constant, comparison_format_settings))
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
            out.function = RPNElement::FUNCTION_ANY;
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

    if (!tokenizer)
        return false;

    const bool dynamic_string_dispatch = isDynamic(removeJSONBloomWrappers(path->type));
    if (!dynamic_string_dispatch
        && !WhichDataType(removeJSONBloomWrappers(path->type)).isStringOrFixedString())
        return false;
    if (path->cast_type
        && (!dynamic_string_dispatch || !WhichDataType(path->cast_type).isStringOrFixedString()))
        return false;

    if (function_name == "multiSearchAny" && constant.getType() == Field::Types::Array)
    {
        for (const auto & value : constant.safeGet<Array>())
        {
            if (value.getType() != Field::Types::String)
                return false;
            auto probes = makeNgramProbes(path->path, path->role, *tokenizer, value.safeGet<String>(), function_name);
            if (probes.empty())
                return false;
            out.alternatives.emplace_back(std::move(probes));
        }
        if (out.alternatives.empty())
            return false;
        if (dynamic_string_dispatch)
            appendDynamicStringPresenceAlternatives(out.alternatives, path->path, path->role);
        out.function = RPNElement::FUNCTION_ANY;
        return true;
    }

    if (constant.getType() != Field::Types::String)
        return false;

    if (function_name == "like" || function_name == "startsWith" || function_name == "endsWith")
    {
        out.hashes = makeNgramProbes(
            path->path,
            path->role,
            *tokenizer,
            constant.safeGet<String>(),
            function_name);
        if (out.hashes.empty())
            return false;
        if (dynamic_string_dispatch)
        {
            out.alternatives.emplace_back(std::move(out.hashes));
            appendDynamicStringPresenceAlternatives(out.alternatives, path->path, path->role);
            out.function = RPNElement::FUNCTION_ANY;
        }
        else
            out.function = RPNElement::FUNCTION_ALL;
        return true;
    }

    if (function_name == "match")
    {
        const auto analysis = OptimizedRegularExpression::analyze(constant.safeGet<String>());
        if (!analysis.alternatives.empty())
        {
            for (const auto & alternative : analysis.alternatives)
            {
                auto probes = makeNgramProbes(path->path, path->role, *tokenizer, alternative, function_name);
                if (probes.empty())
                    return false;
                out.alternatives.emplace_back(std::move(probes));
            }
            if (out.alternatives.empty())
                return false;
            if (dynamic_string_dispatch)
                appendDynamicStringPresenceAlternatives(out.alternatives, path->path, path->role);
            out.function = RPNElement::FUNCTION_ANY;
            return true;
        }

        if (analysis.required_substring.empty())
            return false;
        out.hashes = makeNgramProbes(path->path, path->role, *tokenizer, analysis.required_substring, function_name);
        if (out.hashes.empty())
            return false;
        if (dynamic_string_dispatch)
        {
            out.alternatives.emplace_back(std::move(out.hashes));
            appendDynamicStringPresenceAlternatives(out.alternatives, path->path, path->role);
            out.function = RPNElement::FUNCTION_ANY;
        }
        else
            out.function = RPNElement::FUNCTION_ALL;
        return true;
    }

    return false;
}

MergeTreeIndexJSONBloomFilter::MergeTreeIndexJSONBloomFilter(
    StorageMetadataPtr metadata_snapshot_,
    const IndexDescription & index_,
    size_t bits_per_row_,
    size_t hash_functions_,
    std::unique_ptr<ITokenizer> tokenizer_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
    , tokenizer(std::move(tokenizer_))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexJSONBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, 1);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexJSONBloomFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorJSONBloomFilter>(
        bits_per_row,
        hash_functions,
        index.column_names.front(),
        index.data_types.front(),
        tokenizer.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexJSONBloomFilter::createIndexCondition(
    const ActionsDAG::Node * predicate,
    ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionJSONBloomFilter>(
        predicate,
        context,
        index.sample_block,
        hash_functions,
        tokenizer.get());
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

struct JSONBloomOptions
{
    Float64 false_positive_rate = 0.025;
    std::unique_ptr<ITokenizer> tokenizer;
};

JSONBloomOptions getJSONBloomOptions(const IndexDescription & index)
{
    auto options = parseJSONBloomOptions(index.arguments);
    JSONBloomOptions result;

    if (auto it = options.find("false_positive_rate"); it != options.end())
    {
        const Field value = getFieldFromIndexArgumentAST(it->second);
        if (value.getType() != Field::Types::Float64 || value.safeGet<Float64>() < 0 || value.safeGet<Float64>() > 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`jsonbf_v1` argument `false_positive_rate` must be a `Float64` between 0 and 1");
        result.false_positive_rate = value.safeGet<Float64>();
        options.erase(it);
    }

    if (auto it = options.find("tokenizer"); it != options.end())
    {
        result.tokenizer = TokenizerFactory::instance().get(it->second);
        if (result.tokenizer->getType() != ITokenizer::Type::Ngrams)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`jsonbf_v1` only supports the `ngrams` tokenizer");
        options.erase(it);
    }

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected `jsonbf_v1` argument `{}`", options.begin()->first);
    return result;
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
        std::move(metadata_snapshot),
        index,
        bits_per_row,
        hash_functions,
        std::move(options.tokenizer));
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
