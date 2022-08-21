#include "YAMLRegExpTreeDictionarySource.h"

#include <cstdlib>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#if USE_YAML_CPP

#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h>

#endif

#include <base/types.h>

#include <Columns/IColumn.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/iostream_debug_helpers.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>

#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

inline const std::string kYAMLRegExpTreeDictionarySource = "YAMLRegExpTreeDictionarySource";
inline const std::string kYAMLRegExpTree = "YAMLRegExpTree";

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

void registerDictionarySourceYAMLRegExpTree(DictionarySourceFactory & factory)
{
    auto create_table_source = [=]([[maybe_unused]] const DictionaryStructure & dict_struct,
                                   [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                                   [[maybe_unused]] const std::string & config_prefix,
                                   [[maybe_unused]] Block & sample_block,
                                   [[maybe_unused]] ContextPtr global_context,
                                   const std::string &,
                                   [[maybe_unused]] bool created_from_ddl) -> DictionarySourcePtr
    {
#if USE_CASSANDRA
        if (dict_struct.has_expressions)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `{}` does not support attribute expressions", kYAMLRegExpTree);
        }

        const auto filepath = config.getString(config_prefix + ".file.path");

        const auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        return std::make_unique<YAMLRegExpTreeDictionarySource>(filepath, sample_block, context, created_from_ddl);
#else
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `{}` is disabled because ClickHouse was built without yaml_cpp support",
            kYAMLRegExpTree);
#endif
    };

    factory.registerSource(kYAMLRegExpTree, create_table_source);
}

}

#if USE_CASSANDRA

namespace DB
{

/**
*   Sections in YAMLRegExpTreeDictionary
*
*   Example:
*   ```
*   attributes:
*       attr1:
*           ...
*       attr2:
*           ...
*
*   configuration:
*       - match:
*           ...
*   ```
*/
inline const std::string kAttributes = "attributes";
inline const std::string kConfiguration = "configuration";

/**
*   Attributes allowed fields
*
*   Example:
*   ```
*   attr1:
*       type: String
*       default: "demogorgon"   # optional field
*   attr2:
*       type: Float
*   ```
*/
inline const std::string kType = "type";
inline const std::string kDefault = "default";

/**
    *   Allowed data types
    */
inline const std::string kUInt = "UInt";
inline const std::string kInt = "Int";
inline const std::string kFloat = "Float";
inline const std::string kString = "String";

/**
*   Configuration allowed fields
*
*   Example:
*   ```
*   - match:
*       regexp: "MSIE (\\d+)"
*       set:
*           attr1: "Vecna"
*           attr2: 22.8
*       match:  # nested match for subpattern
*           regexp: "Windows"
*           ...
*   ```
*/
inline const std::string kMatch = "match";
inline const std::string kRegExp = "regexp";
inline const std::string kSet = "set";

/**
*   The data can be loaded from table (using any available dictionary source) with the following structure:
*   ```
*   id UInt64,
*   parent_id UInt64,
*   regexp String,
*   attr_1 DataType,
*   ...
*   attr_n DataType
*   ```
*/
inline const std::string kId = "id";
inline const std::string kParentId = "parent_id";

//////////////////////////////////////////////////////////////////////

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_YAML;
    extern const int INVALID_REGEXP_TREE_CONFIGURATION;
    extern const int PATH_ACCESS_DENIED;
}

//////////////////////////////////////////////////////////////////////

class Attribute
{
public:
    std::string name;
    DataTypePtr data_type;

    MutableColumnPtr column;
    std::optional<std::string> default_value;

    explicit Attribute(const std::string & name_, const DataTypePtr & data_type_, const std::optional<std::string> & default_value_)
        : name(name_), data_type(data_type_), column(data_type->createColumn()), default_value(default_value_)
    {
    }

    Attribute(Attribute && other) noexcept
    {
        column = std::move(other.column);
        default_value = std::move(other.default_value);
    }

    virtual void insert(const std::string &) { }

    virtual void insertDefault()
    {
        if (default_value.has_value())
        {
            insert(default_value.value());
        }
        else
        {
            column->insertDefault();
        }
    }

    virtual ~Attribute() = default;
};

class UIntAttribute : public Attribute
{
public:
    explicit UIntAttribute(const std::string & name_, const DataTypePtr & data_type_, const std::optional<std::string> & default_value_)
        : Attribute(name_, data_type_, default_value_)
    {
    }

    void insert(const std::string & value) override { column->insert(Field(fromString(value))); }

private:
    static UInt64 fromString(const std::string & value) { return static_cast<UInt64>(std::strtoul(value.c_str(), nullptr, 10)); }
};

class IntAttribute : public Attribute
{
public:
    explicit IntAttribute(const std::string & name_, const DataTypePtr & data_type_, const std::optional<std::string> & default_value_)
        : Attribute(name_, data_type_, default_value_)
    {
    }

    void insert(const std::string & value) override { column->insert(Field(fromString(value))); }

private:
    static Int64 fromString(const std::string & value) { return static_cast<Int64>(std::strtol(value.c_str(), nullptr, 10)); }
};

class FloatAttribute : public Attribute
{
public:
    explicit FloatAttribute(const std::string & name_, const DataTypePtr & data_type_, const std::optional<std::string> & default_value_)
        : Attribute(name_, data_type_, default_value_)
    {
    }

    void insert(const std::string & value) override { column->insert(Field(fromString(value))); }

private:
    static Float64 fromString(const std::string & value) { return static_cast<Float64>(std::strtof(value.c_str(), nullptr)); }
};

class StringAttribute : public Attribute
{
public:
    explicit StringAttribute(const std::string & name_, const DataTypePtr & data_type_, const std::optional<std::string> & default_value_)
        : Attribute(name_, data_type_, default_value_)
    {
    }

    void insert(const std::string & value) override { column->insert(Field(fromString(value))); }

private:
    static String fromString(const std::string & value) { return value; }
};

//////////////////////////////////////////////////////////////////////

using StringToString = std::unordered_map<std::string, std::string>;
using StringToNode = std::unordered_map<std::string, YAML::Node>;
using StringToAttribute = std::unordered_map<std::string, Attribute>;

YAML::Node loadYAML(const std::string & filepath)
{
    try
    {
        return YAML::LoadFile(filepath);
    }
    catch (const YAML::BadFile & error)
    {
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Failed to open YAML-configuration file {}. Error: {}", filepath, error.what());
    }
    catch (const YAML::ParserException & error)
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "Failed to parse YAML-configuration file {}. Error: {}", filepath, error.what());
    }
}

StringToNode parseYAMLMap(const YAML::Node & node)
{
    StringToNode result;

    for (const auto & pair : node)
    {
        const auto key = pair.first.as<std::string>();
        result[key] = pair.second;
    }

    return result;
}

Attribute makeAttribute(const std::string & name, const std::string & type, const std::optional<std::string> & default_value)
{
    if (type == kUInt)
    {
        return std::move(UIntAttribute(name, std::make_shared<DataTypeUInt64>(), default_value));
    }
    if (type == kInt)
    {
        return std::move(IntAttribute(name, std::make_shared<DataTypeInt64>(), default_value));
    }
    if (type == kFloat)
    {
        return std::move(FloatAttribute(name, std::make_shared<DataTypeFloat64>(), default_value));
    }
    if (type == kString)
    {
        return std::move(StringAttribute(name, std::make_shared<DataTypeString>(), default_value));
    }

    throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Unsupported data type {}", type);
}

Attribute makeAttribute(const std::string & name, const YAML::Node & node)
{
    if (!node.IsMap())
    {
        throw Exception(
            ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION,
            "Invalid structure for attribute {}, expected `{}` and `{}` (optional) mapping",
            name,
            kType,
            kDefault);
    }

    auto attribute_params = parseYAMLMap(node);

    if (!attribute_params.contains(kType))
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Missing type for attribute {}", name);
    }

    std::optional<std::string> default_value;
    if (attribute_params.contains(kDefault))
    {
        const auto default_value_node = attribute_params[kDefault];

        if (!default_value_node.IsScalar())
        {
            throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Value for `default` must be scalar, attribute {}", kDefault);
        }

        default_value = default_value_node.as<std::string>();
    }

    const auto type_node = attribute_params[kType];
    if (!type_node.IsScalar())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Value for `type` must be scalar, attribute {}", name);
    }

    const auto type = type_node.as<std::string>();

    return makeAttribute(name, type, default_value);
}

StringToAttribute getAttributes(const YAML::Node & node)
{
    StringToAttribute result;

    const auto attributes = parseYAMLMap(node);
    for (const auto & [name, value] : attributes)
    {
        result.insert({name, makeAttribute(name, value)});
    }

    return result;
}

void getValuesFromSet(const YAML::Node & node, StringToString & attributes_to_insert)
{
    if (!node.IsMap())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "`` must be mapping", kSet);
    }
    const auto attributes = parseYAMLMap(node);

    for (const auto & [key, value] : attributes)
    {
        if (!value.IsScalar())
        {
            throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Value for attribute {} must be scalar", key);
        }

        attributes_to_insert[key] = value.as<std::string>();
    }
}

void insertValues(StringToString & attributes_to_insert, StringToAttribute & names_to_attributes)
{
    for (auto & [attribute_name, attribute] : names_to_attributes)
    {
        if (attributes_to_insert.contains(attribute_name))
        {
            attribute.insert(attributes_to_insert[attribute_name]);
        }
        else
        {
            attribute.insertDefault();
        }
    }
}

UInt64 processMatch(UInt64 parent_id, const bool is_root, const YAML::Node & node, StringToAttribute & names_to_attributes)
{
    if (!node.IsMap())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Single `{}` node must be mapping", kMatch);
    }

    auto match = parseYAMLMap(node);

    StringToString attributes_to_insert;

    attributes_to_insert[kParentId] = is_root ? "0" : std::to_string(parent_id);
    attributes_to_insert[kId] = std::to_string(++parent_id);

    if (!match.contains(kRegExp))
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Single `{}` node must contain {}", kMatch, kRegExp);
    }
    const auto regexp_node = match[kRegExp];

    if (!regexp_node.IsScalar())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "`{}` should be a {}", kRegExp, kString);
    }
    attributes_to_insert[kRegExp] = regexp_node.as<std::string>();

    if (match.contains(kSet))
    {
        getValuesFromSet(match[kSet], attributes_to_insert);
    }

    insertValues(attributes_to_insert, names_to_attributes);

    if (!match.contains(kMatch))
    {
        return parent_id;
    }

    return processMatch(parent_id, false, match[kMatch], names_to_attributes);
}

void parseConfiguration(const YAML::Node & node, StringToAttribute & names_to_attributes)
{
    names_to_attributes.insert({kId, UIntAttribute(kId, std::make_shared<DataTypeUInt64>(), std::nullopt)});
    names_to_attributes.insert({kParentId, UIntAttribute(kParentId, std::make_shared<DataTypeUInt64>(), std::nullopt)});
    names_to_attributes.insert({kRegExp, StringAttribute(kRegExp, std::make_shared<DataTypeString>(), std::nullopt)});

    if (!node.IsSequence())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Configuration must be sequence of matches");
    }

    UInt64 uid = 0;

    for (const auto & child_node : node)
    {
        if (!child_node.IsMap())
        {
            throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Element of configuration sequence must be mapping", kMatch);
        }

        if (child_node.first.as<std::string>() != kMatch)
        {
            continue;
        }

        uid = processMatch(uid, true, child_node.second, names_to_attributes);
    }
}

Block parseYAMLAsRegExpTree(const YAML::Node & node)
{
    if (!node.IsMap())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Expected `{}` and `{}` mapping", kAttributes, kConfiguration);
    }

    auto regexp_tree = parseYAMLMap(node);

    if (!regexp_tree.contains(kAttributes))
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Missing `{}` section", kAttributes);
    }
    auto names_to_attributes = getAttributes(regexp_tree[kAttributes]);

    if (!regexp_tree.contains(kConfiguration))
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Missing `{}` section", kConfiguration);
    }
    parseConfiguration(regexp_tree[kConfiguration], names_to_attributes);

    ColumnsWithTypeAndName columns;
    columns.reserve(names_to_attributes.size());

    for (auto & [name, attribute] : names_to_attributes)
    {
        auto column = ColumnWithTypeAndName(attribute.column->convertToFullIfNeeded(), attribute.data_type, name);

        columns.push_back(std::move(column));
    }

    return Block(std::move(columns));
}

YAMLRegExpTreeDictionarySource::YAMLRegExpTreeDictionarySource(
    const std::string & filepath_, Block & sample_block_, ContextPtr context_, bool created_from_ddl)
    : filepath(filepath_), sample_block(sample_block_), context(context_), logger(&Poco::Logger::get(kYAMLRegExpTreeDictionarySource))
{
    const auto user_files_path = context->getUserFilesPath();

    if (created_from_ddl && !fileOrSymlinkPathStartsWith(filepath_, user_files_path))
    {
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File {} is not inside {}", filepath_, user_files_path);
    }
}

YAMLRegExpTreeDictionarySource::YAMLRegExpTreeDictionarySource(const YAMLRegExpTreeDictionarySource & other)
    : filepath(other.filepath)
    , sample_block(other.sample_block)
    , context(Context::createCopy(other.context))
    , logger(other.logger)
    , last_modification(other.last_modification)
{
}

QueryPipeline YAMLRegExpTreeDictionarySource::loadAll()
{
    LOG_INFO(logger, "Loading all from {}", filepath);
    last_modification = getLastModification();

    const auto node = loadYAML(filepath);

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(parseYAMLAsRegExpTree(node)));
}

bool YAMLRegExpTreeDictionarySource::isModified() const
{
    /**
    *   We can't count on that the mtime increases or that it has
    *   a particular relation to system time, so just check for strict
    *   equality.
    */
    return getLastModification() != last_modification;
}

std::string YAMLRegExpTreeDictionarySource::toString() const
{
    return fmt::format("File: {}", filepath);
}

Poco::Timestamp YAMLRegExpTreeDictionarySource::getLastModification() const
{
    return FS::getModificationTimestamp(filepath);
}

}

#endif
