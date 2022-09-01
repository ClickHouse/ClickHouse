#include "YAMLRegExpTreeDictionarySource.h"

#include <cstdlib>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#if USE_YAML_CPP

#    include <yaml-cpp/exceptions.h>
#    include <yaml-cpp/node/node.h>
#    include <yaml-cpp/node/parse.h>
#    include <yaml-cpp/yaml.h>

#endif

#include <base/types.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <DataTypes/DataTypeNullable.h>
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
#if USE_YAML_CPP
        if (dict_struct.has_expressions)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `{}` does not support attribute expressions", kYAMLRegExpTree);
        }

        const auto filepath = config.getString(config_prefix + ".YAMLRegExpTree.path");

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

#if USE_YAML_CPP

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
*   attr2:
*       type: Float
*   ```
*/

inline const std::string kType = "type";

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

Field toUInt(const std::string & value)
{
    const auto result = static_cast<UInt64>(std::strtoul(value.c_str(), nullptr, 10));
    return Field(result);
}

Field toInt(const std::string & value)
{
    const auto result = static_cast<Int64>(std::strtol(value.c_str(), nullptr, 10));
    return Field(result);
}

Field toFloat(const std::string & value)
{
    const auto result = static_cast<Float64>(std::strtof(value.c_str(), nullptr));
    return Field(result);
}

Field toString(const std::string & value)
{
    return Field(value);
}

class Attribute
{
public:
    explicit Attribute(
        const std::string & name_, const bool is_nullable_, const DataTypePtr & data_type_, const std::string & attribute_type_)
        : name(name_)
        , is_nullable(is_nullable_)
        , data_type(data_type_)
        , attribute_type(attribute_type_)
        , column_ptr(data_type_->createColumn())
    {
    }

    void insert(const std::string & value)
    {
        if (attribute_type == kUInt)
        {
            column_ptr->insert(toUInt(value));
        }
        else if (attribute_type == kInt)
        {
            column_ptr->insert(toInt(value));
        }
        else if (attribute_type == kFloat)
        {
            column_ptr->insert(toFloat(value));
        }
        else if (attribute_type == kString)
        {
            column_ptr->insert(toString(value));
        }
        else
        {
            throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Unknown type for attribute {}", attribute_type);
        }
    }

    void insertNull()
    {
        if (!is_nullable)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying insert null to not nullable column {} of type {}", name, attribute_type);
        }

        null_indices.insert(column_ptr->size());
        column_ptr->insertDefault();
    }

    std::string getName() const { return name; }

    DataTypePtr getDataType() const
    {
        if (isNullable())
        {
            return makeNullable(data_type);
        }
        return data_type;
    }

    ColumnPtr getColumn()
    {
        auto result = column_ptr->convertToFullIfNeeded();

        if (isNullable())
        {
            auto null_mask = ColumnUInt8::create(column_ptr->size(), false);
            for (size_t i = 0; i < column_ptr->size(); ++i)
            {
                null_mask->getData()[i] = null_indices.contains(i);
            }

            return ColumnNullable::create(result, std::move(null_mask));
        }

        return result;
    }

private:
    std::string name;

    bool is_nullable;
    std::unordered_set<UInt64> null_indices;

    DataTypePtr data_type;
    std::string attribute_type;

    MutableColumnPtr column_ptr;

    bool isNullable() const { return is_nullable && !null_indices.empty(); }
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

Attribute makeAttribute(const std::string & name, const std::string & type)
{
    DataTypePtr data_type;

    if (type == kUInt)
    {
        data_type = std::make_shared<DataTypeUInt64>();
    }
    else if (type == kInt)
    {
        data_type = std::make_shared<DataTypeInt64>();
    }
    else if (type == kFloat)
    {
        data_type = std::make_shared<DataTypeFloat64>();
    }
    else if (type == kString)
    {
        data_type = std::make_shared<DataTypeString>();
    }
    else
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Unknown type for attribute {}", type);
    }

    return Attribute(name, true, data_type, type);
}

Attribute makeAttribute(const std::string & name, const YAML::Node & node)
{
    if (!node.IsMap())
    {
        throw Exception(
            ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Invalid structure for attribute {}, expected `{}` mapping", name, kType);
    }

    auto attribute_params = parseYAMLMap(node);

    if (!attribute_params.contains(kType))
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Missing type for attribute {}", name);
    }

    const auto type_node = attribute_params[kType];
    if (!type_node.IsScalar())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Value for `type` must be scalar, attribute {}", name);
    }

    const auto type = type_node.as<std::string>();

    return makeAttribute(name, type);
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
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "`{}` must be mapping", kSet);
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
            attribute.insertNull();
        }
    }
}

UInt64 processMatch(const bool is_root, UInt64 parent_id, const YAML::Node & node, StringToAttribute & names_to_attributes)
{
    if (!node.IsMap())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Single `{}` node must be mapping", kMatch);
    }

    auto match = parseYAMLMap(node);

    StringToString attributes_to_insert;

    if (!is_root)
    {
        attributes_to_insert[kParentId] = std::to_string(parent_id);
    }
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

    return processMatch(false, parent_id, match[kMatch], names_to_attributes);
}

void parseConfiguration(const YAML::Node & node, StringToAttribute & names_to_attributes)
{
    names_to_attributes.insert({kId, Attribute(kId, false, std::make_shared<DataTypeUInt64>(), kUInt)});
    names_to_attributes.insert({kParentId, Attribute(kParentId, true, std::make_shared<DataTypeUInt64>(), kUInt)});
    names_to_attributes.insert({kRegExp, Attribute(kRegExp, false, std::make_shared<DataTypeString>(), kString)});

    if (!node.IsSequence())
    {
        throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Configuration must be sequence of matches");
    }

    UInt64 uid = 0;

    for (const auto & child_node : node)
    {
        if (!child_node.IsMap())
        {
            throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Element of {} configuration sequence must be mapping", kMatch);
        }

        auto match = parseYAMLMap(child_node);

        if (!match.contains(kMatch))
        {
            throw Exception(ErrorCodes::INVALID_REGEXP_TREE_CONFIGURATION, "Match mapping should contain key {}", kMatch);
        }

        uid = processMatch(true, uid, match[kMatch], names_to_attributes);
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
        auto column = ColumnWithTypeAndName(attribute.getColumn(), attribute.getDataType(), attribute.getName());
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
