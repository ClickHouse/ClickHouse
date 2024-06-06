#include "YAMLRegExpTreeDictionarySource.h"

#include <cstdlib>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <Poco/Logger.h>
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeArray.h"

#if USE_YAML_CPP

#    include <yaml-cpp/exceptions.h>
#    include <yaml-cpp/node/node.h>
#    include <yaml-cpp/node/parse.h>
#    include <yaml-cpp/yaml.h>

#endif

#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <Core/ColumnsWithTypeAndName.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>

#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

inline const String kYAMLRegExpTreeDictionarySource = "YAMLRegExpTreeDictionarySource";
inline const String kYAMLRegExpTree = "yamlregexptree";

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int INCORRECT_DICTIONARY_DEFINITION;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_YAML;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int PATH_ACCESS_DENIED;
}

void registerDictionarySourceYAMLRegExpTree(DictionarySourceFactory & factory)
{
    auto create_table_source = [=]([[maybe_unused]] const DictionaryStructure & dict_struct,
                                   [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                                   [[maybe_unused]] const String & config_prefix,
                                   Block & ,
                                   [[maybe_unused]] ContextPtr global_context,
                                   const String &,
                                   [[maybe_unused]] bool created_from_ddl) -> DictionarySourcePtr
    {
#if USE_YAML_CPP
        if (dict_struct.has_expressions)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `{}` does not support attribute expressions", kYAMLRegExpTree);
        }

        if (!dict_struct.key.has_value() || dict_struct.key.value().size() != 1 || (*dict_struct.key)[0].type->getName() != "String")
        {
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                            "dictionary source `{}` should have one primary key with string value "
                            "to represent regular expressions", kYAMLRegExpTree);
        }

        const auto & filepath = config.getString(config_prefix + "." + kYAMLRegExpTree + ".path");

        const auto & context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        return std::make_unique<YAMLRegExpTreeDictionarySource>(filepath, dict_struct, context, created_from_ddl);
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
*   Configuration allowed fields
*
*   Example:
*   ```
*   - regexp: "MSIE (\\d+)"
*     attr1: "Vecna"
*     attr2: 22.8
*     (arbitrary name not in attribution list): # nested match node for subpattern
*           - regexp: "Windows"
*             attr2: 22.9
*           - regexp: "Linux"
*           ...
*   ```
*/

static const std::string kMatch = "match";
static const std::string kRegExp = "regexp";

/**
*   The data can be loaded from table (using any available dictionary source) with the following structure:
*   ```
*   id UInt64,
*   parent_id UInt64,
*   regexp String,
*   keys array<String>,
*   values array<String>,
*/

const std::string kId = "id";
const std::string kParentId = "parent_id";
const std::string kKeys = "keys";
const std::string kValues = "values";


struct MatchNode
{
    UInt64 id;
    UInt64 parent_id;
    String reg_exp;
    std::vector<Field> keys;
    std::vector<Field> values;
};

struct ResultColumns
{
    MutableColumnPtr ids = ColumnUInt64::create();
    MutableColumnPtr parent_ids = ColumnUInt64::create();
    MutableColumnPtr reg_exps = ColumnString::create();
    MutableColumnPtr keys = ColumnArray::create(ColumnString::create());
    MutableColumnPtr values = ColumnArray::create(ColumnString::create());
    ResultColumns() = default;
};

using StringToNode = std::unordered_map<String, YAML::Node>;

YAML::Node loadYAML(const String & filepath)
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

static StringToNode parseYAMLMap(const YAML::Node & node)
{
    StringToNode result;

    for (const auto & pair : node)
    {
        const String & key = pair.first.as<String>();
        result[key] = pair.second;
    }

    return result;
}

void insertValues(const MatchNode & node, ResultColumns & result_columns)
{
    result_columns.ids->insert(node.id);
    result_columns.parent_ids->insert(node.parent_id);
    result_columns.reg_exps->insert(node.reg_exp);
    result_columns.keys->insert(Array(node.keys.begin(), node.keys.end()));
    result_columns.values->insert(Array(node.values.begin(), node.values.end()));
}

void parseMatchList(UInt64 parent_id, UInt64 & id, const YAML::Node & node, ResultColumns & names_to_attributes, const String & key_name, const DictionaryStructure & structure);

/// MatchNode has to be a map with structure:
/// 1. regex, indicating a regular expression
/// 2. attribute_name, indicating the attributes to set
/// 3. match (optional), indicating the nested match logic under this node
void parseMatchNode(UInt64 parent_id, UInt64 & id, const YAML::Node & node, ResultColumns & result, const String & key_name, const DictionaryStructure & structure)
{
    if (!node.IsMap())
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}` node must be map type", kMatch);
    }

    auto match = parseYAMLMap(node);

    MatchNode attributes_to_insert;

    attributes_to_insert.id = ++id;
    attributes_to_insert.parent_id = parent_id;

    if (!match.contains(key_name))
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "YAML match rule must contain key {}", key_name);
    }
    for (const auto & [key, node_] : match)
    {
        if (key == key_name)
        {
            if (!node_.IsScalar())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}` should be a String", key_name);

            attributes_to_insert.reg_exp = node_.as<String>();
        }
        else if (structure.hasAttribute(key))
        {
            attributes_to_insert.keys.push_back(key);
            attributes_to_insert.values.push_back(node_.as<String>());
        }
        else if (node_.IsSequence())
        {
            parseMatchList(attributes_to_insert.id, id, node_, result, key_name, structure);
        }
        /// unknown attributes.
    }
    insertValues(attributes_to_insert, result);
}

void parseMatchList(UInt64 parent_id, UInt64 & id, const YAML::Node & node, ResultColumns & names_to_attributes, const String & key_name, const DictionaryStructure & structure)
{
    if (!node.IsSequence())
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Configuration {} must be a yaml list of match rules", node.as<String>());
    }

    for (const auto & child_node : node)
    {
        parseMatchNode(parent_id, id, child_node, names_to_attributes, key_name, structure);
    }
}

Block parseYAMLAsRegExpTree(const YAML::Node & node, const String & key_name, const DictionaryStructure & structure)
{
    ResultColumns result_cols;
    UInt64 id = 0;
    parseMatchList(0, id, node, result_cols, key_name, structure);

    ColumnsWithTypeAndName columns;

    columns.push_back(ColumnWithTypeAndName(std::move(result_cols.ids), DataTypePtr(new DataTypeUInt64()), kId));
    columns.push_back(ColumnWithTypeAndName(std::move(result_cols.parent_ids), DataTypePtr(new DataTypeUInt64()), kParentId));
    columns.push_back(ColumnWithTypeAndName(std::move(result_cols.reg_exps), DataTypePtr(new DataTypeString()), kRegExp));
    columns.push_back(ColumnWithTypeAndName(std::move(result_cols.keys), DataTypePtr(new DataTypeArray(DataTypePtr(new DataTypeString()))), kKeys));
    columns.push_back(ColumnWithTypeAndName(std::move(result_cols.values), DataTypePtr(new DataTypeArray(DataTypePtr(new DataTypeString()))), kValues));

    return Block(std::move(columns));
}

YAMLRegExpTreeDictionarySource::YAMLRegExpTreeDictionarySource(
    const String & filepath_, const DictionaryStructure & dict_struct, ContextPtr context_, bool created_from_ddl)
    : filepath(filepath_), structure(dict_struct), context(context_), logger(getLogger(kYAMLRegExpTreeDictionarySource))
{
    key_name = (*structure.key)[0].name;

    const auto user_files_path = context->getUserFilesPath();

    if (created_from_ddl && !fileOrSymlinkPathStartsWith(filepath_, user_files_path))
    {
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File {} is not inside {}", filepath_, user_files_path);
    }
}

YAMLRegExpTreeDictionarySource::YAMLRegExpTreeDictionarySource(const YAMLRegExpTreeDictionarySource & other)
    : filepath(other.filepath)
    , key_name(other.key_name)
    , structure(other.structure)
    , context(Context::createCopy(other.context))
    , logger(other.logger)
    , last_modification(other.last_modification)
{
}

QueryPipeline YAMLRegExpTreeDictionarySource::loadAll()
{
    LOG_INFO(logger, "Loading regexp tree from yaml '{}'", filepath);
    last_modification = getLastModification();

    const auto node = loadYAML(filepath);

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(parseYAMLAsRegExpTree(node, key_name, structure)));
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

String YAMLRegExpTreeDictionarySource::toString() const
{
    return fmt::format("{} with path: {}", kYAMLRegExpTree, filepath);
}

Poco::Timestamp YAMLRegExpTreeDictionarySource::getLastModification() const
{
    return FS::getModificationTimestamp(filepath);
}

}

#endif
