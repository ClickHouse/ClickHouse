#include <Storages/System/StorageSystemDocumentation.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Columns/IColumn.h>
#include <Common/AsynchronousMetrics.h>
#include <Common/CurrentMetrics.h>
#include <Common/Documentation.h>
#include <Common/FunctionDocumentation.h>
#include <Common/ProfileEvents.h>
#include <Compression/CompressionFactory.h>
#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/IDatabase.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Disks/DiskFactory.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <source_location>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Poco/String.h>
#include <boost/algorithm/string/trim.hpp>


namespace DB
{

namespace
{

/// The kind of the documented entity. The numeric values are part of the on-the-wire schema and must be kept stable.
enum class EntityType : int8_t
{
    Function = 1,
    AggregateFunction = 2,
    TableFunction = 3,
    TableEngine = 4,
    DatabaseEngine = 5,
    DataType = 6,
    DictionaryLayout = 7,
    DictionarySource = 8,
    AggregateFunctionCombinator = 9,
    DataSkippingIndex = 10,
    DiskType = 11,
    Setting = 12,
    MergeTreeSetting = 13,
    ServerSetting = 14,
    Format = 15,
    CompressionCodec = 16,
    ProfileEvent = 17,
    CurrentMetric = 18,
    AsynchronousMetric = 19,
    SystemTable = 20,
};

std::vector<std::pair<String, Int8>> getTypeEnumValues()
{
    return {
        {"Function", static_cast<Int8>(EntityType::Function)},
        {"Aggregate Function", static_cast<Int8>(EntityType::AggregateFunction)},
        {"Table Function", static_cast<Int8>(EntityType::TableFunction)},
        {"Table Engine", static_cast<Int8>(EntityType::TableEngine)},
        {"Database Engine", static_cast<Int8>(EntityType::DatabaseEngine)},
        {"Data Type", static_cast<Int8>(EntityType::DataType)},
        {"Dictionary Layout", static_cast<Int8>(EntityType::DictionaryLayout)},
        {"Dictionary Source", static_cast<Int8>(EntityType::DictionarySource)},
        {"Aggregate Function Combinator", static_cast<Int8>(EntityType::AggregateFunctionCombinator)},
        {"Data Skipping Index", static_cast<Int8>(EntityType::DataSkippingIndex)},
        {"Disk Type", static_cast<Int8>(EntityType::DiskType)},
        {"Setting", static_cast<Int8>(EntityType::Setting)},
        {"MergeTree Setting", static_cast<Int8>(EntityType::MergeTreeSetting)},
        {"Server Setting", static_cast<Int8>(EntityType::ServerSetting)},
        {"Format", static_cast<Int8>(EntityType::Format)},
        {"Compression Codec", static_cast<Int8>(EntityType::CompressionCodec)},
        {"Profile Event", static_cast<Int8>(EntityType::ProfileEvent)},
        {"Current Metric", static_cast<Int8>(EntityType::CurrentMetric)},
        {"Asynchronous Metric", static_cast<Int8>(EntityType::AsynchronousMetric)},
        {"System Table", static_cast<Int8>(EntityType::SystemTable)},
    };
}

/// The source files of the entity groups that are documented in a single source file each, where the documentation
/// is not carried by a `Documentation` object (so the path cannot be captured automatically). Relative to the
/// repository root, consistent with the normalized `source` of the documentation objects.
constexpr std::string_view SETTINGS_SOURCE = "src/Core/Settings.cpp";
constexpr std::string_view MERGE_TREE_SETTINGS_SOURCE = "src/Storages/MergeTree/MergeTreeSettings.cpp";
constexpr std::string_view SERVER_SETTINGS_SOURCE = "src/Core/ServerSettings.cpp";
constexpr std::string_view PROFILE_EVENTS_SOURCE = "src/Common/ProfileEvents.cpp";
constexpr std::string_view CURRENT_METRICS_SOURCE = "src/Common/CurrentMetrics.cpp";

/// The source paths captured by `std::source_location` (in `Documentation`/`FunctionDocumentation`) are produced by
/// the compiler: relative to the repository root when the build remaps source paths (`ENABLE_BUILD_PATH_MAPPING`,
/// the default for non-debug builds) and absolute otherwise. To always expose a repository-relative path, we derive
/// the build-time source-root prefix once from the known relative path of this very file and strip it.
String makeRepoRelative(const char * source)
{
    static constexpr std::string_view this_file = "src/Storages/System/StorageSystemDocumentation.cpp";
    /// Derive the build-time source-root prefix once: it is this file's compiled path with the known relative
    /// tail removed (empty when the build already maps source paths to be repository-relative). The compiled path
    /// always ends with `this_file` because the path prefix-map only ever strips a leading prefix, never the tail,
    /// so the leading part before the tail is exactly the prefix to strip from every other captured path.
    static const std::string prefix = []
    {
        const std::string_view full = std::source_location::current().file_name();
        return std::string(full.substr(0, full.size() - this_file.size()));
    }();

    if (source == nullptr)
        return {};

    std::string_view path(source);
    /// Strip the source-root prefix if present. No `prefix.empty()` guard: an empty prefix (when the build already
    /// maps source paths to be repository-relative) is a prefix of everything and `remove_prefix(0)` is a no-op, and
    /// the guard would be provably dead code in such a build (`prefix` is a compile-time constant) — see -Wunreachable-code.
    if (path.starts_with(prefix))
        path.remove_prefix(prefix.size());

    /// A `Documentation`/`FunctionDocumentation` that was default-initialized without braces (`FunctionDocumentation
    /// doc;`) records the header of its `source` field instead of the construction site. Treat that as unknown rather
    /// than reporting a misleading path; the documented entity should be built with braced initialization.
    if (path == "src/Common/FunctionDocumentation.h" || path == "src/Common/Documentation.h")
        return {};

    return String(path);
}

/// Assembles the individual structured parts of an entity's embedded documentation into a single Markdown document,
/// in the same shape as it appears on the website. Empty parts are omitted.
String composeMarkdown(
    const String & description,
    const String & syntax,
    const String & arguments,
    const String & parameters,
    const String & returned_value,
    const String & examples,
    const String & introduced_in,
    const std::vector<String> & related)
{
    String result = boost::algorithm::trim_copy(description);

    auto add_block = [&](std::string_view title, const String & body, bool as_code)
    {
        const String trimmed = boost::algorithm::trim_copy(body);
        if (trimmed.empty())
            return;
        if (!result.empty())
            result += "\n\n";
        result += "**";
        result += title;
        result += "**\n\n";
        if (as_code)
            result += "```sql\n" + trimmed + "\n```";
        else
            result += trimmed;
    };

    add_block("Syntax", syntax, /*as_code=*/ true);
    add_block("Arguments", arguments, /*as_code=*/ false);
    add_block("Parameters", parameters, /*as_code=*/ false);
    add_block("Returned value", returned_value, /*as_code=*/ false);
    add_block("Examples", examples, /*as_code=*/ false);

    const String introduced = boost::algorithm::trim_copy(introduced_in);
    if (!introduced.empty())
    {
        if (!result.empty())
            result += "\n\n";
        result += "**Introduced in:** " + introduced;
    }

    if (!related.empty())
    {
        String related_str;
        for (const auto & name : related)
        {
            if (!related_str.empty())
                related_str += ", ";
            related_str += "`" + name + "`";
        }
        if (!result.empty())
            result += "\n\n";
        result += "**Related:** " + related_str;
    }

    return result;
}

String renderDoc(const Documentation & doc)
{
    return composeMarkdown(
        doc.description,
        doc.syntaxAsString(),
        /*arguments=*/ "",
        /*parameters=*/ "",
        /*returned_value=*/ "",
        doc.examplesAsString(),
        doc.introducedInAsString(),
        doc.related);
}

String renderFunctionDoc(const FunctionDocumentation & doc)
{
    return composeMarkdown(
        doc.description,
        doc.syntaxAsString(),
        doc.argumentsAsString(),
        doc.parametersAsString(),
        doc.returnedValueAsString(),
        doc.examplesAsString(),
        doc.introducedInAsString(),
        /*related=*/ {});
}

void addRow(MutableColumns & res_columns, EntityType type, const String & name, const String & description, std::string_view source)
{
    /// `system.documentation` is a help surface, so an entity without any documentation (an empty `description`)
    /// has nothing to show and is not exposed. This in particular drops internal functions, which carry
    /// `FunctionDocumentation::INTERNAL_FUNCTION_DOCS` with an empty description.
    if (description.empty())
        return;

    res_columns[0]->insert(name);
    res_columns[1]->insert(static_cast<Int8>(type));
    res_columns[2]->insert(description);
    res_columns[3]->insertData(source.data(), source.size());
}

/// Resolves the source of an alias' canonical entity from `source_by_name`, which is keyed by the lower-cased
/// canonical name. The lookup is case-insensitive because an alias' target string is not always spelled exactly as
/// the canonical entity was registered (e.g. the alias `connection_id` targets `connectionID`, but the function is
/// registered as the case-insensitive `connectionId`). Empty if the canonical entity has no documented source.
std::string_view aliasSource(const std::unordered_map<String, String> & source_by_name, const String & canonical)
{
    const auto it = source_by_name.find(Poco::toLower(canonical));
    return it != source_by_name.end() ? std::string_view(it->second) : std::string_view{};
}

/// For function-like factories (regular and aggregate functions) which carry `FunctionDocumentation` and have aliases.
template <typename Factory>
void addFunctionLike(MutableColumns & res_columns, EntityType type, const Factory & factory)
{
    /// Document the canonical functions first, remembering each one's source by (lower-cased) name, so that an alias
    /// can reference the source of its canonical function without calling `getDocumentation` on the alias' target
    /// string, which is not always a directly resolvable key (see `aliasSource`).
    std::unordered_map<String, String> source_by_name;
    std::vector<String> alias_names;
    for (const auto & name : factory.getAllRegisteredNames())
    {
        if (factory.isAlias(name))
        {
            alias_names.push_back(name);
            continue;
        }

        const auto documentation = factory.getDocumentation(name);
        /// Internal functions are not part of the user-facing documentation.
        if (documentation.category == FunctionDocumentation::Category::Internal)
            continue;
        const String source = makeRepoRelative(documentation.source);
        source_by_name.emplace(Poco::toLower(name), source);
        addRow(res_columns, type, name, renderFunctionDoc(documentation), source);
    }

    for (const auto & name : alias_names)
    {
        const auto & canonical = factory.aliasTo(name);
        addRow(res_columns, type, name, "Alias of `" + canonical + "`.", aliasSource(source_by_name, canonical));
    }
}

/// For factories which carry `Documentation` and have no aliases.
template <typename Factory>
void addDocumented(MutableColumns & res_columns, EntityType type, const Factory & factory)
{
    for (const auto & name : factory.getAllRegisteredNames())
    {
        const auto & documentation = factory.getDocumentation(name);
        addRow(res_columns, type, name, renderDoc(documentation), makeRepoRelative(documentation.source));
    }
}

/// For factories which carry `Documentation` and have aliases (data type families). See `addFunctionLike` for the
/// two-pass scheme used to resolve the source of aliases.
template <typename Factory>
void addDocumentedWithAliases(MutableColumns & res_columns, EntityType type, const Factory & factory)
{
    std::unordered_map<String, String> source_by_name;
    std::vector<String> alias_names;
    for (const auto & name : factory.getAllRegisteredNames())
    {
        if (factory.isAlias(name))
        {
            alias_names.push_back(name);
            continue;
        }
        const auto documentation = factory.getDocumentation(name);
        const String source = makeRepoRelative(documentation.source);
        source_by_name.emplace(Poco::toLower(name), source);
        addRow(res_columns, type, name, renderDoc(documentation), source);
    }

    for (const auto & name : alias_names)
    {
        const auto & canonical = factory.aliasTo(name);
        addRow(res_columns, type, name, "Alias of `" + canonical + "`.", aliasSource(source_by_name, canonical));
    }
}

/// The documentation of a setting is its description (already authored as Markdown), followed by its type and
/// default value, with a note appended for the settings that are not yet generally available.
String renderSettingDoc(std::string_view description, std::string_view type_name, const String & default_value, SettingsTierType tier)
{
    String result = boost::algorithm::trim_copy(String(description));

    auto add_note = [&](const String & note)
    {
        if (!result.empty())
            result += "\n\n";
        result += note;
    };

    if (!type_name.empty())
        add_note("**Type:** `" + String(type_name) + "`");

    /// An empty default value would render as empty backticks (an empty code span), which reads as if
    /// the value were missing; spell it out as italic prose instead.
    if (default_value.empty())
        add_note("**Default:** *empty string*");
    else
        add_note("**Default:** `" + default_value + "`");

    if (tier == SettingsTierType::EXPERIMENTAL)
        add_note("**Tier:** Experimental");
    else if (tier == SettingsTierType::BETA)
        add_note("**Tier:** Beta");

    return result;
}

/// For the settings collections (`Settings`, `MergeTreeSettings`, `ServerSettings`), which expose the name,
/// description, type, default value and tier of every registered setting. All settings of a collection are
/// declared in a single source file, passed as `source`.
template <typename SettingsCollection>
void addSettingsLike(MutableColumns & res_columns, EntityType type, const SettingsCollection & settings, std::string_view source)
{
    for (const auto & name : settings.getAllRegisteredNames())
    {
        /// Obsolete settings carry the placeholder description "Obsolete setting, does nothing." and have
        /// no documentation value on a help surface, so they are not exposed.
        const auto tier = settings.getTier(name);
        if (tier == SettingsTierType::OBSOLETE)
            continue;
        addRow(res_columns, type, String(name),
            renderSettingDoc(settings.getDescription(name), settings.getTypeName(name), settings.getDefaultValueString(name), tier),
            source);
    }
}

/// Settings can have aliases (e.g. `enable_analyzer` for `allow_experimental_analyzer`). As for the other
/// entities with aliases, the alias is rendered as a reference to the canonical setting rather than
/// duplicating its documentation.
template <typename SettingsCollection>
void addSettingAliases(MutableColumns & res_columns, EntityType type, const SettingsCollection & settings, std::string_view source)
{
    for (const auto & alias : settings.getAllAliasNames())
    {
        /// `getTier` resolves the alias to its canonical setting; skip aliases of obsolete settings,
        /// consistent with the canonical settings, which are not exposed either.
        if (settings.getTier(alias) == SettingsTierType::OBSOLETE)
            continue;
        addRow(res_columns, type, String(alias), "Alias of `" + String(SettingsCollection::resolveName(alias)) + "`.", source);
    }
}

/// The documentation of a system table is its table comment, followed by the list of its columns: the name, type
/// and description (the column comment) of each, rendered as a Markdown list.
String renderSystemTableDoc(const String & comment, const ColumnsDescription & columns)
{
    String result = boost::algorithm::trim_copy(comment);

    String columns_list;
    for (const auto & column : columns)
    {
        columns_list += "- `" + column.name + "` (`" + column.type->getName() + "`)";
        const String column_comment = boost::algorithm::trim_copy(column.comment);
        if (!column_comment.empty())
            columns_list += " — " + column_comment;
        columns_list += "\n";
    }

    if (!columns_list.empty())
    {
        if (!result.empty())
            result += "\n\n";
        result += "**Columns**\n\n";
        result += boost::algorithm::trim_copy(columns_list);
    }

    return result;
}

}

ColumnsDescription StorageSystemDocumentation::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the entity, e.g. `domainWithoutWWW` or `MergeTree`."},
        {"type", std::make_shared<DataTypeEnum8>(getTypeEnumValues()), "The kind of the entity, e.g. `Function` or `Table Engine`."},
        {"description", std::make_shared<DataTypeString>(),
            "The reference documentation of the entity rendered as Markdown, assembled from the embedded documentation "
            "(the same content as published on the website), including syntax, examples and other structured parts, if any."},
        {"source", std::make_shared<DataTypeString>(),
            "The path to the source file where the entity's documentation is defined, relative to the repository root. "
            "Empty if the source location is unknown."},
    };
}

void StorageSystemDocumentation::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    addFunctionLike(res_columns, EntityType::Function, FunctionFactory::instance());
    addFunctionLike(res_columns, EntityType::AggregateFunction, AggregateFunctionFactory::instance());

    {
        const auto & factory = TableFunctionFactory::instance();
        for (const auto & name : factory.getAllRegisteredNames())
        {
            if (factory.isAlias(name))
            {
                const auto & canonical = factory.aliasTo(name);
                const auto canonical_documentation = factory.tryGetDocumentation(canonical);
                addRow(res_columns, EntityType::TableFunction, name, "Alias of `" + canonical + "`.",
                    canonical_documentation ? makeRepoRelative(canonical_documentation->source) : String{});
                continue;
            }

            const auto documentation = factory.tryGetDocumentation(name);
            /// Skip table functions without public documentation (no docs at all or internal-only).
            if (!documentation || documentation->category == FunctionDocumentation::Category::Internal)
                continue;
            addRow(res_columns, EntityType::TableFunction, name, renderFunctionDoc(*documentation), makeRepoRelative(documentation->source));
        }
    }

    for (const auto & [name, creator] : StorageFactory::instance().getAllStorages())
        addRow(res_columns, EntityType::TableEngine, name, renderDoc(creator.documentation), makeRepoRelative(creator.documentation.source));

    for (const auto & [name, creator] : DatabaseFactory::instance().getDatabaseEngines())
        addRow(res_columns, EntityType::DatabaseEngine, name, renderDoc(creator.documentation), makeRepoRelative(creator.documentation.source));

    addDocumentedWithAliases(res_columns, EntityType::DataType, DataTypeFactory::instance());
    addDocumented(res_columns, EntityType::DictionaryLayout, DictionaryFactory::instance());
    addDocumented(res_columns, EntityType::DictionarySource, DictionarySourceFactory::instance());

    for (const auto & combinator : AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators())
    {
        if (combinator.combinator_ptr->isForInternalUsageOnly())
            continue;
        addRow(res_columns, EntityType::AggregateFunctionCombinator, combinator.name, renderDoc(combinator.documentation),
            makeRepoRelative(combinator.documentation.source));
    }

    addDocumented(res_columns, EntityType::DataSkippingIndex, MergeTreeIndexFactory::instance());
    addDocumented(res_columns, EntityType::DiskType, DiskFactory::instance());

    addSettingsLike(res_columns, EntityType::Setting, Settings{}, SETTINGS_SOURCE);
    addSettingAliases(res_columns, EntityType::Setting, Settings{}, SETTINGS_SOURCE);
    addSettingsLike(res_columns, EntityType::MergeTreeSetting, MergeTreeSettings{}, MERGE_TREE_SETTINGS_SOURCE);
    addSettingAliases(res_columns, EntityType::MergeTreeSetting, MergeTreeSettings{}, MERGE_TREE_SETTINGS_SOURCE);
    addSettingsLike(res_columns, EntityType::ServerSetting, ServerSettings{}, SERVER_SETTINGS_SOURCE);

    /// The format dictionary is keyed by the lower-cased name; `creators.name` carries the original case.
    for (const auto & name_and_creators : FormatFactory::instance().getAllFormats())
    {
        const auto & creators = name_and_creators.second;
        addRow(res_columns, EntityType::Format, creators.name, renderDoc(creators.documentation), makeRepoRelative(creators.documentation.source));
    }

    for (const auto & [name, documentation] : CompressionCodecFactory::instance().getCodecDocumentations())
        addRow(res_columns, EntityType::CompressionCodec, name, renderDoc(documentation), makeRepoRelative(documentation.source));

    for (ProfileEvents::Event event = ProfileEvents::Event(0); event < ProfileEvents::end(); ++event)
        addRow(res_columns, EntityType::ProfileEvent, String(ProfileEvents::getName(event)),
            boost::algorithm::trim_copy(String(ProfileEvents::getDocumentation(event))), PROFILE_EVENTS_SOURCE);

    for (CurrentMetrics::Metric metric = CurrentMetrics::Metric(0); metric < CurrentMetrics::end(); ++metric)
        addRow(res_columns, EntityType::CurrentMetric, String(CurrentMetrics::getName(metric)),
            boost::algorithm::trim_copy(String(CurrentMetrics::getDocumentation(metric))), CURRENT_METRICS_SOURCE);

    /// Asynchronous metrics and their descriptions are produced at runtime and held by the global instance.
    /// They are available only on the server (the instance may be absent, e.g. in clickhouse-local).
    if (const auto * asynchronous_metrics = context->getAsynchronousMetrics())
    {
        for (const auto & [name, value] : asynchronous_metrics->getValues())
            addRow(res_columns, EntityType::AsynchronousMetric, name,
                value.documentation ? boost::algorithm::trim_copy(String(value.documentation)) : String{},
                makeRepoRelative(value.source));
    }

    /// System tables document themselves with their table comment, authored at the attachment site.
    if (const auto system_database = DatabaseCatalog::instance().tryGetDatabase(DatabaseCatalog::SYSTEM_DATABASE))
    {
        for (auto iterator = system_database->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            if (const auto & table = iterator->table())
            {
                const auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
                if (metadata_snapshot)
                {
                    /// Bind to a reference first: `typeid(*table)` would warn about evaluating an expression with
                    /// side effects (the smart pointer dereference) as the operand of a polymorphic `typeid`.
                    const IStorage & storage = *table;
                    addRow(res_columns, EntityType::SystemTable, iterator->name(),
                        renderSystemTableDoc(metadata_snapshot->comment, metadata_snapshot->getColumns()),
                        makeRepoRelative(getSystemTableSource(typeid(storage))));
                }
            }
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDocumentation) }
