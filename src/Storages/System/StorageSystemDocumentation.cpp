#include <Storages/System/StorageSystemDocumentation.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Columns/IColumn.h>
#include <Common/AsynchronousMetrics.h>
#include <Common/CurrentMetrics.h>
#include <Common/Documentation.h>
#include <Common/FunctionDocumentation.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <Common/re2.h>
#include <Compression/CompressionFactory.h>
#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsChangesHistory.h>
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
#include <Parsers/StatementFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <algorithm>
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
    Statement = 21,
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
        {"Statement", static_cast<Int8>(EntityType::Statement)},
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

/// Detects whether a description is the full body of a reference page rather than a short summary.
/// Components whose website pages are autogenerated verbatim from the embedded documentation (table engines,
/// database engines, data types, formats, table functions, dictionary sources and layouts) keep the entire page
/// in `description`, and such a page contains either a Markdown H1 or its own Markdown section headers, which per
/// the documentation conventions carry an explicit `{#anchor}`. The structured metadata fields (`syntax`,
/// `examples`, `related`, ...) then only restate parts of the page in a machine-readable form, so appending
/// sections composed from them would duplicate material the page body already covers. An H1 unambiguously
/// identifies a complete page; for lower-level headings, the anchor requirement distinguishes a page section
/// header from an incidental header inside a short summary. Headers inside fenced code blocks do not count.
bool isFullPageDescription(std::string_view description)
{
    bool in_code_block = false;
    while (!description.empty())
    {
        const size_t eol = description.find('\n');
        const std::string_view line = description.substr(0, eol);

        if (line.starts_with("```"))
        {
            in_code_block = !in_code_block;
        }
        else if (!in_code_block)
        {
            const size_t hashes = line.find_first_not_of('#');
            if (hashes == 1 && line[hashes] == ' ')
                return true;
            if (hashes >= 2 && hashes <= 6 && line[hashes] == ' ' && line.contains("{#"))
                return true;
        }

        if (eol == std::string_view::npos)
            break;
        description.remove_prefix(eol + 1);
    }
    return false;
}

std::string_view trimMarkdownLine(std::string_view line)
{
    while (!line.empty() && isWhitespaceASCII(line.front()))
        line.remove_prefix(1);
    while (!line.empty() && isWhitespaceASCII(line.back()))
        line.remove_suffix(1);
    return line;
}

std::string_view trimMarkdownSectionLabel(std::string_view line)
{
    line = trimMarkdownLine(line);

    const size_t hashes = line.find_first_not_of('#');
    if (hashes != 0 && hashes != std::string_view::npos)
        line = trimMarkdownLine(line.substr(hashes));

    const size_t anchor = line.find(" {#");
    if (anchor != std::string_view::npos && line.ends_with('}'))
        line = trimMarkdownLine(line.substr(0, anchor));

    if (line.size() >= 4 && line.starts_with("**") && line.ends_with("**"))
        line = trimMarkdownLine(line.substr(2, line.size() - 4));

    if (line.ends_with(':'))
        line = trimMarkdownLine(line.substr(0, line.size() - 1));

    return line;
}

bool isSyntaxSectionLabel(std::string_view line)
{
    return equalsCaseInsensitive(trimMarkdownSectionLabel(line), "syntax");
}

bool isRelatedSectionLabel(std::string_view line)
{
    line = trimMarkdownSectionLabel(line);
    return equalsCaseInsensitive(line, "related")
        || equalsCaseInsensitive(line, "related content")
        || equalsCaseInsensitive(line, "related statements")
        || equalsCaseInsensitive(line, "see also");
}

/// Detects syntax which is already documented by a Markdown section or by prose immediately introducing an SQL block.
bool descriptionDocumentsSyntax(std::string_view description)
{
    bool in_code_block = false;
    bool syntax_block_follows = false;
    while (!description.empty())
    {
        const size_t eol = description.find('\n');
        const std::string_view line = trimMarkdownLine(description.substr(0, eol));

        if (line.starts_with("```"))
        {
            const bool is_sql_fence = equalsCaseInsensitive(line, "```sql");
            if (!in_code_block && syntax_block_follows && is_sql_fence)
                return true;
            in_code_block = !in_code_block;
            syntax_block_follows = false;
        }
        else if (!in_code_block && !line.empty())
        {
            if (isSyntaxSectionLabel(line))
                return true;
            syntax_block_follows = toLowerCopyASCII(line).ends_with("syntax:");
        }

        if (eol == std::string_view::npos)
            break;
        description.remove_prefix(eol + 1);
    }
    return false;
}

/// Detects an existing cross-reference section, whose entries should remain the source of truth for the page.
bool descriptionDocumentsRelated(std::string_view description)
{
    bool in_code_block = false;
    while (!description.empty())
    {
        const size_t eol = description.find('\n');
        const std::string_view line = trimMarkdownLine(description.substr(0, eol));

        if (line.starts_with("```"))
            in_code_block = !in_code_block;
        else if (!in_code_block && isRelatedSectionLabel(line))
            return true;

        if (eol == std::string_view::npos)
            break;
        description.remove_prefix(eol + 1);
    }
    return false;
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
    const String & parent,
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

    const String trimmed_syntax = boost::algorithm::trim_copy(syntax);
    /// Do not append structured syntax when the description already documents it, whether verbatim or as a
    /// dedicated Markdown section whose formatting or punctuation differs from the structured representation.
    if (!trimmed_syntax.empty() && !descriptionDocumentsSyntax(result) && !result.contains(trimmed_syntax))
        add_block("Syntax", trimmed_syntax, /*as_code=*/ true);
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

    const String enclosing = boost::algorithm::trim_copy(parent);
    if (!enclosing.empty())
    {
        if (!result.empty())
            result += "\n\n";
        result += "**Part of:** `" + enclosing + "`";
    }

    if (!related.empty() && !descriptionDocumentsRelated(result))
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
    /// A full-page description is already the complete document, published on the website as-is.
    if (isFullPageDescription(doc.description))
        return boost::algorithm::trim_copy(doc.description);

    return composeMarkdown(
        doc.description,
        doc.syntaxAsString(),
        /*arguments=*/ "",
        /*parameters=*/ "",
        /*returned_value=*/ "",
        doc.examplesAsString(),
        doc.introducedInAsString(),
        doc.parent,
        doc.related);
}

String renderFunctionDoc(const FunctionDocumentation & doc)
{
    /// A full-page description is already the complete document, published on the website as-is.
    if (isFullPageDescription(doc.description))
        return boost::algorithm::trim_copy(doc.description);

    return composeMarkdown(
        doc.description,
        doc.syntaxAsString(),
        doc.argumentsAsString(),
        doc.parametersAsString(),
        doc.returnedValueAsString(),
        doc.examplesAsString(),
        doc.introducedInAsString(),
        /*parent=*/ "",
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

/// A value of a setting as it appears in the documentation. An empty value would render as empty backticks
/// (an empty code span), which reads as if the value were missing; spell it out as italic prose instead.
String renderSettingValue(const String & value)
{
    return value.empty() ? "*empty string*" : "`" + value + "`";
}

/// One recorded change of the default value of one setting: the version that introduced the change, together with
/// the change itself. The change is referenced rather than copied — it is owned by the static change history, which
/// is built once and never modified afterwards.
struct SettingHistoryEntry
{
    String version;
    const SettingsChangesHistory::SettingChange * change;
};

/// The change history of every setting of a collection, keyed by setting name and ordered from the oldest recorded
/// version to the newest. The keys reference the names owned by the static change history.
using SettingsHistoryIndex = std::unordered_map<std::string_view, std::vector<SettingHistoryEntry>>;

/// The change history of a settings collection, indexed for lookup in the two ways it is looked up.
struct SettingsHistory
{
    /// Keyed by the canonical name of the setting. `compatibility` resolves the recorded name of every change
    /// through `resolveName` before applying it, so a change recorded under an alias of a setting belongs to the
    /// history of that setting as much as one recorded under its canonical name. Without this, the history of a
    /// setting that was renamed would be cut at the rename. `resolveName` knows only the names a setting has
    /// today, so a rename whose old name was not kept as an alias is followed separately, by the names the
    /// reasons of the rename records give — see `buildRenames`.
    SettingsHistoryIndex by_setting;
    /// Keyed by the name of an alias, and holding the history of that name as opposed to the history of the setting
    /// it resolves to: every record written under the alias itself, plus the records written under another name of
    /// the same setting that register this one as an alias. The second part is needed because the history file is
    /// inconsistent about where aliasing is recorded — `async_insert_busy_timeout_ms` was registered as an alias by
    /// a record written under the canonical `async_insert_busy_timeout_max_ms`, and a setting that is renamed with
    /// its old name kept as an alias (`text_index_density_threshold`) has that rename recorded under the new name.
    SettingsHistoryIndex by_alias;
};

/// Whether `reason` refers to `name` as a whole word. The reasons in the change history mention setting names bare
/// as well as inside backticks or quotes, and one setting name is often a prefix of another
/// (`async_insert_busy_timeout_ms` and `async_insert_busy_timeout_max_ms`), so a substring match would not do.
bool reasonMentionsName(std::string_view reason, std::string_view name)
{
    for (size_t pos = reason.find(name); pos != std::string_view::npos; pos = reason.find(name, pos + 1))
    {
        const bool left_is_boundary = pos == 0 || !isWordCharASCII(reason[pos - 1]);
        const bool right_is_boundary = pos + name.size() == reason.size() || !isWordCharASCII(reason[pos + name.size()]);
        if (left_is_boundary && right_is_boundary)
            return true;
    }
    return false;
}

/// Whether a record written under one name of a setting registers `alias` as another name of it: it names the alias
/// and says either that an alias is being added ("`x` is aliased to `y`") or that the setting is being renamed,
/// which is how the file words keeping the old name as an alias ("Renamed from `text_index_density_threshold`
/// (kept as an alias)", "The setting was renamed. The previous name is `allow_statistic_optimize`.").
///
/// Whether the record also changes the default value is irrelevant: the two happen in the same version often
/// enough ("Lightweight updates were moved to Beta. Added an alias for setting
/// `allow_experimental_lightweight_update`." changes the default from `false` to `true`), and the alias appeared
/// in that version all the same.
bool recordRegistersAliasNamed(const SettingsChangesHistory::SettingChange & change, std::string_view alias)
{
    static const re2::RE2 aliasing_or_renaming(R"((?i)alias|renam)");
    return re2::RE2::PartialMatch(change.reason, aliasing_or_renaming)
        && reasonMentionsName(change.reason, alias);
}

/// Whether the reason authored for a change in `SettingsChangesHistory.cpp` says that the record is there to
/// register an alias of a setting: "Added an alias for setting `x`", "Add alias to x", "Added as an alias for 'x'",
/// "Alias for os_threads_nice_value_query.".
///
/// The bound of the first form keeps the verb and the alias in one phrase, and the second form is anchored at the
/// beginning of a sentence, so that a record which introduces a setting and mentions an alias of it in passing
/// ("New setting ... . 'y' is an alias for this setting.") is not mistaken for one.
bool reasonRegistersAnAlias(std::string_view reason)
{
    static const re2::RE2 registers_an_alias(
        R"((?i)\b(?:add\w*|new|introduc\w*)\b[^.]{0,20}\balias\b|(?:^|[.;]\s+)(?:an?\s+)?alias\s+(?:for|of|to)\b)");
    return re2::RE2::PartialMatch(reason, registers_an_alias);
}

/// Adds a record to the history of one name, unless the same change of the default value in the same version is
/// already listed there under another name. One change is recorded twice whenever it concerns both a setting and
/// an alias of it, once under each name and with a reason authored separately for each, and the history of a name
/// lists it once. Which of the two records is kept is decided by `authoritative`: the history of a setting keeps
/// the record written under the name of that setting, and the history of an alias keeps the record that registers
/// the alias, each being the more direct account of the change for the name it is rendered for.
///
/// The two records are recognized as the same change by the version and the values, and not by the reason, which
/// is free-form and authored per record ("Lightweight updates were moved to Beta. Added an alias for setting
/// `allow_experimental_lightweight_update`." against "Lightweight updates were moved to Beta."). Records written
/// under the same name are never coalesced: they are separate entries of the history file, not two accounts of
/// one change (`enable_max_bytes_limit_for_min_age_to_force_merge` has two 25.1 records with the same values but
/// different reasons), and each keeps its own reason.
void addSettingHistoryEntry(std::vector<SettingHistoryEntry> & entries, const SettingHistoryEntry & entry, bool authoritative)
{
    const auto same_change = std::find_if(entries.begin(), entries.end(), [&](const SettingHistoryEntry & other)
    {
        return other.change->name != entry.change->name
            && other.version == entry.version
            && other.change->previous_value == entry.change->previous_value
            && other.change->new_value == entry.change->new_value;
    });

    if (same_change == entries.end())
        entries.push_back(entry);
    else if (authoritative)
        *same_change = entry;
}

/// The name that the reason of a record gives as another name of the same setting across a rename: the old name
/// for a record written under the new one ("Rename of `x`", "Rename of setting `x`", "Renamed from `x`",
/// "New name of `x`", "The setting was renamed. The previous name is `x`."), and the new name for a record written
/// under the old one ("Obsolete setting, renamed to `x`."). Empty when the reason notes no rename.
///
/// The returned view points into `reason`, which is owned by the static change history.
std::string_view renamedName(std::string_view reason, const re2::RE2 & wording)
{
    std::string_view name;
    if (re2::RE2::PartialMatch(reason, wording, &name))
        return name;
    return {};
}

std::string_view previousNameOfRenamedSetting(std::string_view reason)
{
    static const re2::RE2 wording(
        R"((?i)(?:rename of(?: setting)?|renamed from|new name of|previous name is)[^0-9A-Za-z_]*([0-9A-Za-z_]+))");
    return renamedName(reason, wording);
}

std::string_view newNameOfRenamedSetting(std::string_view reason)
{
    static const re2::RE2 wording(R"((?i)renamed to[^0-9A-Za-z_]*([0-9A-Za-z_]+))");
    return renamedName(reason, wording);
}

/// Maps a name a setting used to have onto the name it has today, for the renames that `resolveName` cannot follow:
/// a rename keeps the old name as an alias only sometimes, and when it does not — `distributed_cache_read_alignment`
/// became `distributed_cache_alignment` and was made obsolete — the history recorded under the old name is not
/// reachable from the setting through the aliases. The history file records such a rename in the reason of a
/// record, written either under the new name or under the old one, and that reason is the only account of it.
///
/// Chains are followed, so a setting renamed twice maps to its current name; the recursion is bounded by the number
/// of mappings, as a cycle would otherwise be possible for a pair of records naming each other.
template <typename SettingsCollection>
std::unordered_map<std::string_view, std::string_view> buildRenames(const VersionToSettingsChangesMap & history)
{
    std::unordered_map<std::string_view, std::string_view> renames;

    auto add = [&](std::string_view previous_name, std::string_view current_name)
    {
        /// A rename that kept the old name as an alias needs no mapping: `resolveName` already follows it. The same
        /// goes for a reason that names the setting it is written under ("Renamed from ... (kept as an alias)").
        if (previous_name.empty() || SettingsCollection::resolveName(previous_name) == current_name)
            return;
        renames.emplace(previous_name, current_name);
    };

    for (const auto & [_, changes] : history)
        for (const auto & change : changes)
        {
            const std::string_view current = SettingsCollection::resolveName(change.name);
            add(previousNameOfRenamedSetting(change.reason), current);
            /// The record is written under the old name here, so it is that name which maps onto the new one.
            if (const std::string_view new_name = newNameOfRenamedSetting(change.reason); !new_name.empty())
                add(change.name, SettingsCollection::resolveName(new_name));
        }

    /// Collapse the chains, so that a single lookup gives the current name.
    for (auto & [previous_name, current_name] : renames)
    {
        for (size_t hop = 0; hop < renames.size(); ++hop)
        {
            const auto next = renames.find(current_name);
            if (next == renames.end() || next->second == previous_name)
                break;
            current_name = next->second;
        }
    }

    return renames;
}

/// Inverts the change history — a map of version to the changes made in that version — into per-setting indices.
template <typename SettingsCollection>
SettingsHistory buildSettingsHistory(const SettingsCollection & settings, const VersionToSettingsChangesMap & history)
{
    /// The names that settings used to have before a rename that `resolveName` cannot follow.
    const auto renames = buildRenames<SettingsCollection>(history);

    /// The aliases of every setting of the collection, to attribute a record that registers an alias to that alias
    /// even when the record is written under another name of the same setting.
    std::unordered_map<std::string_view, std::vector<std::string_view>> aliases_by_setting;
    for (const auto & alias : settings.getAllAliasNames())
        aliases_by_setting[SettingsCollection::resolveName(alias)].push_back(alias);

    SettingsHistory result;
    /// The history is ordered by version, so every per-setting vector comes out ordered by version as well.
    for (const auto & [version, changes] : history)
    {
        const String version_string = version.toString();
        for (const auto & change : changes)
        {
            const SettingHistoryEntry entry{version_string, &change};

            const std::string_view canonical = SettingsCollection::resolveName(change.name);

            /// A record written under an alias is history of that alias, and a record written under another name of
            /// the same setting is too when it is what registered the alias.
            if (canonical != change.name)
                addSettingHistoryEntry(result.by_alias[change.name], entry, /* authoritative= */ false);
            if (const auto it = aliases_by_setting.find(canonical); it != aliases_by_setting.end())
                for (const auto & alias : it->second)
                    if (alias != change.name && recordRegistersAliasNamed(change, alias))
                        addSettingHistoryEntry(result.by_alias[alias], entry, /* authoritative= */ true);

            /// A record written under an alias for the sole purpose of registering that alias is the history of
            /// the alias and not of the setting it aliases: it neither introduces that setting nor changes its
            /// default. Without this, `max_insert_block_size` — older than the change history and with no
            /// recorded change of its own — would pick up the 26.1 record that registered its alias
            /// `max_insert_block_size_rows` and claim to have been introduced in that version.
            if (canonical != change.name && change.previous_value == change.new_value
                && reasonRegistersAnAlias(change.reason))
                continue;

            addSettingHistoryEntry(result.by_setting[canonical], entry, /* authoritative= */ canonical == change.name);

            /// A record written under a name the setting had before a rename is history of the setting as it is
            /// named today as well. It is kept under the old name too: a name that is still a setting of its own
            /// (an obsolete setting, say) keeps its own account of the change.
            if (const auto renamed = renames.find(canonical); renamed != renames.end())
                addSettingHistoryEntry(result.by_setting[renamed->second], entry, /* authoritative= */ false);
        }
    }
    return result;
}

/// Whether the reason authored for a change in `SettingsChangesHistory.cpp` says that the record is there to
/// register something that did not exist before, rather than to note something about a setting that already
/// existed.
///
/// The history has no structured marker for it, so this has to go by the wording, and the wording is free-form:
/// a record that registers a new setting is as likely to describe what the setting does ("Cloud sync", "Max
/// retries for general keeper operations", "Allow to skip empty files in azure table engine") as to say that it
/// is new. Recognizing the phrasings that announce a new setting would therefore drop most introductions, so
/// this recognizes the opposite — the far smaller and more formulaic set of phrasings the file uses when a
/// no-op record is about a setting that already existed: that it became obsolete, that it graduated to another
/// maturity tier, that an existing setting became settable per query, that it was renamed, or that the record adds
/// an alias.
///
/// A rename is never an introduction: the setting existed before under another name, whether or not the history
/// recorded under that name can be recovered — and where it cannot, claiming the rename version as the introducing
/// one would be exactly the wrong answer, a version in which the setting demonstrably already existed.
///
/// A record that adds an alias introduces the alias and not the setting it aliases — hence `documenting_an_alias`.
bool reasonRecordsIntroduction(std::string_view reason, bool documenting_an_alias)
{
    /// "Obsolete setting", "Old setting which popped up here being renamed", "Made this setting adjustable on a
    /// per-query level", "became the Beta tier feature", "was moved to Beta", "is now Beta", "Rename of
    /// distributed_cache_read_alignment", "New name of `allow_experimental_delta_kernel_rs`".
    static const re2::RE2 concerns_an_existing_setting(
        R"((?i)\bobsolete\b|\bdeprecated\b|\bold setting\b|made this setting|no longer|became (?:the\s)?\w+ tier|moved to beta|is now beta|\brenam\w*|\bnew name of\b)");

    if (reasonRegistersAnAlias(reason))
        return documenting_an_alias;
    return !re2::RE2::PartialMatch(reason, concerns_an_existing_setting);
}

/// Whether a recorded change is the introduction of the entity — a setting or an alias of one — that it is being
/// rendered for. It is, when the change is from a value to itself and its reason does not say otherwise: such a
/// record has no effect on `compatibility` and is written either to make a setting that did not exist before known
/// to it, or to note something else about a setting that already exists (that it became obsolete, or that it graduated from
/// experimental to beta, say). A setting introduced with a compatibility value that differs from its default is
/// instead recorded as an ordinary change of the default and is indistinguishable from one, so it is reported as
/// such rather than claimed to be an introduction that the history does not actually record.
///
/// The record that adds an alias to a setting introduces the alias, while for the setting itself it is one more
/// record that changes nothing — hence `documenting_an_alias`.
bool isIntroduction(const SettingHistoryEntry & entry, bool documenting_an_alias)
{
    return entry.change->previous_value == entry.change->new_value
        && reasonRecordsIntroduction(entry.change->reason, documenting_an_alias);
}

/// The history of the default value of a setting, appended to its documentation as a Markdown list, newest change
/// first: in which version the setting was introduced, if that is recorded, and how its default value changed since.
/// Every change also carries the reason it was made, as authored in `SettingsChangesHistory.cpp`.
///
/// Not every setting has a recorded history: the history exists to implement the `compatibility` setting, so it
/// covers the changes made since that mechanism was introduced, and a setting that is older than it and never
/// changed its default has no records at all.
void appendSettingHistory(String & result, const std::vector<SettingHistoryEntry> & entries, bool documenting_an_alias)
{
    if (entries.empty())
        return;

    if (!result.empty())
        result += "\n\n";

    /// The introducing version is called out separately, ahead of the list, because it is the single most
    /// looked-up fact of the history while being the last item of a list that can be long.
    const bool introduced = isIntroduction(entries.front(), documenting_an_alias);
    if (introduced)
        result += "**Introduced in:** v" + entries.front().version + "\n\n";

    result += "**History**\n\n";

    /// The entries are ordered from the oldest to the newest, and are listed in the opposite order.
    for (size_t i = entries.size(); i > 0; --i)
    {
        const auto & entry = entries[i - 1];
        const auto & change = *entry.change;

        if (i != entries.size())
            result += "\n";

        result += "- **" + entry.version + "** — ";
        if (change.previous_value == change.new_value)
        {
            /// A change to the same value leaves the default as it was; it is recorded either to introduce a new
            /// setting or, for a setting that already exists, to note something else about it (that it became
            /// obsolete, or that it graduated from experimental to beta, say) — hence the reason below.
            if (i == 1 && introduced)
                result += "introduced with the default value " + renderSettingValue(fieldToString(change.new_value)) + ".";
            else
                result += "the default value remained " + renderSettingValue(fieldToString(change.new_value)) + ".";
        }
        else
            result += "the default value changed from " + renderSettingValue(fieldToString(change.previous_value))
                + " to " + renderSettingValue(fieldToString(change.new_value)) + ".";

        const String reason = boost::algorithm::trim_copy(change.reason);
        if (!reason.empty())
            result += " " + reason;
    }
}

/// The documentation of a setting is its description (already authored as Markdown), followed by its type and
/// default value, with a note appended for the settings that are not yet generally available, and the history of
/// the changes of its default value across ClickHouse versions.
String renderSettingDoc(
    std::string_view description,
    std::string_view type_name,
    const String & default_value,
    SettingsTierType tier,
    const std::vector<SettingHistoryEntry> * history)
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

    add_note("**Default:** " + renderSettingValue(default_value));

    if (tier == SettingsTierType::EXPERIMENTAL)
        add_note("**Tier:** Experimental");
    else if (tier == SettingsTierType::PRIVATE_PREVIEW)
        add_note("**Tier:** Private preview");
    else if (tier == SettingsTierType::BETA)
        add_note("**Tier:** Beta");

    if (history)
        appendSettingHistory(result, *history, /* documenting_an_alias = */ false);

    return result;
}

/// The change history of a setting, or `nullptr` if the setting has no recorded changes.
const std::vector<SettingHistoryEntry> * findSettingHistory(const SettingsHistoryIndex & history, std::string_view name)
{
    const auto it = history.find(name);
    return it == history.end() ? nullptr : &it->second;
}

/// For the settings collections (`Settings`, `MergeTreeSettings`, `ServerSettings`), which expose the name,
/// description, type, default value and tier of every registered setting. All settings of a collection are
/// declared in a single source file, passed as `source`. `history` is the change history of the collection, which
/// is empty for the collections that do not have one (server settings are not covered by `compatibility`).
template <typename SettingsCollection>
void addSettingsLike(
    MutableColumns & res_columns,
    EntityType type,
    const SettingsCollection & settings,
    std::string_view source,
    const SettingsHistory & history)
{
    for (const auto & name : settings.getAllRegisteredNames())
    {
        /// Obsolete settings carry the placeholder description "Obsolete setting, does nothing." and have
        /// no documentation value on a help surface, so they are not exposed.
        const auto tier = settings.getTier(name);
        if (tier == SettingsTierType::OBSOLETE)
            continue;
        addRow(res_columns, type, String(name),
            renderSettingDoc(settings.getDescription(name), settings.getTypeName(name), settings.getDefaultValueString(name), tier,
                findSettingHistory(history.by_setting, name)),
            source);
    }
}

/// Settings can have aliases (e.g. `enable_analyzer` for `allow_experimental_analyzer`). As for the other
/// entities with aliases, the alias is rendered as a reference to the canonical setting rather than
/// duplicating its documentation. An alias is introduced in a particular version and can have a history of its
/// own, distinct from the history of the setting it resolves to, so it is appended when there is one.
template <typename SettingsCollection>
void addSettingAliases(
    MutableColumns & res_columns,
    EntityType type,
    const SettingsCollection & settings,
    std::string_view source,
    const SettingsHistory & history)
{
    for (const auto & alias : settings.getAllAliasNames())
    {
        /// `getTier` resolves the alias to its canonical setting; skip aliases of obsolete settings,
        /// consistent with the canonical settings, which are not exposed either.
        if (settings.getTier(alias) == SettingsTierType::OBSOLETE)
            continue;
        String description = "Alias of `" + String(SettingsCollection::resolveName(alias)) + "`.";
        if (const auto * alias_history = findSettingHistory(history.by_alias, alias))
            appendSettingHistory(description, *alias_history, /* documenting_an_alias = */ true);
        addRow(res_columns, type, String(alias), description, source);
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

    const SettingsHistory settings_history = buildSettingsHistory(Settings{}, getSettingsChangesHistory());
    const SettingsHistory merge_tree_settings_history = buildSettingsHistory(MergeTreeSettings{}, getMergeTreeSettingsChangesHistory());
    /// Server settings are not covered by the `compatibility` setting, so no history of their changes is recorded.
    const SettingsHistory server_settings_history;

    addSettingsLike(res_columns, EntityType::Setting, Settings{}, SETTINGS_SOURCE, settings_history);
    addSettingAliases(res_columns, EntityType::Setting, Settings{}, SETTINGS_SOURCE, settings_history);
    addSettingsLike(res_columns, EntityType::MergeTreeSetting, MergeTreeSettings{}, MERGE_TREE_SETTINGS_SOURCE, merge_tree_settings_history);
    addSettingAliases(res_columns, EntityType::MergeTreeSetting, MergeTreeSettings{}, MERGE_TREE_SETTINGS_SOURCE, merge_tree_settings_history);
    addSettingsLike(res_columns, EntityType::ServerSetting, ServerSettings{}, SERVER_SETTINGS_SOURCE, server_settings_history);

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

    /// SQL statements are documented by the parsers which parse them; the registry is filled by `registerStatements`.
    addDocumented(res_columns, EntityType::Statement, StatementFactory::instance());

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
