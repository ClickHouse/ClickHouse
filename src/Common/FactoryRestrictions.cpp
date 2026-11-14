#include <Common/FactoryRestrictions.h>
#include <Common/Exception.h>

#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr std::string_view config_root = "factory_restrictions";

struct KindInfo
{
    std::string_view section;
    std::string_view description;
};

constexpr std::array<KindInfo, static_cast<size_t>(FactoryRestrictions::Kind::MAX)> kind_infos{{
    {"formats", "Format"},
    {"table_engines", "Table engine"},
    {"table_functions", "Table function"},
    {"database_engines", "Database engine"},
    {"dictionary_sources", "Dictionary source"},
    {"dictionary_layouts", "Dictionary layout"},
    {"disks", "Disk type"},
    {"backup_engines", "Backup engine"},
    {"functions", "Function"},
    {"aggregate_functions", "Aggregate function"},
    {"aggregate_function_combinators", "Aggregate function combinator"},
    {"data_types", "Data type"},
    {"compression_codecs", "Compression codec"},
    {"skip_indexes", "Skip index type"},
    {"statistics", "Statistics type"},
}};

}

FactoryRestrictions & FactoryRestrictions::instance()
{
    static FactoryRestrictions instance;
    return instance;
}

FactoryRestrictions::RulesPtr FactoryRestrictions::parse(const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(String(config_root)))
        return nullptr;

    auto result = std::make_unique<Rules>();

    Poco::Util::AbstractConfiguration::Keys sections;
    config.keys(String(config_root), sections);
    for (const auto & section : sections)
    {
        const auto info = std::find_if(kind_infos.begin(), kind_infos.end(), [&](const auto & i) { return i.section == section; });
        if (info == kind_infos.end())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown or duplicate section `{}.{}`", config_root, section);

        String section_path = fmt::format("{}.{}", config_root, section);
        Poco::Util::AbstractConfiguration::Keys list_keys;
        config.keys(section_path, list_keys);
        if (list_keys.size() != 1 || (list_keys[0] != "allow" && list_keys[0] != "deny"))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Section `{}` must contain exactly one of `allow` or `deny`", section_path);

        Rule rule;
        rule.is_allow_list = list_keys[0] == "allow";

        String list_path = fmt::format("{}.{}", section_path, list_keys[0]);
        Poco::Util::AbstractConfiguration::Keys names;
        config.keys(list_path, names);
        for (const auto & key : names)
        {
            String key_path = fmt::format("{}.{}", list_path, key);
            Poco::Util::AbstractConfiguration::Keys children;
            config.keys(key_path, children);
            if (!children.empty() || !config.getString(key_path).empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}` must be an empty element, like `<Name/>`", key_path);

            /// A repeated element is reported by Poco as `Name[1]`.
            rule.names.insert(Poco::toLower(key.substr(0, key.find('['))));
        }

        result->rules[info - kind_infos.begin()] = std::move(rule);
    }

    return result;
}

void FactoryRestrictions::set(RulesPtr new_rules)
{
    /// `has_rules` is only a fast path, the rules themselves are synchronized by `MultiVersion`.
    /// The order of the stores lets a concurrent reader see either the old or the new state, never neither.
    bool any = new_rules != nullptr;
    if (any)
        has_rules.store(true, std::memory_order_relaxed);
    rules.set(std::move(new_rules));
    if (!any)
        has_rules.store(false, std::memory_order_relaxed);
}

void FactoryRestrictions::check(Kind kind, std::string_view name, std::string_view canonical_name) const
{
    if (!has_rules.load(std::memory_order_relaxed))
        return;

    auto current = rules.get();
    if (!current)
        return;

    const auto & rule = current->rules[static_cast<size_t>(kind)];
    if (!rule)
        return;

    bool matches = rule->names.contains(Poco::toLower(String(name)))
        || (!canonical_name.empty() && rule->names.contains(Poco::toLower(String(canonical_name))));
    if (matches == rule->is_allow_list)
        return;

    const auto & info = kind_infos[static_cast<size_t>(kind)];
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "{} `{}` is disabled by the server configuration (`{}.{}`)",
        info.description, name, config_root, info.section);
}

}
