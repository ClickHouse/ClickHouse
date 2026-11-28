#pragma once

#include <base/types.h>
#include <Common/MultiVersion.h>

#include <array>
#include <atomic>
#include <functional>
#include <optional>
#include <string_view>
#include <unordered_set>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

/// Server-wide allow or deny lists of names for factories, read from the `factory_restrictions` config section,
/// for example `factory_restrictions.table_engines.deny.URL`.
/// A factory without a section allows everything. Names are matched case-insensitively.
class FactoryRestrictions
{
public:
    enum class Kind : uint8_t
    {
        Formats,
        TableEngines,
        TableFunctions,
        DatabaseEngines,
        DictionarySources,
        DictionaryLayouts,
        Disks,
        BackupEngines,
        Functions,
        AggregateFunctions,
        AggregateFunctionCombinators,
        DataTypes,
        CompressionCodecs,
        SkipIndexes,
        Statistics,
        MAX
    };

    struct Rule
    {
        bool is_allow_list = false;
        std::unordered_set<String> names; /// Lowercase.
    };

    struct Rules
    {
        std::array<std::optional<Rule>, static_cast<size_t>(Kind::MAX)> rules;
    };

    using RulesPtr = std::unique_ptr<const Rules>;

    static FactoryRestrictions & instance();

    /// Whether a name is registered in the factory of a kind, see `isRegisteredInFactory`.
    using IsRegisteredFn = std::function<bool(Kind kind, const String & name)>;

    /// Parses and validates the `factory_restrictions` section, throws on an invalid one,
    /// including a name that `is_registered` does not know. Returns nullptr if the section is absent.
    static RulesPtr parse(const Poco::Util::AbstractConfiguration & config, const IsRegisteredFn & is_registered);

    void set(RulesPtr new_rules);

    /// Throws `SUPPORT_IS_DISABLED` unless the object is allowed. An object matches a list
    /// if either the requested `name` or its `canonical_name` (after resolving aliases) is in it.
    void check(Kind kind, std::string_view name, std::string_view canonical_name = {}) const;

    /// Whether there is a list for `kind`, to skip preparing the names to check when there is none.
    bool hasRule(Kind kind) const;

private:
    std::atomic_bool has_rules = false;
    MultiVersion<Rules> rules;
};

}
