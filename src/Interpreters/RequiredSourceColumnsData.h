#pragma once

#include <ostream>
#include <optional>
#include <unordered_set>
#include <vector>

#include <Core/Names.h>
#include <base/types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTIdentifier;

/// Information about table and column names extracted from ASTSelectQuery block. Do not include info from subselects.
struct RequiredSourceColumnsData
{
    struct NameInfo
    {
        std::set<String> aliases;
        size_t appears = 0;

        void addInclusion(const String & alias)
        {
            if (!alias.empty())
                aliases.insert(alias);
            ++appears;
        }
    };

    std::unordered_map<String, NameInfo> required_names;
    NameSet private_aliases; /// lambda aliases that should not be interpreted as required columns
    NameSet complex_aliases; /// aliases to functions results: they are not required cause calculated by query itself
    NameSet masked_columns;  /// columns names masked by function aliases: we still need them in required columns
    NameSet array_join_columns; /// Tech debt: we exclude ArrayJoin columns from general logic cause they have own logic outside

    bool has_table_join = false;
    bool has_array_join = false;
    /// Do not interpret the subcolumns of lambda arguments and of aliases, e.g. `a.x` in `a -> a.x` or in `(tuple(1) AS a).x + a.x`,
    /// as required columns either.
    bool mask_subcolumns_of_aliases = false;
    /// A compound identifier with the lambda scopes it is in, to mask the subcolumns of aliases, which can be defined after it.
    struct CompoundIdentifier
    {
        String prefix;
        std::vector<size_t> scopes;
    };
    std::unordered_map<String, std::vector<CompoundIdentifier>> compound_identifiers;
    /// The lambda scopes the aliases are defined in: an alias is visible in its scope and the scopes nested in it. 0 is the top level.
    std::unordered_map<String, std::unordered_set<size_t>> alias_scopes;
    /// The lambda scopes the visitor is in, starting with the top level.
    std::vector<size_t> current_scopes{0};
    size_t scopes_count = 1;

    bool addColumnAliasIfAny(const IAST & ast);
    void addColumnIdentifier(const ASTIdentifier & node);
    bool addArrayJoinAliasIfAny(const IAST & ast);
    void addArrayJoinIdentifier(const ASTIdentifier & node);

    [[nodiscard]] NameSet requiredColumns() const;
    /// Whether all `appears` appearances of `name` are subcolumns of aliases visible from where they are, e.g. `a.x` in `(tuple(1) AS a).x`.
    [[nodiscard]] bool isSubcolumnOfAliases(const String & name, size_t appears) const;
    [[nodiscard]] size_t nameInclusion(const String & name) const;
};

std::ostream & operator << (std::ostream & os, const RequiredSourceColumnsData & cols);

}
