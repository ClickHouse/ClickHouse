#pragma once

#include <ostream>
#include <optional>

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
    /// The first parts of the compound identifiers, to mask the subcolumns of aliases, which can be defined after them.
    std::unordered_map<String, String> compound_identifier_prefixes;

    bool addColumnAliasIfAny(const IAST & ast);
    void addColumnIdentifier(const ASTIdentifier & node);
    bool addArrayJoinAliasIfAny(const IAST & ast);
    void addArrayJoinIdentifier(const ASTIdentifier & node);

    NameSet requiredColumns() const;
    size_t nameInclusion(const String & name) const;
};

std::ostream & operator << (std::ostream & os, const RequiredSourceColumnsData & cols);

}
