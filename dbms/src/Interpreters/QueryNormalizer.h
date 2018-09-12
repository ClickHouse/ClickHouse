#pragma once

#include <Parsers/IAST.h>
#include "Settings.h"

namespace DB
{

class QueryNormalizer
{
public:
    using Aliases = std::unordered_map<String, ASTPtr>;

    QueryNormalizer(ASTPtr & query, const Aliases & aliases, const Settings & settings, const Names & all_columns_name);

    void perform();

private:
    using SetOfASTs = std::set<const IAST *>;
    using MapOfASTs = std::map<ASTPtr, ASTPtr>;

    ASTPtr & query;
    const Aliases & aliases;
    const Settings & settings;
    const Names & all_columns_name;

    void performImpl(ASTPtr &ast, MapOfASTs &finished_asts, SetOfASTs &current_asts, std::string current_alias, size_t level);
};

}
