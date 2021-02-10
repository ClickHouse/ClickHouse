#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ProjectionCondition
{
public:
    ProjectionCondition(const Names & key_column_names, const Names & required_column_names);

    /// Check if given predicate can be evaluated by `key_columns`.
    bool check(const ASTPtr & node);

    Names getRequiredColumns() const;

    void rewrite(ASTPtr & node) const;

private:
    std::unordered_map<std::string, size_t> key_columns;
};

}
