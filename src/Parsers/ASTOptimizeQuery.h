#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{


/** OPTIMIZE query
  */
class ASTOptimizeQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    /// The partition to optimize can be specified.
    ASTPtr partition;
    /// A flag can be specified - perform optimization "to the end" instead of one step.
    bool final = false;
    /// Do deduplicate (default: false)
    bool deduplicate = false;
    /// Deduplicate by columns.
    ASTPtr deduplicate_by_columns;
    /// Delete 'is_deleted' data
    bool cleanup = false;
    /// Dry run mode: execute merge but do not commit the result
    bool dry_run = false;
    /// List of part names for DRY RUN (ASTExpressionList of ASTLiteral strings)
    ASTPtr parts_list;
    /** Get the text that identifies this element. */
    String getID(char delim) const override
    {
        return "OptimizeQuery" + (delim + getDatabase()) + delim + getTable() + (final ? "_final" : "") + (deduplicate ? "_deduplicate" : "") + (cleanup ? "_cleanup" : "") + (dry_run ? "_dry_run" : "");
    }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTOptimizeQuery>(*this);
        res->children.clear();

        if (partition)
        {
            res->partition = partition->clone();
            res->children.push_back(res->partition);
        }

        if (deduplicate_by_columns)
        {
            res->deduplicate_by_columns = deduplicate_by_columns->clone();
            res->children.push_back(res->deduplicate_by_columns);
        }

        if (parts_list)
        {
            res->parts_list = parts_list->clone();
            res->children.push_back(res->parts_list);
        }

        return res;
    }

    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTOptimizeQuery>(clone(), params.default_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::Optimize; }
};

}
