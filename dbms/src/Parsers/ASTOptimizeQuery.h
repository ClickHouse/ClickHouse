#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{


/** OPTIMIZE query
  */
class ASTOptimizeQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    String database;
    String table;

    /// The partition to optimize can be specified.
    ASTPtr partition;
    /// A flag can be specified - perform optimization "to the end" instead of one step.
    bool final;
    /// Do deduplicate (default: false)
    bool deduplicate;

    /** Get the text that identifies this element. */
    String getID() const override
    { return "OptimizeQuery_" + database + "_" + table + (final ? "_final" : "") + (deduplicate ? "_deduplicate" : ""); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTOptimizeQuery>(*this);
        res->children.clear();

        if (partition)
        {
            res->partition = partition->clone();
            res->children.push_back(res->partition);
        }

        return res;
    }

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &new_database) const override;

};

}
