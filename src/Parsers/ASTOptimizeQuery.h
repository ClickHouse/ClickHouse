#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/SipHash.h>

namespace Poco::JSON { class Object; }

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
    /// Compact manifests only (for Iceberg tables)
    bool manifest = false;
    /** Get the text that identifies this element. */
    String getID(char delim) const override
    {
        return "OptimizeQuery" + (delim + getDatabase()) + delim + getTable() + (final ? "_final" : "") + (deduplicate ? "_deduplicate" : "") + (cleanup ? "_cleanup" : "") + (dry_run ? "_dry_run" : "") + (manifest ? "_manifest" : "");
    }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTOptimizeQuery>(*this);
        res->children.clear();

        /// `ParserOptimizeQuery` adds the children in this order: the partition, the parts list,
        /// then the database/table, and `ParserQueryWithOutput` appends the output options last.
        /// `deduplicate_by_columns` is not a child: the parser puts it into the member only.
        /// Reproduce that shape so the clone has the same tree hash.
        if (partition)
        {
            res->partition = partition->clone();
            res->children.push_back(res->partition);
        }

        if (parts_list)
        {
            res->parts_list = parts_list->clone();
            res->children.push_back(res->parts_list);
        }

        if (deduplicate_by_columns)
            res->deduplicate_by_columns = deduplicate_by_columns->clone();

        cloneTableOptions(*res);
        cloneOutputOptions(*res);

        return res;
    }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override
    {
        /// `deduplicate_by_columns` and `cluster` are not children, so without hashing them
        /// `OPTIMIZE TABLE t DEDUPLICATE BY a` and `... BY b`, or `ON CLUSTER c1` and `... c2`,
        /// would hash the same. The remaining non-child members (`final`, `deduplicate`,
        /// `cleanup`, `dry_run`, `manifest`) are part of `getID`, which the default implementation
        /// hashes.
        hash_state.update(deduplicate_by_columns != nullptr);
        if (deduplicate_by_columns)
            deduplicate_by_columns->updateTreeHash(hash_state, ignore_aliases);
        hash_state.update(cluster.size());
        hash_state.update(cluster);
        ASTQueryWithTableAndOutput::updateTreeHashImpl(hash_state, ignore_aliases);
    }

    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTOptimizeQuery>(clone(), params.default_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::Optimize; }
};

}
