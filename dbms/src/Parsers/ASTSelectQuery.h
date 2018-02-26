#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Core/Names.h>


namespace DB
{

struct ASTTablesInSelectQueryElement;


/** SELECT query
  */
class ASTSelectQuery : public ASTQueryWithOutput
{
public:
    /** Get the text that identifies this element. */
    String getID() const override { return "SelectQuery"; };

    /// Check for the presence of the `arrayJoin` function. (Not capital `ARRAY JOIN`.)
    static bool hasArrayJoin(const ASTPtr & ast);

    /// Does the query contain an asterisk?
    bool hasAsterisk() const;

    /// Rename the query columns to the same names as in the original query.
    void renameColumns(const ASTSelectQuery & source);

    /// Rewrites select_expression_list to return only the required columns in the correct order.
    void rewriteSelectExpressionList(const Names & required_column_names);

    bool isUnionAllHead() const { return (prev_union_all == nullptr) && next_union_all != nullptr; }

    ASTPtr clone() const override;

    /// Get a deep copy of the first SELECT query tree.
    std::shared_ptr<ASTSelectQuery> cloneFirstSelect() const;

private:
    std::shared_ptr<ASTSelectQuery> cloneImpl(bool traverse_union_all) const;

public:
    bool distinct = false;
    ASTPtr with_expression_list;
    ASTPtr select_expression_list;
    ASTPtr tables;
    ASTPtr prewhere_expression;
    ASTPtr where_expression;
    ASTPtr group_expression_list;
    bool group_by_with_totals = false;
    ASTPtr having_expression;
    ASTPtr order_expression_list;
    ASTPtr limit_by_value;
    ASTPtr limit_by_expression_list;
    ASTPtr limit_offset;
    ASTPtr limit_length;
    ASTPtr settings;

    /// Compatibility with old parser of tables list. TODO remove
    ASTPtr database() const;
    ASTPtr table() const;
    ASTPtr sample_size() const;
    ASTPtr sample_offset() const;
    ASTPtr array_join_expression_list() const;
    const ASTTablesInSelectQueryElement * join() const;
    bool array_join_is_left() const;
    bool final() const;
    void setDatabaseIfNeeded(const String & database_name);
    void replaceDatabaseAndTable(const String & database_name, const String & table_name);

    /// A double-linked list of SELECT queries inside a UNION ALL query.

    /// The next SELECT query in the UNION ALL chain, if there is one
    ASTPtr next_union_all;
    /// Previous SELECT query in the UNION ALL chain (not inserted into children and not cloned)
    /// The pointer is null for the following reasons:
    /// 1. to prevent the occurrence of cyclic dependencies and, hence, memory leaks;
    IAST * prev_union_all = nullptr;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
