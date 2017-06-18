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
    ASTSelectQuery() = default;
    ASTSelectQuery(const StringRange range_);

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

    ASTPtr clone() const override;
private:
    std::shared_ptr<ASTSelectQuery> cloneImpl(bool traverse_union_all) const;

public:
    bool distinct = false;
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

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
