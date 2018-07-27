#pragma once

#include <Parsers/IAST.h>
#include <Core/Names.h>


namespace DB
{

struct ASTTablesInSelectQueryElement;


/** SELECT query
  */
class ASTSelectQuery : public IAST
{
public:
    /** Get the text that identifies this element. */
    String getID() const override { return "SelectQuery"; }

    ASTPtr clone() const override;

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
    void addTableFunction(ASTPtr & table_function_ptr);

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
