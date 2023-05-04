#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>

namespace DB
{

class ReadBuffer;

/// INSERT query
class ASTInsertQuery : public IAST
{
public:
    StorageID table_id = StorageID::createEmpty();

    ASTPtr database;
    ASTPtr table;

    ASTPtr columns;
    String format;
    ASTPtr table_function;
    ASTPtr partition_by;
    ASTPtr settings_ast;

    ASTPtr select;
    ASTPtr watch;
    ASTPtr infile;
    ASTPtr compression;

    /// Data inlined into query
    const char * data = nullptr;
    const char * end = nullptr;

    /// Data from buffer to insert after inlined one - may be nullptr.
    ReadBuffer * tail = nullptr;

    String getDatabase() const;
    String getTable() const;

    void setDatabase(const String & name);
    void setTable(const String & name);

    bool hasInlinedData() const { return data || tail; }

    /// Try to find table function input() in SELECT part
    void tryFindInputFunction(ASTPtr & input_function) const;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "InsertQuery" + (delim + table_id.database_name) + delim + table_id.table_name; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTInsertQuery>(*this);
        res->children.clear();

        if (database) { res->database = database->clone(); res->children.push_back(res->database); }
        if (table) { res->table = table->clone(); res->children.push_back(res->table); }
        if (columns) { res->columns = columns->clone(); res->children.push_back(res->columns); }
        if (select) { res->select = select->clone(); res->children.push_back(res->select); }
        if (watch) { res->watch = watch->clone(); res->children.push_back(res->watch); }
        if (table_function) { res->table_function = table_function->clone(); res->children.push_back(res->table_function); }
        if (partition_by) { res->partition_by = partition_by->clone(); res->children.push_back(res->partition_by); }
        if (settings_ast) { res->settings_ast = settings_ast->clone(); res->children.push_back(res->settings_ast); }

        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Insert; }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void updateTreeHashImpl(SipHash & hash_state) const override;
};

}
