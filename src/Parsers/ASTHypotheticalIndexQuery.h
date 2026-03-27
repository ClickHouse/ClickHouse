#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>

namespace DB
{

/** CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name ON [db.]table (expr) TYPE type(args) GRANULARITY n
  * DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table
  * DROP ALL HYPOTHETICAL INDEXES
  */
class ASTHypotheticalIndexQuery : public ASTQueryWithTableAndOutput
{
public:
    enum Kind
    {
        Create,
        Drop,
        DropAll,
    };

    Kind kind;

    /// For Create: stores ASTIndexDeclaration
    ASTPtr index_decl;

    /// For Create and Drop: index name
    ASTPtr index_name;

    bool if_not_exists{false};
    bool if_exists{false};

    String getID(char delim) const override;
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override
    {
        return kind == Create ? QueryKind::Create : QueryKind::Drop;
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
