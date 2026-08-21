#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>

namespace DB
{

/** CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name ON [db.]table (expr) TYPE type(args) GRANULARITY n
  * DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table
  * DROP ALL HYPOTHETICAL INDEXES
  *
  * CREATE HYPOTHETICAL PROJECTION [IF NOT EXISTS] name ON [db.]table (SELECT ...)
  * DROP HYPOTHETICAL PROJECTION [IF EXISTS] name ON [db.]table
  * DROP ALL HYPOTHETICAL PROJECTIONS
  */
class ASTHypotheticalObjectQuery : public ASTQueryWithTableAndOutput
{
public:
    enum Kind
    {
        Create,
        Drop,
        DropAll,
    };

    /// Which kind of hypothetical object the statement is about
    enum ObjectKind
    {
        Index,
        Projection,
    };

    Kind kind = Create;
    ObjectKind object_kind = Index;

    ASTPtr index_decl;      /// ASTIndexDeclaration, for Create of an index
    ASTPtr projection_decl; /// ASTProjectionDeclaration, for Create of a projection
    ASTPtr object_name;     /// Index or projection name, for Create and Drop

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
