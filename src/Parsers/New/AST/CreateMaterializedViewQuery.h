#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class CreateMaterializedViewQuery : public DDLQuery
{
    public:
        CreateMaterializedViewQuery(
            bool if_not_exists, PtrTo<TableIdentifier> identifier, PtrTo<EngineExpr> engine, PtrTo<SelectUnionQuery> query);

    private:
        const bool if_not_exists;
};

}
