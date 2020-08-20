#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class DropQuery : public DDLQuery
{
    public:
        static PtrTo<DropQuery> createDropDatabase(bool if_exists, PtrTo<DatabaseIdentifier> identifier);
        static PtrTo<DropQuery> createDropTable(bool if_exists, bool temporary, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
        };

        enum class QueryType
        {
            DATABASE,
            TABLE,
        };

        const QueryType query_type;

        bool if_exists = false;
        bool temporary = false;

        DropQuery(QueryType type, PtrList exprs);
};

}
