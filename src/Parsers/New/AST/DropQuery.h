#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class DropQuery : public DDLQuery
{
    public:
        static PtrTo<DropQuery>
        createDropDatabase(bool detach, bool if_exists, PtrTo<DatabaseIdentifier> identifier, PtrTo<ClusterClause> cluster);
        static PtrTo<DropQuery>
        createDropTable(bool detach, bool if_exists, bool temporary, PtrTo<TableIdentifier> identifier, PtrTo<ClusterClause> cluster);
        static PtrTo<DropQuery>
        createDropDictionary(bool detach, bool if_exists, PtrTo<TableIdentifier> identifier, PtrTo<ClusterClause> cluster);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
        };

        enum class QueryType
        {
            DATABASE,
            DICTIONARY,
            TABLE,
        };

        const QueryType query_type;

        bool detach = false;
        bool if_exists = false;
        bool temporary = false;

        DropQuery(PtrTo<ClusterClause> cluster, QueryType type, PtrList exprs);
};

}
