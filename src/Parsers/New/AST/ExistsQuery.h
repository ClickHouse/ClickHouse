#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ExistsQuery : public Query
{
    public:
        enum class QueryType
        {
            DICTIONARY,
            TABLE,
            VIEW,
            DATABASE,
        };

        static PtrTo<ExistsQuery> createTable(QueryType type, bool temporary, PtrTo<TableIdentifier> identifier);
        static PtrTo<ExistsQuery> createDatabase(PtrTo<DatabaseIdentifier> identifier);

        ExistsQuery(QueryType type, bool temporary, PtrList exprs);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            IDENTIFIER = 0,  // DatabaseIdentifier or TableIdentifier
        };

        const QueryType query_type;
        const bool temporary;
};

}
