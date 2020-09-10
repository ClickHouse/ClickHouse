#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ShowCreateQuery : public Query
{
    public:
        static PtrTo<ShowCreateQuery> createDatabase(PtrTo<DatabaseIdentifier> identifier);
        static PtrTo<ShowCreateQuery> createTable(bool temporary, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum class QueryType
        {
            DATABASE,
            TABLE,
        };

        QueryType query_type;
        bool temporary;

        ShowCreateQuery(QueryType type, PtrList exprs);
};

}
