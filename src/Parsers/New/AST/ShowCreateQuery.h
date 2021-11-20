#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ShowCreateQuery : public Query
{
    public:
        static PtrTo<ShowCreateQuery> createDatabase(PtrTo<DatabaseIdentifier> identifier);
        static PtrTo<ShowCreateQuery> createDictionary(PtrTo<TableIdentifier> identifier);
        static PtrTo<ShowCreateQuery> createTable(bool temporary, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            IDENTIFIER = 0,  // DatabaseIdentifier or TableIdentifier
        };
        enum class QueryType
        {
            DATABASE,
            DICTIONARY,
            TABLE,
        };

        QueryType query_type;
        bool temporary = false;

        ShowCreateQuery(QueryType type, PtrList exprs);
};

}
