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
        };

        ExistsQuery(QueryType type, bool temporary, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,  // TableIdentifier
        };

        const QueryType query_type;
        const bool temporary;
};

}
