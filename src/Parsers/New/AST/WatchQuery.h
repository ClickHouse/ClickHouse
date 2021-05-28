#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class WatchQuery : public Query
{
    public:
        WatchQuery(bool events, PtrTo<TableIdentifier> identifier, PtrTo<NumberLiteral> literal);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,  // TableIdentifier
            LIMIT = 1,  // NumberLiteral (optional)
        };

        const bool events;
};

}
