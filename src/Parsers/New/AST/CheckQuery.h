#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class CheckQuery : public Query
{
    public:
        explicit CheckQuery(PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // TableIdentifier
        };
};

}
