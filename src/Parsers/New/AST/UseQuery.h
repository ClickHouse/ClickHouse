#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class UseQuery : public Query
{
    public:
        explicit UseQuery(PtrTo<DatabaseIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            DATABASE = 0,
        };
};

}
