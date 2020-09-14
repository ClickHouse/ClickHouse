#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ExistsQuery : public Query
{
    public:
        ExistsQuery(bool temporary, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        const bool temporary;
};

}
