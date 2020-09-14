#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class TruncateQuery : public DDLQuery
{
    public:
        TruncateQuery(bool temporary, bool if_exists, PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        const bool temporary, if_exists;
};

}
