#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class TruncateQuery : public DDLQuery
{
    public:
        TruncateQuery(bool temporary, bool if_exists, PtrTo<TableIdentifier> identifier);

    private:
        const bool temporary, if_exists;
};

}
