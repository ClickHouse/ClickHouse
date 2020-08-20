#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class ShowCreateTableQuery : public Query
{
    public:
        ShowCreateTableQuery(bool temporary, PtrTo<TableIdentifier> identifier);

    private:
        const bool temporary;
};

}
