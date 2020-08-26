#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class CreateViewQuery : public DDLQuery
{
    public:
        CreateViewQuery(bool if_not_exists, PtrTo<TableIdentifier> identifier, PtrTo<SelectUnionQuery> query);

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            SUBQUERY = 1,
        };

        const bool if_not_exists;
};

}
