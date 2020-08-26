#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class DetachQuery : public DDLQuery
{
    public:
        DetachQuery(bool if_exists, PtrTo<TableIdentifier> identifier);

    private:
        const bool if_exists;
};

}
