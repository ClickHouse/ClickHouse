#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class CheckQuery : public Query
{
    public:
        explicit CheckQuery(PtrTo<TableIdentifier> identifier);
};

}
