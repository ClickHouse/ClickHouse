#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class UseQuery : public Query
{
    public:
        explicit UseQuery(PtrTo<DatabaseIdentifier> identifier);
};

}
