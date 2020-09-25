#pragma once

#include <Parsers/New/AST/Query.h>

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

class DDLQuery : public Query
{
    protected:
        DDLQuery() = default;
        DDLQuery(std::initializer_list<Ptr> list) : Query(list) {}
        explicit DDLQuery(PtrList list) : Query(list) {}
};

}
