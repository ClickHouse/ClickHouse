#pragma once

#include <list>
#include <unordered_set>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

class AddAliasToAnonymousSubqueryVisitor
{
private:
    std::unordered_set<String> aliases;
    std::list<IAST *> anonymous_subqueries;
    void visitImpl(IAST *);

public:
    void visit(IAST * ast);
};

}
