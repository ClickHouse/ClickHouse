#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

void DDLQuery::setOnCluster(PtrTo<Identifier> identifier)
{
    // FIXME: assert(!cluster);
    cluster = identifier;
    children.push_back(cluster);
}

}
