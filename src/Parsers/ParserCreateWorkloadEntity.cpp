#include <Parsers/ParserCreateWorkloadEntity.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>

namespace DB
{

bool ParserCreateWorkloadEntity::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserCreateWorkloadQuery create_workload_p;
    ParserCreateResourceQuery create_resource_p;

    return create_workload_p.parse(pos, node, expected) || create_resource_p.parse(pos, node, expected);
}

}
