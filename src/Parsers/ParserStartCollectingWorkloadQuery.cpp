#include <Parsers/ParserStartCollectingWorkloadQuery.h>
#include <Parsers/ASTStartCollectingWorkloadQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserStartCollectingWorkloadQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & /* expected */)
{
    ParserKeyword s_start(Keyword::START);
    ParserKeyword s_collecting(Keyword::COLLECTING);
    ParserKeyword s_workload(Keyword::WORKLOAD);

    if (!s_start.ignore(pos) || !s_collecting.ignore(pos) || !s_workload.ignore(pos))
        return false;

    node = std::make_shared<ASTStartCollectingWorkloadQuery>();
    return true;
}

}
