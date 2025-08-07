#include <Parsers/ParserFinishCollectingWorkloadQuery.h>
#include <Parsers/ASTFinishCollectingWorkloadQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserFinishCollectingWorkloadQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & /* expected */)
{
    ParserKeyword s_finish(Keyword::FINISH);
    ParserKeyword s_collecting(Keyword::COLLECTING);
    ParserKeyword s_workload(Keyword::WORKLOAD);

    if (!s_finish.ignore(pos) || !s_collecting.ignore(pos) || !s_workload.ignore(pos))
        return false;

    node = std::make_shared<ASTFinishCollectingWorkloadQuery>();
    return true;
}

}
