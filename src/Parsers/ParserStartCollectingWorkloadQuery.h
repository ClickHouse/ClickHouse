#include <Parsers/IParserBase.h>

namespace DB
{

class ParserStartCollectingWorkloadQuery : public IParserBase
{
public:
    const char * getName() const override { return "START COLLECTING WORKLOAD"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & /* expected */) override;
};

}
