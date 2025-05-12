#include <Parsers/IParserBase.h>

namespace DB
{

class ParserFinishCollectingWorkloadQuery : public IParserBase
{
public:
    const char * getName() const override { return "FINISH COLLECTING WORKLOAD"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & /* expected */) override;
};

}
