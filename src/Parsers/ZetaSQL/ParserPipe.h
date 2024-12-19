#include "Parsers/IAST_fwd.h"
#include "Parsers/IParserBase.h"
namespace DB::ZetaSQL {
class ParserPipe : public IParserBase
{
    protected:
        const char * getName() const override { return "Pipe Query"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
