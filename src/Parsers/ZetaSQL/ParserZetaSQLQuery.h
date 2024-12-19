#include "Parsers/IAST_fwd.h"
#include "Parsers/IParserBase.h"
namespace DB::ZetaSQL {
class ParserZetaSQLQuery : public IParserBase
{
    protected:
        const char * getName() const override { return "Piped Query"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
