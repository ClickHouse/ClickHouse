#include "Parsers/IAST_fwd.h"
#include "Parsers/IParserBase.h"
namespace DB::ZetaSQL {
class ParserWhereClause : public IParserBase
{
    protected:
        const char * getName() const override { return "Where Clause"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
