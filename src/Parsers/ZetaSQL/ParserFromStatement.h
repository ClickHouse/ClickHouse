#include "Parsers/IParserBase.h"
namespace DB::ZetaSQL
{
class ParserFromStatement : public IParserBase
{
    protected:
        const char * getName() const override { return "From Clause"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
