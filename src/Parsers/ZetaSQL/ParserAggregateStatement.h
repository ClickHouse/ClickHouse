#include "Parsers/IParserBase.h"
namespace DB::ZetaSQL
{
class ParserAggregateStatement : public IParserBase
{
    protected:
        const char * getName() const override { return "Aggregation Operator"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
