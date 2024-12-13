
#include <memory>
#include "Parsers/IAST_fwd.h"
#include "Parsers/ZetaSQL/ASTZetaSQLQuery.h"
#include <Parsers/ASTSelectQuery.h>

namespace DB::ZetaSQL {
class Translator
{
public:
    ASTPtr translateAST(ASTZetaSQLQuery & zetasql_ast);
private:
    void processFromStage(std::shared_ptr<ASTSelectQuery>  select_query,  ASTZetaSQLQuery::PipelineStage & stage);
    void processWhereStage(std::shared_ptr<ASTSelectQuery>  select_query,  ASTZetaSQLQuery::PipelineStage & stage);
};
}
