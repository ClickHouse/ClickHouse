#include "Parsers/ZetaSQL/ASTZetaSQLQuery.h"
#include <memory>
#include <Parsers/GoogleSQL/ASTPipelineQuery.h>
#include "Parsers/IAST_fwd.h"
namespace DB::ZetaSQL
{

ASTPtr ASTZetaSQLQuery::clone() const
{
    auto res = std::make_shared<ASTZetaSQLQuery>(*this);
    res->children = children;
    return std::static_pointer_cast<IAST>(res);
}
};
