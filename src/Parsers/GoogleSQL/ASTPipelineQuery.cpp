#include <memory>
#include <Parsers/GoogleSQL/ASTPipelineQuery.h>
#include "Parsers/IAST_fwd.h"
namespace DB::GoogleSQL
{

ASTPtr ASTPipelineQuery::clone() const
{
    auto res = std::make_shared<ASTPipelineQuery>(*this);
    res->children = children;
    return std::static_pointer_cast<IAST>(res);
}
};
