#include <Parsers/ASTWithAliasPending.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTQueryParameter.h>
#include <IO/Operators.h>

namespace DB
{

ASTWithAliasPending::ASTWithAliasPending(ASTPtr wrapped_, ASTPtr param_)
: wrapped_ast(wrapped_)
, query_parameter(param_)
{}

ASTPtr ASTWithAliasPending::clone() const
{
    const auto res = std::make_shared<ASTWithAliasPending>(*this);
    res->wrapped_ast = this->wrapped_ast->clone();
    res->query_parameter = this->query_parameter->clone();
    return res;
}

void ASTWithAliasPending::appendColumnNameImpl(WriteBuffer & ostr) const
{
    wrapped_ast->appendColumnName(ostr);
}

void ASTWithAliasPending::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    wrapped_ast->formatImpl(settings, state, frame);
}


}
