#include <Parsers/ASTIntersectOrExcept.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

ASTPtr ASTIntersectOrExcept::clone() const
{
    auto res = std::make_shared<ASTIntersectOrExcept>(*this);
    res->children.clear();
    res->children.push_back(children[0]->clone());
    res->children.push_back(children[1]->clone());
    res->is_except = is_except;
    cloneOutputOptions(*res);
    return res;
}

void ASTIntersectOrExcept::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children[0]->formatImpl(settings, state, frame);
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
    settings.ostr << settings.nl_or_ws << indent_str << (settings.hilite ? hilite_keyword : "")
                  << (is_except ? "EXCEPT" : "INTERSECT")
                  << (settings.hilite ? hilite_none : "") << settings.nl_or_ws;
    children[1]->formatImpl(settings, state, frame);
}

}
