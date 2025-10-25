#include <Parsers/ASTParallelWithQuery.h>

#include <IO/Operators.h>


namespace DB
{

String ASTParallelWithQuery::getID(char delim) const
{
    return fmt::format("ParallelWithQuery{}{}{}", delim, children.size(), children.empty() ? "" : (delim + children[0]->getID()));
}

ASTPtr ASTParallelWithQuery::clone() const
{
    auto res = std::make_shared<ASTParallelWithQuery>(*this);
    for (auto & child : res->children)
        child = child->clone();
    return res;
}

void ASTParallelWithQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    chassert(!children.empty());
    for (size_t i = 0; i != children.size(); ++i)
    {
        if (i != 0)
        {
            String indent_str = settings.one_line ? "" : String(4 * frame.indent, ' ');
            ostr << settings.nl_or_ws << indent_str;
            ostr << "PARALLEL WITH";
            ostr << settings.nl_or_ws;
        }
        children[i]->format(ostr, settings, state, frame);
    }
}

}
