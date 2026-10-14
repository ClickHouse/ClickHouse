#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

ASTPtr ASTSelectIntersectExceptQuery::clone() const
{
    auto res = make_intrusive<ASTSelectIntersectExceptQuery>(*this);

    res->children.clear();
    for (const auto & child : children)
        res->children.push_back(child->clone());

    res->final_operator = final_operator;
    return res;
}

void ASTSelectIntersectExceptQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            ostr << settings.nl_or_ws << indent_str
                          << fromOperator(final_operator)

                          << settings.nl_or_ws;
        }

        /// Wrap compound children in parentheses: `UNION` has lower precedence than
        /// `INTERSECT`/`EXCEPT`, and we also keep nested `INTERSECT`/`EXCEPT` explicit.
        bool need_parens = (*it)->as<ASTSelectWithUnionQuery>() != nullptr
            || (*it)->as<ASTSelectIntersectExceptQuery>() != nullptr;

        if (need_parens)
        {
            ostr << indent_str;
            auto subquery = make_intrusive<ASTSubquery>(*it);
            subquery->format(ostr, settings, state, frame);
        }
        else
        {
            (*it)->format(ostr, settings, state, frame);
        }
    }
}

ASTs ASTSelectIntersectExceptQuery::getListOfSelects() const
{
    /**
     * Because of normalization actual number of selects is 2.
     * But this is checked in InterpreterSelectIntersectExceptQuery.
     */
    ASTs selects;
    for (const auto & child : children)
    {
        if (typeid_cast<ASTSelectQuery *>(child.get())
            || typeid_cast<ASTSelectWithUnionQuery *>(child.get())
            || typeid_cast<ASTSelectIntersectExceptQuery *>(child.get()))
            selects.push_back(child);
    }
    return selects;
}

const char * ASTSelectIntersectExceptQuery::fromOperator(Operator op)
{
    switch (op)
    {
        case Operator::EXCEPT_ALL:
            return "EXCEPT ALL";
        case Operator::EXCEPT_DISTINCT:
            return "EXCEPT DISTINCT";
        case Operator::INTERSECT_ALL:
            return "INTERSECT ALL";
        case Operator::INTERSECT_DISTINCT:
            return "INTERSECT DISTINCT";
        default:
            return "";
    }
}
}
