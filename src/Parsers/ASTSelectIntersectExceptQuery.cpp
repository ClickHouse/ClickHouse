#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


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

        (*it)->format(ostr, settings, state, frame);
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

void ASTSelectIntersectExceptQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "SelectIntersectExceptQuery");
    w.writeString("final_operator", fromOperator(final_operator));
    w.writeChildren(children);
}

static ASTSelectIntersectExceptQuery::Operator parseOperator(const String & str)
{
    if (str == "EXCEPT ALL")
        return ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL;
    if (str == "EXCEPT DISTINCT")
        return ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT;
    if (str == "INTERSECT ALL")
        return ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL;
    if (str == "INTERSECT DISTINCT")
        return ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT;
    return ASTSelectIntersectExceptQuery::Operator::UNKNOWN;
}

void ASTSelectIntersectExceptQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    final_operator = parseOperator(r.getString("final_operator"));
    children = r.readChildren();
}
}
