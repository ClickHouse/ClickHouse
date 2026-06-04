#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

    if (!r.has("final_operator"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'final_operator' field in `SelectIntersectExceptQuery` during AST JSON deserialization");

    const String final_operator_str = r.getString("final_operator");
    final_operator = parseOperator(final_operator_str);
    if (final_operator == Operator::UNKNOWN)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown 'final_operator' value '{}' in `SelectIntersectExceptQuery` during AST JSON deserialization", final_operator_str);

    children = r.readChildren();

    /// An `INTERSECT`/`EXCEPT` query requires at least two select operands.
    if (children.size() < 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected at least two select operands in `SelectIntersectExceptQuery`, got {}, during AST JSON deserialization", children.size());
}
}
