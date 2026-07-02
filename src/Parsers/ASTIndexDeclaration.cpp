#include <Parsers/ASTIndexDeclaration.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// True when `str` starts with a `(` whose matching `)` is not the final character (the leading
/// parenthesis does not enclose the whole expression). Parentheses inside string / quoted-identifier
/// literals are ignored (ClickHouse backslash-escapes the closing quote in formatted output).
bool leadingParenClosesEarly(std::string_view str)
{
    if (str.empty() || str.front() != '(')
        return false;

    int depth = 0;
    for (size_t i = 0; i < str.size(); ++i)
    {
        const char c = str[i];
        if (c == '\'' || c == '"' || c == '`')
        {
            /// Skip a quoted literal; ClickHouse escapes the closing quote with a backslash.
            const char quote = c;
            for (++i; i < str.size(); ++i)
            {
                if (str[i] == '\\')
                    ++i;
                else if (str[i] == quote)
                    break;
            }
            continue;
        }
        if (c == '(')
            ++depth;
        else if (c == ')' && --depth == 0)
            return i + 1 != str.size();
    }
    return false;
}

}


ASTIndexDeclaration::ASTIndexDeclaration(ASTPtr expression, ASTPtr type, const String & name_)
    : name(name_)
{
    if (!expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration must have an expression");
    children.push_back(expression);

    if (type)
    {
        if (!dynamic_cast<const ASTFunction *>(type.get()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration type must be a function");
        children.push_back(type);
    }
}

ASTPtr ASTIndexDeclaration::clone() const
{
    ASTPtr expr = getExpression();
    if (expr)
        expr = expr->clone();

    ASTPtr type = getType();
    if (type)
        type = type->clone();

    auto res = make_intrusive<ASTIndexDeclaration>(expr, type, name);
    res->granularity = granularity;

    return res;
}

ASTPtr ASTIndexDeclaration::getExpression() const
{
    if (children.size() <= expression_idx)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration must have an expression");
    return children[expression_idx];
}

boost::intrusive_ptr<ASTFunction> ASTIndexDeclaration::getType() const
{
    if (children.size() <= type_idx)
        return nullptr;
    auto func_ast = boost::dynamic_pointer_cast<ASTFunction>(children[type_idx]);
    if (!func_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration type must be a function");
    return func_ast;
}

void ASTIndexDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (auto expr = getExpression())
    {
        auto nested_frame = frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(expr.get()); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;

        if (part_of_create_index_query)
        {
            if (expr->as<ASTExpressionList>())
            {
                ostr << "(";
                expr->format(ostr, s, state, nested_frame);
                ostr << ")";
            }
            else
            {
                /// The parser consumes one leading `(` as the index's own bracket. If the single
                /// expression formats to a leading `(` that closes before the end (`(a, b).1`,
                /// `(x, y) -> x`, `(a + b) * c`), re-wrap it so the re-parse does not swallow that
                /// `(` as the index bracket and drop the trailing operator. Forms already enclosed
                /// by their leading `(` (`(a, b)`, `(expr AS alias)`) are left as is.
                WriteBufferFromOwnString expr_buf;
                expr->format(expr_buf, s, state, nested_frame);
                const auto expr_str = expr_buf.stringView();
                if (leadingParenClosesEarly(expr_str))
                    ostr << "(" << expr_str << ")";
                else
                    ostr << expr_str;
            }
        }
        else
        {
            s.writeIdentifier(ostr, name, /*ambiguous=*/false);
            ostr << " ";
            expr->format(ostr, s, state, nested_frame);
        }
    }

    if (auto type = getType())
    {
        ostr << " TYPE ";
        type->format(ostr, s, state, frame);
    }

    /// Always emit so AST round-trip is invariant for every granularity (zero included).
    ostr << " GRANULARITY " << granularity;
}

UInt64 getSecondaryIndexGranularity(const boost::intrusive_ptr<ASTFunction> & type, const ASTPtr & granularity)
{
    /// Text index is always built for the whole part and granularity is ignored.
    if (type && type->name == "text")
        return ASTIndexDeclaration::DEFAULT_TEXT_INDEX_GRANULARITY;

    if (granularity)
        return granularity->as<ASTLiteral &>().value.safeGet<UInt64>();

    if (type && type->name == "vector_similarity")
        return ASTIndexDeclaration::DEFAULT_VECTOR_SIMILARITY_INDEX_GRANULARITY;

    return ASTIndexDeclaration::DEFAULT_INDEX_GRANULARITY;
}

}
