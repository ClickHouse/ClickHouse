#include <Parsers/ASTIndexDeclaration.h>

#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
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
    /// The flag selects the `CREATE INDEX` formatting; a clone must keep it so it formats identically
    /// to the original (otherwise `clone()->format()` diverges from `format()`).
    res->part_of_create_index_query = part_of_create_index_query;

    return res;
}

void ASTIndexDeclaration::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `name` and `granularity` are not children, so the default implementation does not see them.
    /// `part_of_create_index_query` only affects formatting and is deliberately not hashed.
    /// The expected size is for 64-bit targets; the layout differs on 32-bit ones (the wasm parser build).
    static_assert(sizeof(void *) != 8 || sizeof(*this) == 72, "If members were added to ASTIndexDeclaration, hash them here unless they are purely cosmetic.");
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(granularity);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
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

void ASTIndexDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "IndexDeclaration");
    w.writeString("name", name);
    w.writeUInt("granularity", granularity);
    if (part_of_create_index_query)
        w.writeBool("part_of_create_index_query", true);
    w.writeChild("expression", getExpression());
    w.writeChild("index_type", getType());
}

void ASTIndexDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    granularity = r.getUInt("granularity");
    part_of_create_index_query = r.getBool("part_of_create_index_query");

    auto expression = r.readChild("expression");
    if (!expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index declaration must have an expression during AST JSON deserialization");
    children.push_back(expression);

    auto index_type = r.readChild("index_type");
    if (index_type)
    {
        if (!dynamic_cast<const ASTFunction *>(index_type.get()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index declaration type must be a function during AST JSON deserialization");
        children.push_back(index_type);
    }
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
                /// The parser consumes one leading `(` as the index's own bracket, so re-wrap the
                /// single expression whenever the re-parse would otherwise not reconstruct the same
                /// AST. Three such cases:
                ///   - the canonical form starts with a `(` that closes before the end (`(a, b).1`,
                ///     `(x, y) -> x`, `(a + b) * c`), detected on the rendered text;
                ///   - a scalar subquery / VALUES, which formats as `(SELECT ...)`: the leading `(`
                ///     encloses the whole node, but `SELECT ...` is not a valid
                ///     `ParserOrderByExpressionList`, so a bare bracket cannot round-trip;
                ///   - a tuple literal, which formats as `(1, 2)`: a bare bracket re-parses as a
                ///     multi-column order-by list and is rebuilt as a `tuple(...)` function, so the
                ///     string round-trips but the AST does not.
                /// An alias already forces its own enclosing `(...)` (`need_parens`), which supplies
                /// the compensating bracket, so the AST-kind re-wraps are skipped in that case. Forms
                /// whose leading `(` both encloses the whole expression and re-parses to the same AST
                /// (bare tuple `(a, b)`, `(expr AS alias)`) are left as is.
                WriteBufferFromOwnString expr_buf;
                expr->format(expr_buf, s, state, nested_frame);
                const auto expr_str = expr_buf.stringView();
                const auto * literal = expr->as<ASTLiteral>();
                const bool ast_kind_needs_wrap = !nested_frame.need_parens
                    && (expr->as<ASTSubquery>() || (literal && literal->value.getType() == Field::Types::Tuple));
                if (ast_kind_needs_wrap || leadingParenClosesEarly(expr_str))
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
