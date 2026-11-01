#include <Common/StringUtils.h>
#include <IO/Operators.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/CommonParsers.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTObjectTypedPathArgument::clone() const
{
    auto res = make_intrusive<ASTObjectTypedPathArgument>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    return res;
}

void ASTObjectTypedPathArgument::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// We must quote path "SKIP" to avoid its confusion with SKIP keyword in Object arguments.
    if (equalsCaseInsensitive(path, "SKIP"))
        ostr << backQuote(path) << ' ';
    else
        ostr << backQuoteIfNeed(path) << ' ';

    type->format(ostr, settings, state, frame);
}

ASTPtr ASTObjectTypeArgument::clone() const
{
    auto res = make_intrusive<ASTObjectTypeArgument>(*this);
    res->children.clear();

    if (path_with_type)
    {
        res->path_with_type = path_with_type->clone();
        res->children.push_back(res->path_with_type);
    }
    else if (skip_path)
    {
        res->skip_path = skip_path->clone();
        res->children.push_back(res->skip_path);
    }
    else if (skip_path_regexp)
    {
        res->skip_path_regexp = skip_path_regexp->clone();
        res->children.push_back(res->skip_path_regexp);
    }
    else if (parameter)
    {
        res->parameter = parameter->clone();
        res->children.push_back(res->parameter);
    }

    return res;
}

void ASTObjectTypeArgument::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (path_with_type)
    {
        path_with_type->format(ostr, settings, state, frame);
    }
    else if (parameter)
    {
        parameter->format(ostr, settings, state, frame);
    }
    else if (skip_path)
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << indent_str << "SKIP" << ' ';
        skip_path->format(ostr, settings, state, frame);
    }
    else if (skip_path_regexp)
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << indent_str << "SKIP REGEXP" << ' ';
        skip_path_regexp->format(ostr, settings, state, frame);
    }
}

void ASTObjectTypedPathArgument::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ObjectTypedPathArgument");
    w.writeString("path", path);
    w.writeChild("data_type", type);
}

void ASTObjectTypedPathArgument::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    path = r.getString("path");
    if (path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`ObjectTypedPathArgument` must have a non-empty 'path' during AST JSON deserialization");
    /// `ObjectArgumentParser` produces this slot only through `ParserDataType`, whose top-level node
    /// is always a data type (`ASTDataType` / `ASTEnumDataType` / `ASTTupleDataType`). Use
    /// `dynamic_cast` so the hierarchy's specialized subclasses are accepted — `IAST::as` matches
    /// exact `typeid` and would reject `ASTTupleDataType` / `ASTEnumDataType` here, which breaks
    /// the round-trip of `JSON(a Tuple(...))` / `JSON(a Enum8(...))`.
    auto child = r.readChild("data_type");
    if (!child || !dynamic_cast<const ASTDataType *>(child.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`ObjectTypedPathArgument` must have a data type as 'data_type' during AST JSON deserialization");
    type = child;
    children.push_back(type);
}

void ASTObjectTypeArgument::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ObjectTypeArgument");
    w.writeChild("path_with_type", path_with_type);
    w.writeChild("skip_path", skip_path);
    w.writeChild("skip_path_regexp", skip_path_regexp);
    w.writeChild("parameter", parameter);
}

void ASTObjectTypeArgument::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    size_t count = 0;
    if (auto child = r.readChildOfType<ASTObjectTypedPathArgument>("path_with_type"))
    {
        path_with_type = child;
        children.push_back(path_with_type);
        ++count;
    }
    if (auto child = r.readChildOfType<ASTIdentifier>("skip_path"))
    {
        skip_path = child;
        children.push_back(skip_path);
        ++count;
    }
    /// `ObjectArgumentParser` parses `SKIP REGEXP '<regexp>'` with `ParserStringLiteral`, so the
    /// literal is always a `String` field. Reject non-string literals so `formatImpl` cannot emit
    /// `SKIP REGEXP 42` or similar parser-impossible shapes.
    if (auto child = r.readStringLiteralChild("skip_path_regexp"))
    {
        skip_path_regexp = child;
        children.push_back(skip_path_regexp);
        ++count;
    }
    /// `ObjectArgumentParser` builds `parameter` with `makeASTOperator("equals", identifier, number)`,
    /// i.e. an `equals` operator-function whose arguments are exactly an identifier followed by a
    /// numeric literal. Validate that shape so a payload that stashes an arbitrary function here
    /// cannot format as `JSON(<garbage>)` — the generic `ASTFunction` check accepts anything.
    if (auto child = r.readChildOfType<ASTFunction>("parameter"))
    {
        const auto * function = child->as<ASTFunction>();
        const bool shape_ok = function
            && function->name == "equals"
            && function->isOperator()
            && function->arguments
            && function->arguments->as<ASTExpressionList>()
            && function->arguments->children.size() == 2
            && function->arguments->children[0]
            && function->arguments->children[0]->as<ASTIdentifier>()
            && function->arguments->children[1]
            && [&]()
            {
                const auto * literal = function->arguments->children[1]->as<ASTLiteral>();
                if (!literal)
                    return false;
                const auto type = literal->value.getType();
                return type == Field::Types::UInt64
                    || type == Field::Types::Int64
                    || type == Field::Types::Float64;
            }();
        if (!shape_ok)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`ObjectTypeArgument` 'parameter' must be an `equals(identifier, number)` operator during AST JSON deserialization");
        parameter = child;
        children.push_back(parameter);
        ++count;
    }
    if (count != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`ObjectTypeArgument` must have exactly one of 'path_with_type', 'skip_path', 'skip_path_regexp' or 'parameter' during AST JSON deserialization");
}

}
