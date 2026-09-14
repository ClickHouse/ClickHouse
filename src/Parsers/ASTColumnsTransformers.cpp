#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTColumnsTransformerList::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & child : children)
    {
        ostr << ' ';
        child->format(ostr, settings, state, frame);
    }
}

void ASTColumnsApplyTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "APPLY" << " ";

    if (!column_name_prefix.empty())
        ostr << "(";

    if (lambda)
    {
        lambda->format(ostr, settings, state, frame);
    }
    else
    {
        ostr << func_name;

        if (parameters)
        {
            auto nested_frame = frame;
            nested_frame.expression_list_prepend_whitespace = false;
            ostr << "(";
            parameters->format(ostr, settings, state, nested_frame);
            ostr << ")";
        }
    }

    if (!column_name_prefix.empty())
        ostr << ", '" << column_name_prefix << "')";
}

void ASTColumnsApplyTransformer::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("APPLY ", ostr);
    if (!column_name_prefix.empty())
        writeChar('(', ostr);

    if (lambda)
        lambda->appendColumnName(ostr);
    else
    {
        writeString(func_name, ostr);

        if (parameters)
            parameters->appendColumnName(ostr);
    }

    if (!column_name_prefix.empty())
    {
        writeCString(", '", ostr);
        writeString(column_name_prefix, ostr);
        writeCString("')", ostr);
    }
}

void ASTColumnsApplyTransformer::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(func_name.size());
    hash_state.update(func_name);
    /// `parameters` and `lambda` are not children, so `updateTreeHash` is needed to reach their own
    /// subtrees; `updateTreeHashImpl` alone stops at the node itself, and the arguments of
    /// `APPLY quantile(0.5)` live below it.
    if (parameters)
        parameters->updateTreeHash(hash_state, ignore_aliases);

    if (lambda)
        lambda->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(lambda_arg.size());
    hash_state.update(lambda_arg);

    hash_state.update(column_name_prefix.size());
    hash_state.update(column_name_prefix);

    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsExceptTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "EXCEPT" << (is_strict ? " STRICT " : " ");

    if (children.size() > 1)
        ostr << "(";

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            ostr << ", ";
        }
        (*it)->format(ostr, settings, state, frame);
    }

    if (pattern)
        ostr << quoteString(*pattern);

    if (children.size() > 1)
        ostr << ")";
}

void ASTColumnsExceptTransformer::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("EXCEPT ", ostr);
    if (is_strict)
        writeCString("STRICT ", ostr);

    if (children.size() > 1)
        writeChar('(', ostr);

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            writeCString(", ", ostr);
        (*it)->appendColumnName(ostr);
    }

    if (pattern)
        writeQuotedString(*pattern, ostr);

    if (children.size() > 1)
        writeChar(')', ostr);
}

void ASTColumnsExceptTransformer::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(is_strict);
    if (pattern)
    {
        hash_state.update(pattern->size());
        hash_state.update(*pattern);
    }

    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsExceptTransformer::setPattern(String pattern_)
{
    pattern = std::move(pattern_);
}

void ASTColumnsReplaceTransformer::Replacement::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    chassert(children.size() == 1);

    children[0]->format(ostr, settings, state, frame);
    ostr << " AS " << backQuoteIfNeed(name);
}

void ASTColumnsReplaceTransformer::Replacement::appendColumnName(WriteBuffer & ostr) const
{
    chassert(children.size() == 1);

    children[0]->appendColumnName(ostr);
    writeCString(" AS ", ostr);
    writeProbablyBackQuotedString(name, ostr);
}

void ASTColumnsReplaceTransformer::Replacement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    chassert(children.size() == 1);

    hash_state.update(name.size());
    hash_state.update(name);
    children[0]->updateTreeHashImpl(hash_state, ignore_aliases);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsReplaceTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "REPLACE" << (is_strict ? " STRICT " : " ");

    ostr << "(";
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            ostr << ", ";

        (*it)->format(ostr, settings, state, frame);
    }
    ostr << ")";
}

void ASTColumnsReplaceTransformer::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("REPLACE ", ostr);
    if (is_strict)
        writeCString("STRICT ", ostr);

    if (children.size() > 1)
        writeChar('(', ostr);

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            writeCString(", ", ostr);
        (*it)->appendColumnName(ostr);
    }

    if (children.size() > 1)
        writeChar(')', ostr);
}

void ASTColumnsReplaceTransformer::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(is_strict);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsTransformerList::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ColumnsTransformerList");
    w.writeChildren(children);
}

void ASTColumnsApplyTransformer::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ColumnsApplyTransformer");
    if (!func_name.empty())
        w.writeString("func_name", func_name);
    w.writeChild("parameters", parameters);
    w.writeChild("lambda", lambda);
    if (!lambda_arg.empty())
        w.writeString("lambda_arg", lambda_arg);
    if (!column_name_prefix.empty())
        w.writeString("column_name_prefix", column_name_prefix);
}

void ASTColumnsExceptTransformer::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ColumnsExceptTransformer");
    if (is_strict)
        w.writeBool("is_strict", true);
    if (pattern)
        w.writeString("pattern", *pattern);
    w.writeChildren(children);
}

void ASTColumnsReplaceTransformer::Replacement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ColumnsReplaceTransformerReplacement");
    w.writeString("name", name);
    w.writeChildren(children);
}

void ASTColumnsReplaceTransformer::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ColumnsReplaceTransformer");
    if (is_strict)
        w.writeBool("is_strict", true);
    w.writeChildren(children);
}

void ASTColumnsTransformerList::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    children = r.readChildren();

    /// `applyColumnsTransformer` only dispatches `ColumnsApplyTransformer`,
    /// `ColumnsExceptTransformer`, and `ColumnsReplaceTransformer`, silently ignoring any
    /// other child type. Reject foreign children from malformed `clickhouse_json` here so
    /// they cannot be formatted in the AST while being skipped during semantic transformation.
    for (const auto & child : children)
        if (!child
            || !(child->as<ASTColumnsApplyTransformer>()
                 || child->as<ASTColumnsExceptTransformer>()
                 || child->as<ASTColumnsReplaceTransformer>()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unexpected child node type in ColumnsTransformerList during AST JSON deserialization");
}

void ASTColumnsApplyTransformer::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    func_name = r.getString("func_name");
    /// `parameters` is parser-produced as an `ASTExpressionList` (it is assigned to
    /// `ASTFunction::parameters`, which `QueryTreeBuilder` downcasts to `ASTExpressionList`),
    /// and `lambda` is an `ASTFunction` (the lambda execution path does
    /// `lambda->as<const ASTFunction &>()`). Restoring them through the generic child path
    /// would let a wrong node type reach those downcasts as an internal cast error instead of
    /// a user-facing `BAD_ARGUMENTS`.
    parameters = r.readChildOfType<ASTExpressionList>("parameters");
    lambda = r.readChildOfType<ASTFunction>("lambda");
    lambda_arg = r.getString("lambda_arg");
    column_name_prefix = r.getString("column_name_prefix");

    /// `formatImpl`, `appendColumnName`, `updateTreeHashImpl`, and `applyColumnsApplyTransformer`
    /// all branch on either a lambda or a function name; exactly one of the two
    /// mutually exclusive shapes must be present. The lambda branch takes priority
    /// in `formatImpl`/`applyColumnsApplyTransformer` and silently drops `func_name`/`parameters`,
    /// so accepting both at once would lose information the parser never produces.
    bool has_function_mode = !func_name.empty();
    bool has_lambda_mode = lambda != nullptr;

    if (has_function_mode && has_lambda_mode)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ColumnsApplyTransformer must specify either a function name or a lambda, not both, during AST JSON deserialization");

    if (!has_function_mode && !has_lambda_mode)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ColumnsApplyTransformer requires either a function name or a lambda during AST JSON deserialization");

    /// `parameters` is only meaningful for function mode (`func_name(parameters)`),
    /// so it must not appear without a function name.
    if (parameters && !has_function_mode)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ColumnsApplyTransformer with parameters requires a function name during AST JSON deserialization");

    /// `applyColumnsApplyTransformer` substitutes the current column for `lambda_arg` inside the lambda body,
    /// so a lambda without its argument name would be meaningless, and the argument name
    /// must not appear without a lambda.
    if (has_lambda_mode && lambda_arg.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ColumnsApplyTransformer with a lambda requires a non-empty lambda argument during AST JSON deserialization");

    if (!lambda_arg.empty() && !has_lambda_mode)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ColumnsApplyTransformer with a lambda argument requires a lambda during AST JSON deserialization");

    /// `applyColumnsApplyTransformer` reads the lambda body as `lambda->as<const ASTFunction &>().arguments->children.at(1)`,
    /// so the lambda must carry the parser-produced shape: a non-null `arguments` list with at least the
    /// argument tuple and the body. Reject a function-shaped lambda missing them, which would otherwise
    /// fail later as an internal null-deref / out-of-range access instead of `BAD_ARGUMENTS`.
    if (has_lambda_mode)
    {
        const auto & lambda_function = lambda->as<const ASTFunction &>();
        if (!lambda_function.arguments || lambda_function.arguments->children.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ColumnsApplyTransformer lambda must have an argument tuple and a body during AST JSON deserialization");
    }
}

void ASTColumnsExceptTransformer::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    is_strict = r.getBool("is_strict");
    /// Distinguish an empty regexp pattern (`EXCEPT('')`) from the column-list form by key presence,
    /// not by empty string: `writeJSON` emits "pattern" exactly when the regexp form was used, so
    /// `EXCEPT('')` must round-trip rather than be misread as "no pattern" (and then rejected).
    if (r.has("pattern"))
        setPattern(r.getString("pattern"));
    /// In the column-list form, `QueryTreeBuilder::buildColumnTransformers` downcasts each child to
    /// `ASTIdentifier`, so reject any other child type here.
    children = r.readChildrenOfType<ASTIdentifier>("ColumnsExceptTransformer");

    /// `formatImpl`, `appendColumnName`, and `applyColumnsExceptTransformer` rely on exactly one of the two
    /// mutually exclusive shapes: a regexp `pattern` or an explicit list of column children.
    if (pattern && !children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ColumnsExceptTransformer must have either a pattern or an explicit list of columns, not both, during AST JSON deserialization");

    if (!pattern && children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ColumnsExceptTransformer requires either a non-empty pattern or an explicit list of columns during AST JSON deserialization");
}

void ASTColumnsReplaceTransformer::Replacement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ASTColumnsReplaceTransformer::Replacement JSON requires a non-empty 'name'");
    children = r.readChildren();

    /// `formatImpl`, `appendColumnName`, `updateTreeHashImpl`, and `applyColumnsReplaceTransformer`
    /// all access `children[0]`, so the invariant must hold after JSON deserialization.
    if (children.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ASTColumnsReplaceTransformer::Replacement JSON must have exactly one child (the expression), got {}",
            children.size());
}

void ASTColumnsReplaceTransformer::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    is_strict = r.getBool("is_strict");
    /// `applyColumnsReplaceTransformer` downcasts every child with `replace_child->as<Replacement &>()` and then
    /// reads `replacement.children[0]`, so a foreign child type from malformed `clickhouse_json`
    /// must be rejected here instead of reaching that downcast during execution.
    children = r.readChildrenOfType<ASTColumnsReplaceTransformer::Replacement>("ColumnsReplaceTransformer");
}

}
