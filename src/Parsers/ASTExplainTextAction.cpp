#include <Parsers/ASTExplainTextAction.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTWithAlias.h>

#include <Common/Exception.h>
#include <IO/Operators.h>
#include <Poco/JSON/Object.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTExplainTextAction::ASTExplainTextAction(Kind kind_)
    : kind(kind_)
{
}

String ASTExplainTextAction::toString(Kind kind_)
{
    switch (kind_)
    {
        case Kind::Oneline:
            return "ONELINE";
        case Kind::Multiline:
            return "MULTILINE";
        case Kind::ModifyLimit:
            return "MODIFY LIMIT";
        case Kind::ModifyOffset:
            return "MODIFY OFFSET";
        case Kind::Page:
            return "PAGE";
        case Kind::ModifyFormat:
            return "MODIFY FORMAT";
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown EXPLAIN TEXT action kind {}", static_cast<UInt64>(kind_));
}

ASTExplainTextAction::Kind ASTExplainTextAction::fromString(const String & value)
{
    if (value == "ONELINE")
        return Kind::Oneline;
    if (value == "MULTILINE")
        return Kind::Multiline;
    if (value == "MODIFY LIMIT")
        return Kind::ModifyLimit;
    if (value == "MODIFY OFFSET")
        return Kind::ModifyOffset;
    if (value == "PAGE")
        return Kind::Page;
    if (value == "MODIFY FORMAT")
        return Kind::ModifyFormat;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown EXPLAIN TEXT action '{}'", value);
}

String ASTExplainTextAction::getID(char delim) const
{
    return "ExplainTextAction" + (delim + toString(kind));
}

bool ASTExplainTextAction::hasOperand() const
{
    return !children.empty();
}

ASTPtr ASTExplainTextAction::getOperand() const
{
    return children.empty() ? ASTPtr{} : children.front();
}

void ASTExplainTextAction::setOperand(ASTPtr operand)
{
    children.clear();

    if (!operand)
        return;

    /// Output formats are parser-owned special identifiers, just like `ASTQueryWithOutput::format_ast`.
    if (kind == Kind::ModifyFormat)
        setIdentifierSpecial(operand);

    children.push_back(std::move(operand));
}

ASTPtr ASTExplainTextAction::clone() const
{
    auto res = make_intrusive<ASTExplainTextAction>(*this);
    res->cloneChildren();
    return res;
}

void ASTExplainTextAction::validateShape() const
{
    switch (kind)
    {
        case Kind::Oneline:
        case Kind::Multiline:
        {
            if (!children.empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "{} cannot have an operand",
                    toString(kind));
            return;
        }

        case Kind::ModifyLimit:
        case Kind::ModifyOffset:
        {
            if (children.size() != 1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "{} requires exactly one expression operand, got {}",
                    toString(kind), children.size());

            const auto * expression = dynamic_cast<const ASTWithAlias *>(children.front().get());
            if (!expression || !expression->tryGetAlias().empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "{} requires one expression operand without an alias",
                    toString(kind));
            return;
        }

        case Kind::Page:
        {
            if (children.size() != 1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "PAGE requires exactly one operand, got {}",
                    children.size());

            const auto * literal = children.front()->as<ASTLiteral>();
            if (!literal
                || literal->value.getType() != Field::Types::UInt64
                || literal->value.safeGet<UInt64>() == 0
                || !literal->tryGetAlias().empty())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "PAGE requires a positive UInt64 literal");
            }

            return;
        }

        case Kind::ModifyFormat:
        {
            if (children.size() != 1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "MODIFY FORMAT requires exactly one operand, got {}",
                    children.size());

            const auto * identifier = children.front()->as<ASTIdentifier>();
            if (!identifier
                || !identifier->isShort()
                || identifier->isParam()
                || !identifier->tryGetAlias().empty())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "MODIFY FORMAT requires one non-parameterized identifier");
            }
            return;
        }
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown EXPLAIN TEXT action kind {}",
        static_cast<UInt64>(kind));
}

void ASTExplainTextAction::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << toString(kind);

    if (!children.empty())
    {
        ostr << ' ';
        children.front()->format(ostr, settings, state, frame);
    }
}

void ASTExplainTextAction::writeJSON(WriteBuffer & out) const
{
    validateShape();

    JSONObjectWriter writer(out, "ExplainTextAction");
    writer.writeString("kind", toString(kind));

    if (!children.empty())
        writer.writeChild("operand", children.front());
}

void ASTExplainTextAction::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader reader(json);

    kind = fromString(reader.getString("kind"));
    setOperand(reader.readChild("operand"));
    validateShape();
}

}
