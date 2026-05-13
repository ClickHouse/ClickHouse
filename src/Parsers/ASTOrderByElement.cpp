#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    int validateOrderByDirection(Int64 value, std::string_view field_name)
    {
        if (value != -1 && value != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Invalid '{}' value in JSON AST for OrderByElement: {} (must be -1 or 1)", field_name, value);
        return static_cast<int>(value);
    }
}

void ASTOrderByElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(direction);
    hash_state.update(nulls_direction);
    hash_state.update(nulls_direction_was_explicitly_specified);
    hash_state.update(with_fill);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTOrderByElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->format(ostr, settings, state, frame);
    ostr
        << (direction == -1 ? " DESC" : " ASC")
       ;

    if (nulls_direction_was_explicitly_specified)
    {
        ostr
            << " NULLS "
            << (nulls_direction == direction ? "LAST" : "FIRST")
           ;
    }

    if (auto collation = getCollation())
    {
        ostr << " COLLATE ";
        collation->format(ostr, settings, state, frame);
    }

    if (with_fill)
    {
        ostr << " WITH FILL";
        if (auto fill_from = getFillFrom())
        {
            ostr << " FROM ";
            fill_from->format(ostr, settings, state, frame);
        }
        if (auto fill_to = getFillTo())
        {
            ostr << " TO ";
            fill_to->format(ostr, settings, state, frame);
        }
        if (auto fill_step = getFillStep())
        {
            ostr << " STEP ";
            fill_step->format(ostr, settings, state, frame);
        }
        if (auto fill_staleness = getFillStaleness())
        {
            ostr << " STALENESS ";
            fill_staleness->format(ostr, settings, state, frame);
        }
    }
}

void ASTStorageOrderByElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(direction);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTStorageOrderByElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->format(ostr, settings, state, frame);

    if (direction == -1)
        ostr << " DESC";
}

void ASTOrderByElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "OrderByElement");
    w.writeInt("direction", direction);
    w.writeInt("nulls_direction", nulls_direction);
    if (nulls_direction_was_explicitly_specified)
        w.writeBool("nulls_direction_was_explicitly_specified", true);
    if (with_fill)
        w.writeBool("with_fill", true);
    /// Each child is serialized exactly once via its named key.
    /// `children[0]` is the expression; optional nodes (collation, fill_*) are
    /// also stored in `children` with positions tracked by the `positions` map.
    /// We don't emit the generic `children` array to avoid duplicating optional
    /// nodes on JSON round-trip.
    if (!children.empty())
        w.writeChild("expression", children.front());
    w.writeChild("collation", getCollation());
    w.writeChild("fill_from", getFillFrom());
    w.writeChild("fill_to", getFillTo());
    w.writeChild("fill_step", getFillStep());
    w.writeChild("fill_staleness", getFillStaleness());
}

void ASTStorageOrderByElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "StorageOrderByElement");
    w.writeInt("direction", direction);
    w.writeChildren(children);
}

void ASTOrderByElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    direction = validateOrderByDirection(r.getInt("direction"), "direction");
    nulls_direction = validateOrderByDirection(r.getInt("nulls_direction"), "nulls_direction");
    nulls_direction_was_explicitly_specified = r.getBool("nulls_direction_was_explicitly_specified");
    with_fill = r.getBool("with_fill");

    auto expression = r.readChild("expression");
    if (!expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required field 'expression' is missing in JSON AST for OrderByElement");
    children.push_back(std::move(expression));

    if (auto child = r.readChild("collation"))
        setCollation(child);

    if (auto child = r.readChild("fill_from"))
        setFillFrom(child);

    if (auto child = r.readChild("fill_to"))
        setFillTo(child);

    if (auto child = r.readChild("fill_step"))
        setFillStep(child);

    if (auto child = r.readChild("fill_staleness"))
        setFillStaleness(child);
}

void ASTStorageOrderByElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    direction = validateOrderByDirection(r.getInt("direction"), "direction");
    children = r.readChildren();

    /// `formatImpl` unconditionally dereferences `children.front()`, so the invariant must
    /// hold after JSON deserialization.
    if (children.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ASTStorageOrderByElement JSON must have exactly one child (the expression), got {}",
            children.size());
}

}
