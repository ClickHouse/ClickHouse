#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

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
    w.writeChild("collation", getCollation());
    w.writeChild("fill_from", getFillFrom());
    w.writeChild("fill_to", getFillTo());
    w.writeChild("fill_step", getFillStep());
    w.writeChild("fill_staleness", getFillStaleness());
    w.writeChildren(children);
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
    direction = static_cast<int>(r.getInt("direction"));
    nulls_direction = static_cast<int>(r.getInt("nulls_direction"));
    nulls_direction_was_explicitly_specified = r.getBool("nulls_direction_was_explicitly_specified");
    with_fill = r.getBool("with_fill");

    children = r.readChildren();

    auto child = r.readChild("collation");
    if (child)
        setCollation(child);

    child = r.readChild("fill_from");
    if (child)
        setFillFrom(child);

    child = r.readChild("fill_to");
    if (child)
        setFillTo(child);

    child = r.readChild("fill_step");
    if (child)
        setFillStep(child);

    child = r.readChild("fill_staleness");
    if (child)
        setFillStaleness(child);
}

void ASTStorageOrderByElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    direction = static_cast<int>(r.getInt("direction"));
    children = r.readChildren();
}

}
