#pragma once

#include <Parsers/IAST.h>
#include <Storages/DataDestinationType.h>


namespace DB
{
/** Element of TTL expression.
  */
class ASTTTLElement : public IAST
{
public:
    DataDestinationType destination_type;
    String destination_name;

    ASTTTLElement(DataDestinationType destination_type_, const String & destination_name_)
        : destination_type(destination_type_)
        , destination_name(destination_name_)
    {
    }

    String getID(char) const override { return "TTLElement"; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTTTLElement>(*this);
        clone->cloneChildren();
        return clone;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
