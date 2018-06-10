#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** A pair of the name and type. For example, browser FixedString(2).
  */
class ASTNameTypePair : public IAST
{
public:
    /// name
    String name;
    /// type
    ASTPtr type;

    /** Get the text that identifies this element. */
    String getID() const override { return "NameTypePair_" + name; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTNameTypePair>(*this);
        res->children.clear();

        if (type)
        {
            res->type = type;
            res->children.push_back(res->type);
        }

        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

        settings.ostr << settings.nl_or_ws << indent_str << backQuoteIfNeed(name) << " ";
        type->formatImpl(settings, state, frame);
    }
};


}

