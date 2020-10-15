#pragma once

#include <Common/FieldVisitors.h>
#include <Common/SettingsChanges.h>
#include <Parsers/IAST.h>


namespace DB
{


/** SET query
  */
class ASTSetQuery : public IAST
{
public:
    bool is_standalone = true; /// If false, this AST is a part of another query, such as SELECT.

    SettingsChanges changes;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Set"; }

    ASTPtr clone() const override { return std::make_shared<ASTSetQuery>(*this); }

    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        if (is_standalone)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "SET " << (settings.hilite ? hilite_none : "");

        for (auto it = changes.begin(); it != changes.end(); ++it)
        {
            if (it != changes.begin())
                settings.ostr << ", ";

            settings.ostr << it->name << " = " << applyVisitor(FieldVisitorToString(), it->value);
        }
    }
};

}
