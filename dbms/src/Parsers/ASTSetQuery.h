#pragma once

#include <Core/Field.h>
#include <Core/FieldVisitors.h>
#include <Parsers/IAST.h>


namespace DB
{


/** SET запрос
  */
class ASTSetQuery : public IAST
{
public:
    struct Change
    {
        String name;
        Field value;
    };

    using Changes = std::vector<Change>;
    Changes changes;

    bool global;    /// Если запрос SET GLOBAL.

    ASTSetQuery() = default;
    ASTSetQuery(const StringRange range_) : IAST(range_) {}

    /** Получить текст, который идентифицирует этот элемент. */
    String getID() const override { return "Set"; };

    ASTPtr clone() const override { return std::make_shared<ASTSetQuery>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SET " << (global ? "GLOBAL " : "") << (settings.hilite ? hilite_none : "");

        for (ASTSetQuery::Changes::const_iterator it = changes.begin(); it != changes.end(); ++it)
        {
            if (it != changes.begin())
                settings.ostr << ", ";

            settings.ostr << it->name << " = " << applyVisitor(FieldVisitorToString(), it->value);
        }
    }
};

}
