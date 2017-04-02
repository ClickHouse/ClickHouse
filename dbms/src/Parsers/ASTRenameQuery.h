#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/** RENAME запрос
  */
class ASTRenameQuery : public IAST
{
public:
    struct Table
    {
        String database;
        String table;
    };

    struct Element
    {
        Table from;
        Table to;
    };

    using Elements = std::vector<Element>;
    Elements elements;

    ASTRenameQuery() = default;
    ASTRenameQuery(const StringRange range_) : IAST(range_) {}

    /** Получить текст, который идентифицирует этот элемент. */
    String getID() const override { return "Rename"; };

    ASTPtr clone() const override { return std::make_shared<ASTRenameQuery>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "RENAME TABLE " << (settings.hilite ? hilite_none : "");

        for (ASTRenameQuery::Elements::const_iterator it = elements.begin(); it != elements.end(); ++it)
        {
            if (it != elements.begin())
                settings.ostr << ", ";

            settings.ostr << (!it->from.database.empty() ? backQuoteIfNeed(it->from.database) + "." : "") << backQuoteIfNeed(it->from.table)
                << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
                << (!it->to.database.empty() ? backQuoteIfNeed(it->to.database) + "." : "") << backQuoteIfNeed(it->to.table);
        }
    }
};

}
