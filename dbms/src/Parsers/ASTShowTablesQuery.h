#pragma once

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query SHOW TABLES or SHOW DATABASES
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
    bool databases{false};
    bool temporary{false};
    String from;
    String like;
    bool not_like{false};

    /** Get the text that identifies this element. */
    String getID() const override { return "ShowTables"; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTShowTablesQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        if (databases)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES" << (settings.hilite ? hilite_none : "");
        }
        else
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW TABLES" << (settings.hilite ? hilite_none : "");

            if (!from.empty())
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
                    << backQuoteIfNeed(from);

            if (!like.empty())
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIKE " << (settings.hilite ? hilite_none : "")
                    << std::quoted(like, '\'');
        }
    }
};

}
