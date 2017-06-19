#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{


/** RENAME query
  */
class ASTRenameQuery : public IAST, public ASTQueryWithOnCluster
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

    /** Get the text that identifies this element. */
    String getID() const override { return "Rename"; };

    ASTPtr clone() const override { return std::make_shared<ASTRenameQuery>(*this); }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database = {}) const override
    {
        auto query_ptr = clone();
        ASTRenameQuery & query = static_cast<ASTRenameQuery &>(*query_ptr);

        query.cluster.clear();
        for (Element & elem : query.elements)
        {
            if (elem.from.database.empty())
                elem.from.database = new_database;
            if (elem.to.database.empty())
                elem.to.database = new_database;
        }

        return query_ptr;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "RENAME TABLE " << (settings.hilite ? hilite_none : "");

        for (auto it = elements.cbegin(); it != elements.cend(); ++it)
        {
            if (it != elements.cbegin())
                settings.ostr << ", ";

            settings.ostr << (!it->from.database.empty() ? backQuoteIfNeed(it->from.database) + "." : "") << backQuoteIfNeed(it->from.table)
                << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
                << (!it->to.database.empty() ? backQuoteIfNeed(it->to.database) + "." : "") << backQuoteIfNeed(it->to.table);
        }

        formatOnCluster(settings);
    }
};

}
