#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

/** RENAME query
  */
class ASTRenameQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
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
        bool if_exists{false};   /// If this directive is used, one will not get an error if the table/database/dictionary to be renamed/exchanged doesn't exist.
    };

    using Elements = std::vector<Element>;
    Elements elements;

    bool exchange{false};   /// For EXCHANGE TABLES
    bool database{false};   /// For RENAME DATABASE
    bool dictionary{false};   /// For RENAME DICTIONARY

    /// Special flag for CREATE OR REPLACE. Do not throw if the second table does not exist.
    bool rename_if_cannot_exchange{false};

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Rename"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTRenameQuery>(*this);
        cloneOutputOptions(*res);
        return res;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        auto query_ptr = clone();
        auto & query = query_ptr->as<ASTRenameQuery &>();

        query.cluster.clear();
        for (Element & elem : query.elements)
        {
            if (elem.from.database.empty())
                elem.from.database = params.default_database;
            if (elem.to.database.empty())
                elem.to.database = params.default_database;
        }

        return query_ptr;
    }

    QueryKind getQueryKind() const override { return QueryKind::Rename; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        if (database)
        {
            settings.writeKeyword("RENAME DATABASE ");

            if (elements.at(0).if_exists)
                settings.writeKeyword("IF EXISTS ");

            settings.ostr << backQuoteIfNeed(elements.at(0).from.database);
            settings.writeKeyword(" TO ");
            settings.ostr << backQuoteIfNeed(elements.at(0).to.database);
            formatOnCluster(settings);
            return;
        }

        if (exchange && dictionary)
            settings.writeKeyword("EXCHANGE DICTIONARIES ");
        else if (exchange)
            settings.writeKeyword("EXCHANGE TABLES ");
        else if (dictionary)
            settings.writeKeyword("RENAME DICTIONARY ");
        else
            settings.writeKeyword("RENAME TABLE ");

        for (auto it = elements.cbegin(); it != elements.cend(); ++it)
        {
            if (it != elements.cbegin())
                settings.ostr << ", ";

            if (it->if_exists)
                settings.writeKeyword("IF EXISTS ");
            settings.ostr << (!it->from.database.empty() ? backQuoteIfNeed(it->from.database) + "." : "") << backQuoteIfNeed(it->from.table);
            settings.writeKeyword(exchange ? " AND " : " TO ");
            settings.ostr << (!it->to.database.empty() ? backQuoteIfNeed(it->to.database) + "." : "") << backQuoteIfNeed(it->to.table);
        }

        formatOnCluster(settings);
    }
};

}
