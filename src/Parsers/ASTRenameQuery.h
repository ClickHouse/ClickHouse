#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier_fwd.h>
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
        ASTPtr database;
        ASTPtr table;

        String getDatabase() const
        {
            String name;
            tryGetIdentifierNameInto(database, name);
            return name;
        }

        String getTable() const
        {
            String name;
            tryGetIdentifierNameInto(table, name);
            return name;
        }
    };

    struct Element
    {
        Table from;
        Table to;
        bool if_exists{false};   /// If this directive is used, one will not get an error if the table/database/dictionary to be renamed/exchanged doesn't exist.
    };

    using Elements = std::vector<Element>;

    bool exchange{false};   /// For EXCHANGE TABLES
    bool database{false};   /// For RENAME DATABASE
    bool dictionary{false};   /// For RENAME DICTIONARY

    /// Special flag for CREATE OR REPLACE. Do not throw if the second table does not exist.
    bool rename_if_cannot_exchange{false};

    explicit ASTRenameQuery(Elements elements_ = {})
        : elements(std::move(elements_))
    {
        for (const auto & elem : elements)
        {
            if (elem.from.database)
                children.push_back(elem.from.database);
            if (elem.from.table)
                children.push_back(elem.from.table);
            if (elem.to.database)
                children.push_back(elem.to.database);
            if (elem.to.table)
                children.push_back(elem.to.table);
        }
    }

    void setDatabaseIfNotExists(const String & database_name)
    {
        for (auto & elem : elements)
        {
            if (!elem.from.database)
            {
                elem.from.database = std::make_shared<ASTIdentifier>(database_name);
                children.push_back(elem.from.database);
            }
            if (!elem.to.database)
            {
                elem.to.database = std::make_shared<ASTIdentifier>(database_name);
                children.push_back(elem.to.database);
            }
        }
    }

    const Elements & getElements() const { return elements; }

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Rename"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTRenameQuery>(*this);
        res->children.clear();

        auto clone_child = [&res](ASTPtr & node)
        {
            if (node)
            {
                node = node->clone();
                res->children.push_back(node);
            }
        };

        for (auto & elem : res->elements)
        {
            clone_child(elem.from.database);
            clone_child(elem.from.table);
            clone_child(elem.to.database);
            clone_child(elem.to.table);
        }
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
            if (!elem.from.database)
            {
                elem.from.database = std::make_shared<ASTIdentifier>(params.default_database);
                query.children.push_back(elem.from.database);
            }
            if (!elem.to.database)
            {
                elem.to.database = std::make_shared<ASTIdentifier>(params.default_database);
                query.children.push_back(elem.to.database);
            }
        }

        return query_ptr;
    }

    QueryKind getQueryKind() const override { return QueryKind::Rename; }

    void addElement(const String & from_db, const String & from_table, const String & to_db, const String & to_table)
    {
        auto identifier = [&](const String & name) -> ASTPtr
        {
            if (name.empty())
                return nullptr;
            ASTPtr ast = std::make_shared<ASTIdentifier>(name);
            children.push_back(ast);
            return ast;
        };
        elements.push_back(Element {.from = Table {.database = identifier(from_db), .table = identifier(from_table)}, .to = Table {.database = identifier(to_db), .table = identifier(to_table)}});
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        if (database)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "RENAME DATABASE " << (settings.hilite ? hilite_none : "");

            if (elements.at(0).if_exists)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << "IF EXISTS " << (settings.hilite ? hilite_none : "");

            elements.at(0).from.database->formatImpl(settings, state, frame);
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "");
            elements.at(0).to.database->formatImpl(settings, state, frame);
            formatOnCluster(settings);
            return;
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "");
        if (exchange && dictionary)
            settings.ostr << "EXCHANGE DICTIONARIES ";
        else if (exchange)
            settings.ostr << "EXCHANGE TABLES ";
        else if (dictionary)
            settings.ostr << "RENAME DICTIONARY ";
        else
            settings.ostr << "RENAME TABLE ";

        settings.ostr << (settings.hilite ? hilite_none : "");

        for (auto it = elements.cbegin(); it != elements.cend(); ++it)
        {
            if (it != elements.cbegin())
                settings.ostr << ", ";

            if (it->if_exists)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << "IF EXISTS " << (settings.hilite ? hilite_none : "");


            if (it->from.database)
            {
                it->from.database->formatImpl(settings, state, frame);
                settings.ostr << '.';
            }

            chassert(it->from.table);
            it->from.table->formatImpl(settings, state, frame);

            settings.ostr << (settings.hilite ? hilite_keyword : "") << (exchange ? " AND " : " TO ") << (settings.hilite ? hilite_none : "");

            if (it->to.database)
            {
                it->to.database->formatImpl(settings, state, frame);
                settings.ostr << '.';
            }

            chassert(it->to.table);
            it->to.table->formatImpl(settings, state, frame);

        }

        formatOnCluster(settings);
    }

    Elements elements;
};

}
