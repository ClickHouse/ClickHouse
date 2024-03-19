#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

String ASTQueryWithTableAndOutput::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTQueryWithTableAndOutput::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTQueryWithTableAndOutput::setDatabase(const String & name)
{
    setOrReplace(database, std::make_shared<ASTIdentifier>(name));
}

void ASTQueryWithTableAndOutput::setTable(const String & name)
{
    setOrReplace(table, std::make_shared<ASTIdentifier>(name));
}

void ASTQueryWithTableAndOutput::cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const
{
    if (database)
        cloned.set(cloned.database, database->clone());
    if (table)
        cloned.set(cloned.table, table->clone());
}

void ASTQueryWithTableAndOutput::forEachPointerToChild(std::function<void(void**)> f)
{
    f(reinterpret_cast<void **>(&database));
    f(reinterpret_cast<void **>(&table));
}

template <typename AstIDAndQueryNames>
ASTPtr ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>::clone() const
{
    auto res = std::make_shared<ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

template <typename AstIDAndQueryNames>
void ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
        << (temporary ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query)
        << " " << (settings.hilite ? hilite_none : "");

    if (database)
    {
        database->formatImpl(settings, state, frame);
        settings.ostr << '.';
    }

    chassert(table != nullptr, "Table is empty for the ASTQueryWithTableAndOutputImpl.");
    table->formatImpl(settings, state, frame);
}

template <typename AstIDAndQueryNames>
void ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>::forEachPointerToChild(std::function<void(void**)> f)
{
    ASTQueryWithOutput::forEachPointerToChild(f);
    ASTQueryWithTableAndOutput::forEachPointerToChild(f);
}


template class ASTQueryWithTableAndOutputImpl<ASTExistsTableQueryIDAndQueryNames>;
template class ASTQueryWithTableAndOutputImpl<ASTExistsViewQueryIDAndQueryNames>;
template class ASTQueryWithTableAndOutputImpl<ASTExistsDictionaryQueryIDAndQueryNames>;
template class ASTQueryWithTableAndOutputImpl<ASTShowCreateTableQueryIDAndQueryNames>;
template class ASTQueryWithTableAndOutputImpl<ASTShowCreateViewQueryIDAndQueryNames>;
template class ASTQueryWithTableAndOutputImpl<ASTShowCreateDictionaryQueryIDAndQueryNames>;


ASTPtr ASTExistsDatabaseQuery::clone() const
{
    auto res = std::make_shared<ASTExistsDatabaseQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTExistsDatabaseQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << ASTExistsDatabaseQueryIDAndQueryNames::Query
                << " " << (settings.hilite ? hilite_none : "");
    database->formatImpl(settings, state, frame);
}

ASTPtr ASTShowCreateDatabaseQuery::clone() const
{
    auto res = std::make_shared<ASTShowCreateDatabaseQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTShowCreateDatabaseQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << ASTShowCreateDatabaseQueryIDAndQueryNames::Query
                    << " " << (settings.hilite ? hilite_none : "");
    database->formatImpl(settings, state, frame);
}

ASTPtr ASTDescribeQuery::clone() const
{
    auto res = std::make_shared<ASTDescribeQuery>(*this);
    res->children.clear();

    cloneOutputOptions(*res);
    if (table_expression)
        res->set(res->table_expression, table_expression->clone());

    return res;
}

void ASTDescribeQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                    << "DESCRIBE TABLE" << (settings.hilite ? hilite_none : "");
    table_expression->formatImpl(settings, state, frame);
}

void ASTDescribeQuery::forEachPointerToChild(std::function<void(void**)> f)
{
    ASTQueryWithOutput::forEachPointerToChild(f);
    f(reinterpret_cast<void **>(&table_expression));
}

}
