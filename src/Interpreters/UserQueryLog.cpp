#include "UserQueryLog.h"
#include "Context.h"
#include "Parsers/ASTCreateQuery.h"
#include "Storages/StorageFactory.h"

#include "DatabaseCatalog.h"
#include "QueryLog.h"


namespace DB
{

static ASTPtr getParsedQuery()
{
    ParserCreateViewQuery parser;
    String queryStr = "create view system.user_query_log as select * from system.query_log prewhere user = currentUser()";
    return parseQuery(parser, queryStr.data(), queryStr.data() + queryStr.size(), "", 0, 0, 0);
}

UserQueryLog::UserQueryLog(
    ContextMutablePtr context_, const SystemLogSettings & settings_, std::shared_ptr<SystemLogQueue<UserQueryLogElement>> queue_)
    : SystemLog(std::move(context_), settings_, std::move(queue_))
    , create(getParsedQuery())
{
}

StoragePtr UserQueryLog::getStorage() const
{
    auto res = StorageFactory::instance().get(*create->as<ASTCreateQuery>(), "" /*relative data path??*/, getContext(), getContext()->getGlobalContext(),
        QueryLogElement::getColumnsDescription(), {} /*constraints description??*/, LoadingStrictnessLevel::CREATE, false);
    res->startup();

    // TODO: Is this correct?
    DatabaseCatalog::instance().getSystemDatabase()->attachTable(getContext(), "user_query_log", res, "" /*relative data path??*/);
    return res;
}

}
