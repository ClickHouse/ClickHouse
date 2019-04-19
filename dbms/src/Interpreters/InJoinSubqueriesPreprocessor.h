#pragma once

#include <string>
#include <memory>
#include <Core/Types.h>
#include <Core/SettingsCommon.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class IStorage;
class ASTSelectQuery;
class Context;


/** Scheme of operation:
  *
  * If "main" table in a query is distributed enough (thus, have at least two shards),
  *  and there are non-GLOBAL subqueries in IN or JOIN,
  *  and in that subqueries, there is a table
  *   (in any nesting level in that subquery or in more deep subqueries),
  *   that exist on local server and (according to information on local server) is also distributed enough
  * then, according to setting 'distributed_product_mode',
  * either
  * - throw an exception;
  * - or add GLOBAL to top subquery;
  * - or replace database and table name in subquery to remote database and table name,
  *   as Distributed storage on local server knows it.
  *
  * Do not recursively preprocess subqueries, as it will be done by calling code.
  */

class InJoinSubqueriesPreprocessor
{
public:
    struct CheckShardsAndTables
    {
        using Ptr = std::unique_ptr<CheckShardsAndTables>;

        /// These methods could be overriden for the need of the unit test.
        virtual bool hasAtLeastTwoShards(const IStorage & table) const;
        virtual std::pair<std::string, std::string> getRemoteDatabaseAndTableName(const IStorage & table) const;
        virtual ~CheckShardsAndTables() {}
    };

    InJoinSubqueriesPreprocessor(const Context & context, CheckShardsAndTables::Ptr _checker = std::make_unique<CheckShardsAndTables>())
        : context(context)
        , checker(std::move(_checker))
    {}

    void visit(ASTPtr & query) const;

private:
    const Context & context;
    CheckShardsAndTables::Ptr checker;
};


}
