#pragma once

#include <string>
#include <Core/Types.h>
#include <Interpreters/SettingsCommon.h>


namespace DB
{

class IAST;
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
    InJoinSubqueriesPreprocessor(const Context & context) : context(context) {}
    void process(ASTSelectQuery * query) const;

    /// These methods could be overriden for the need of the unit test.
    virtual bool hasAtLeastTwoShards(const IStorage & table) const;
    virtual std::pair<std::string, std::string> getRemoteDatabaseAndTableName(const IStorage & table) const;
    virtual ~InJoinSubqueriesPreprocessor() {}

private:
    const Context & context;
};


}
