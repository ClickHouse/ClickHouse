#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Interpreters/InterpreterKillQueryQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Common/typeid_cast.h>
#include <Parsers/ASTSystemQuery.h>
#include "InterpreterSystemQuery.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int UNKNOWN_TYPE_OF_QUERY;
}


static void throwIfReadOnly(Context & context)
{
    if (context.getSettingsRef().limits.readonly)
        throw Exception("Cannot execute query in readonly mode", ErrorCodes::READONLY);
}


std::unique_ptr<IInterpreter> InterpreterFactory::get(ASTPtr & query, Context & context, QueryProcessingStage::Enum stage)
{
    if (typeid_cast<ASTSelectQuery *>(query.get()))
    {
        return std::make_unique<InterpreterSelectQuery>(query, context, stage);
    }
    else if (typeid_cast<ASTInsertQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterInsertQuery>(query, context);
    }
    else if (typeid_cast<ASTCreateQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterCreateQuery>(query, context);
    }
    else if (typeid_cast<ASTDropQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterDropQuery>(query, context);
    }
    else if (typeid_cast<ASTRenameQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterRenameQuery>(query, context);
    }
    else if (typeid_cast<ASTShowTablesQuery *>(query.get()))
    {
        return std::make_unique<InterpreterShowTablesQuery>(query, context);
    }
    else if (typeid_cast<ASTUseQuery *>(query.get()))
    {
        return std::make_unique<InterpreterUseQuery>(query, context);
    }
    else if (typeid_cast<ASTSetQuery *>(query.get()))
    {
        /// readonly is checked inside InterpreterSetQuery
        return std::make_unique<InterpreterSetQuery>(query, context);
    }
    else if (typeid_cast<ASTOptimizeQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterOptimizeQuery>(query, context);
    }
    else if (typeid_cast<ASTExistsQuery *>(query.get()))
    {
        return std::make_unique<InterpreterExistsQuery>(query, context);
    }
    else if (typeid_cast<ASTShowCreateQuery *>(query.get()))
    {
        return std::make_unique<InterpreterShowCreateQuery>(query, context);
    }
    else if (typeid_cast<ASTDescribeQuery *>(query.get()))
    {
        return std::make_unique<InterpreterDescribeQuery>(query, context);
    }
    else if (typeid_cast<ASTShowProcesslistQuery *>(query.get()))
    {
        return std::make_unique<InterpreterShowProcesslistQuery>(query, context);
    }
    else if (typeid_cast<ASTAlterQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterAlterQuery>(query, context);
    }
    else if (typeid_cast<ASTCheckQuery *>(query.get()))
    {
        return std::make_unique<InterpreterCheckQuery>(query, context);
    }
    else if (typeid_cast<ASTKillQueryQuery *>(query.get()))
    {
        return std::make_unique<InterpreterKillQueryQuery>(query, context);
    }
    else if (typeid_cast<ASTSystemQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterSystemQuery>(query, context);
    }
    else
        throw Exception("Unknown type of query: " + query->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
}

}
