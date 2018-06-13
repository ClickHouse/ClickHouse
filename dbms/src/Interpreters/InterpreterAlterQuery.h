#pragma once

#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTAlterQuery.h>


namespace DB
{

/** Allows you add or remove a column in the table.
  * It also allows you to manipulate the partitions of the MergeTree family tables.
  */
class InterpreterAlterQuery : public IInterpreter
{
public:
    InterpreterAlterQuery(const ASTPtr & query_ptr_, const Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    const Context & context;
};

}
