#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class IStorage;
class Context;

struct MutationCommand
{
    enum Type
    {
        EMPTY,     /// Not used.
        DELETE,
    };

    Type type = EMPTY;

    ASTPtr predicate;

    static MutationCommand delete_(const ASTPtr & predicate)
    {
        MutationCommand res;
        res.type = DELETE;
        res.predicate = predicate;
        return res;
    }
};

struct MutationCommands
{
    std::vector<MutationCommand> commands;

    void validate(const IStorage & table, const Context & context);
};

}
