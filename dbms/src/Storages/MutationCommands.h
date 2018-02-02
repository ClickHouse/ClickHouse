#pragma once

#include <Parsers/IAST.h>


namespace DB
{

struct MutationCommand
{
    enum Type
    {
        DELETE,
    };

    Type type;

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
};

}
