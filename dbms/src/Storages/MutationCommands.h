#pragma once

#include <Parsers/IAST.h>
#include <IO/WriteHelpers.h>


namespace DB
{

struct MutationCommand
{
    enum Type
    {
        UNKNOWN,
        DELETE,
    };

    Type type = UNKNOWN;

    ASTPtr predicate;

    static MutationCommand delete_(const ASTPtr & predicate)
    {
        MutationCommand res;
        res.type = DELETE;
        res.predicate = predicate;
        return res;
    }

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

struct MutationCommands
{
    std::vector<MutationCommand> commands;

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

}
