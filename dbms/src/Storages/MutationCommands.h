#pragma once

#include <Parsers/IAST.h>
#include <IO/WriteHelpers.h>


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

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

struct MutationCommands
{
    std::vector<MutationCommand> commands;

    void validate(const IStorage & table, const Context & context);

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

}
