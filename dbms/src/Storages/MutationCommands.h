#pragma once

#include <Parsers/ASTAlterQuery.h>
#include <optional>


namespace DB
{

class IStorage;
class Context;

struct MutationCommand
{
    ASTPtr ast; /// The AST of the whole command

    enum Type
    {
        EMPTY,     /// Not used.
        DELETE,
    };

    Type type = EMPTY;

    ASTPtr predicate;

    static std::optional<MutationCommand> parse(ASTAlterCommand * command);
};

class MutationCommands : public std::vector<MutationCommand>
{
public:
    std::shared_ptr<ASTAlterCommandList> ast() const;

    void validate(const IStorage & table, const Context & context) const;
};

}
