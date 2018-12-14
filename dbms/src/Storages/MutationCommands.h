#pragma once

#include <Parsers/ASTAlterQuery.h>
#include <optional>
#include <unordered_map>


namespace DB
{

class IStorage;
class Context;
class WriteBuffer;
class ReadBuffer;

struct MutationCommand
{
    ASTPtr ast; /// The AST of the whole command

    enum Type
    {
        EMPTY,     /// Not used.
        DELETE,
        UPDATE,
    };

    Type type = EMPTY;

    ASTPtr predicate;

    std::unordered_map<String, ASTPtr> column_to_update_expression;

    static std::optional<MutationCommand> parse(ASTAlterCommand * command);
};

class MutationCommands : public std::vector<MutationCommand>
{
public:
    std::shared_ptr<ASTAlterCommandList> ast() const;

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

}
