#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>

#include <optional>
#include <vector>


namespace DB
{

class ASTAlterCommand;

struct ReplicaCommand
{
    enum Type
    {
        DROP_REPLICA,
    };

    Type type;

    ASTPtr replica;
    String replica_name;

    static std::optional<ReplicaCommand> parse(const ASTAlterCommand * command);
};

class ReplicaCommands : public std::vector<ReplicaCommand>
{
public:
    void validate(const IStorage & table);
};


}
