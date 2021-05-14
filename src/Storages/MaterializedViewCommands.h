#pragma once

#include <optional>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/StorageMaterializedView.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

struct MaterializedViewCommand
{
    enum Type
    {
        REFRESH
    };

    Type type;

    ASTPtr values;

    static MaterializedViewCommand refresh(const ASTPtr & values)
    {
        MaterializedViewCommand res;
        res.type = REFRESH;
        res.values = values;
        return res;
    }

    static std::optional<MaterializedViewCommand> parse(ASTAlterCommand * command)
    {
        if (command->type == ASTAlterCommand::VIEW_REFRESH)
            return refresh(command->values);
        return {};
    }
};


class MaterializedViewCommands : public std::vector<MaterializedViewCommand>
{
public:
    void validate(const IStorage & table)
    {
        if (!empty() && !dynamic_cast<const StorageMaterializedView *>(&table))
            throw Exception("Wrong storage type. Must be StorageMaterializedView", DB::ErrorCodes::UNKNOWN_STORAGE);
    }
};

}
