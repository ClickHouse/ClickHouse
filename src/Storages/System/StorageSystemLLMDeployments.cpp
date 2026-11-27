#include <Storages/System/StorageSystemLLMDeployments.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/LLMDeploymentRegistry.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MutationCommands.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

String getDeletedName(const MutationCommands & commands)
{
    if (commands.size() == 1 && commands.front().type == MutationCommand::DELETE)
    {
        auto command = commands.front().ast();
        if (command && command->predicate && !command->partition && !command->partitions)
        {
            const auto * function = command->predicate->as<ASTFunction>();
            if (function && function->name == "equals" && function->arguments && function->arguments->children.size() == 2)
            {
                const auto & args = function->arguments->children;
                const auto * identifier = args[0]->as<ASTIdentifier>();
                const auto * value = args[1]->as<ASTLiteral>();
                if (identifier && identifier->full_name == "name" && value && value->value.getType() == Field::Types::String)
                    return value->value.safeGet<String>();
            }
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM deployment deletion requires WHERE name = 'deployment_name'");
}

class LLMDeploymentsSink final : public SinkToStorage
{
public:
    LLMDeploymentsSink(SharedHeader header, ContextPtr context_)
        : SinkToStorage(std::move(header))
        , context(std::move(context_))
    {
    }

    String getName() const override
    {
        return "LLMDeploymentsSink";
    }

    void consume(Chunk & chunk) override
    {
        VectorWithMemoryTracking<LLMDeployment> deployments;
        const auto & columns = chunk.getColumns();
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            deployments.push_back(
                {String(columns[0]->getDataAt(row)),
                 String(columns[1]->getDataAt(row)),
                 String(columns[2]->getDataAt(row)),
                 String(columns[3]->getDataAt(row)),
                 columns[4]->getUInt(row),
                 columns[5]->getUInt(row),
                 columns[6]->getUInt(row)});
        }
        context->getLLMDeploymentRegistry()->insert(context, deployments);
    }

private:
    ContextPtr context;
};

}

ColumnsDescription StorageSystemLLMDeployments::getColumnsDescription()
{
    return ColumnsDescription{
        {"name", std::make_shared<DataTypeString>(), "Unique deployment name."},
        {"route", std::make_shared<DataTypeString>(), "Logical model name accepted by `llmQuery`."},
        {"credentials", std::make_shared<DataTypeString>(), "Named collection containing connection credentials."},
        {"model", std::make_shared<DataTypeString>(), "Upstream model name."},
        {"weight", std::make_shared<DataTypeUInt64>(), "Routing weight; zero disables the deployment."},
        {"max_requests_per_minute",
         std::make_shared<DataTypeUInt64>(),
         "Per-server request limit in a fixed 60-second window; zero means unlimited."},
        {"max_concurrent_requests", std::make_shared<DataTypeUInt64>(), "Per-server concurrent request limit; zero means unlimited."},
    };
}

StorageSystemLLMDeployments::StorageSystemLLMDeployments(const StorageID & table_id, ColumnsDescription columns_description)
    : IStorageSystemOneBlock(table_id, std::move(columns_description))
{
}

void StorageSystemLLMDeployments::fillData(MutableColumns & columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto snapshot = context->getLLMDeploymentRegistry()->getSnapshot(context);
    for (const auto & deployment : snapshot.deployments)
    {
        columns[0]->insert(deployment.name);
        columns[1]->insert(deployment.route);
        columns[2]->insert(deployment.credentials);
        columns[3]->insert(deployment.model);
        columns[4]->insert(deployment.weight);
        columns[5]->insert(deployment.max_requests_per_minute);
        columns[6]->insert(deployment.max_concurrent_requests);
    }
}

SinkToStoragePtr StorageSystemLLMDeployments::write(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, bool)
{
    auto header = std::make_shared<Block>();
    for (const auto & [name, type] : getColumnsDescription().getInsertable())
        header->insert(ColumnWithTypeAndName(type, name));
    return std::make_shared<LLMDeploymentsSink>(std::move(header), std::move(context));
}

void StorageSystemLLMDeployments::checkMutationIsPossible(const MutationCommands & commands, const Settings &) const
{
    getDeletedName(commands);
}

void StorageSystemLLMDeployments::mutate(const MutationCommands & commands, ContextPtr context)
{
    context->getLLMDeploymentRegistry()->remove(context, getDeletedName(commands));
}

}

namespace DB
{
REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemLLMDeployments)
}
