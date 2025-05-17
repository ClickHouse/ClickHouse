#include <Interpreters/InterpreterCreateModelQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateModelQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Interpreters/executeQuery.h>
#include <Columns/IColumn.h>

#include <Models/ModelRegistry.h>
#include <Models/createModel.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_TYPE_OF_FIELD;
}

namespace
{

HyperParameters getHyperParameters(ASTPtr options_ptr)
{
    if (!options_ptr)
        return {};

    ASTSetQuery* options = options_ptr->as<ASTSetQuery>();

    HyperParameters hyperparamers;

    for (const auto& pair: options->changes)
    {
        const auto type = pair.value.getType();
        String value;

        switch (type)
        {
            case Field::Types::Which::UInt64:
            case Field::Types::Which::Int64:
                value = toString(pair.value.safeGet<UInt64>());
                break;
            case Field::Types::Which::Float64:
                value = toString(pair.value.safeGet<Float64>());
                break;
            case Field::Types::Which::String:
                value = pair.value.safeGet<String>();
                break;
            default:
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "Unsupported field type {} for field {}", type, pair.name);

        }
        hyperparamers[pair.name] = std::move(value);
    }

    return hyperparamers;
}

}

BlockIO InterpreterCreateModelQuery::execute()
{
    auto context = getContext();

    const auto & create_model_query = query_ptr->as<const ASTCreateModelQuery &>();

    // TODO: access
    // current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION, query.collection_name);

    const String model_name = create_model_query.model_name->as<ASTIdentifier>()->name();
    const String algorithm = create_model_query.algorithm->as<ASTLiteral>()->value.safeGet<String>();
    const HyperParameters hyperparameters = getHyperParameters(create_model_query.options);
    const String target_name = create_model_query.target->as<ASTLiteral>()->value.safeGet<String>();
    const String table_name = create_model_query.table_name->as<ASTIdentifier>()->name();

    {
        StorageID table_id{context->getCurrentDatabase(), table_name};
        StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, context);

        const auto metadata_snapshot = storage->getInMemoryMetadataPtr();

        if (!metadata_snapshot->columns.hasPhysical(target_name))
            throw Exception(
                ErrorCodes::THERE_IS_NO_COLUMN,
                "Model {} requires target column {} which does not exist in the table {}",
                model_name, target_name, table_name
            );
    }

    auto io = executeQuery("SELECT * FROM " + table_name, context, QueryFlags{ .internal = true }, QueryProcessingStage::Complete).second;
    PullingPipelineExecutor executor(io.pipeline);

    FeatureMatrix feature_matrix;
    Targets targets;

    Block block;
    while (executor.pull(block)) {
        for (size_t row = 0; row < block.rows(); ++row) {
            Features features;
            Target target;

            for (const auto& column: block.getColumnsWithTypeAndName())
            {
                const Feature value = column.column->getFloat64(row);
                if (column.name == target_name)
                {
                    target = value;
                } else {
                    features.push_back(value);
                }
            }

            feature_matrix.push_back(std::move(features));
            targets.push_back(target);
        }
    }

    // Create, register and fit the model

    ModelPtr model = createModel(algorithm, hyperparameters);

    ModelRegistry::instance().registerModel(
        model_name,
        model
    );

    model->fit(feature_matrix, targets);

    return {};
}

void registerInterpreterCreateModelQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateModelQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateModelQuery", create_fn);
}

}
