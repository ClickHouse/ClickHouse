#include <TableFunctions/TableFunctionPredict.h>

#include <Parsers/ASTPredictQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/StorageMemory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Models/ModelRegistry.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/StorageValues.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionPredict::parseArguments(const ASTPtr & ast_function, ContextPtr)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());

    auto * predict = args_func[0]->as<ASTPredictQuery>();

    model_name = predict->model_name->as<ASTIdentifier>()->name();
    table_name = predict->table_name->as<ASTIdentifier>()->name();
}

StoragePtr TableFunctionPredict::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & tf_table_name,
    ColumnsDescription /*cached_columns*/,
    bool) const
{
    StorageID table_id{context->getCurrentDatabase(), table_name};
    StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, context);

    auto ctx = std::const_pointer_cast<Context>(context);
    auto io = executeQuery("SELECT * FROM " + table_name, ctx, QueryFlags{ .internal = true }, QueryProcessingStage::Complete).second;
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
                if (column.name == "target")
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


    auto model = ModelRegistry::instance().getModel(model_name);
    // Targets y_hat = model->predict(X);
    Targets y_hat;

    auto col = ColumnFloat64::create();
    auto& data = col->getData();
    std::copy(y_hat.begin(), y_hat.end(), data.begin());

    Block result_block;
    result_block.insert({std::move(col),
                        std::make_shared<DataTypeFloat64>(),
                        "prediction"});

    ColumnsDescription descr{{{"prediction",
                            std::make_shared<DataTypeFloat64>()}}};

    auto storage_pred = std::make_shared<StorageValues>(
        StorageID{context->getCurrentDatabase(), tf_table_name},
        descr,
        result_block);

    storage_pred->startup();
    return storage_pred;
}

void registerTableFunctionPredict(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPredict>(
        {.documentation
         = {.description = "Runs model's predictions on the input table",
            .returned_value = "A table containing a single row - predicted target values of the model",
            .category = FunctionDocumentation::Category::TableFunction
         },
         .allow_readonly = true});
}

}
