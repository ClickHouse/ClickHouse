#pragma once

#include <mlpack/methods/linear_regression/linear_regression.hpp>
#include <mlpack/core.hpp>
#include <Storages/IStorage.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/MLpack/IModel.h>

namespace DB
{

struct MLmodelConfiguration
{
    String filepath;
};

/* Implements storage in the MongoDB database.
 * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
 * Read only.
 */

class StorageMLmodel final : public IStorage
{
public:
    StorageMLmodel(
        const StorageID & table_id_,
        IModelPtr model_,
        const String filepath,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "MLmodel"; }

    // maybe check on null?
    IModelPtr getModel() const { return model; }

    // check on null?
    String getModelName() const { return model->GetName(); }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    static MLmodelConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

private:
    const String filepath;
    IModelPtr model;
};

}
