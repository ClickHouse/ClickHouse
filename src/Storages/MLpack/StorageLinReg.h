#pragma once

#include <mlpack/methods/linear_regression/linear_regression.hpp>
#include <mlpack/core.hpp>
#include <Storages/IStorage.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/MLpack/LinRegSettings.h>
// #include "LinRegSettings.h"

namespace DB
{

struct LinRegConfiguration
{
    String filepath;
};

/* Implements storage in the MongoDB database.
 * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
 * Read only.
 */

class StorageLinReg final : public IStorage
{
public:
    StorageLinReg(
        const StorageID & table_id_,
        std::unique_ptr<LinRegSettings> linreg_settings_,
        const String filepath,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "LinReg"; }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    static LinRegConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

private:
    const String filepath;
    std::unique_ptr<LinRegSettings> linreg_settings;
    std::shared_ptr<mlpack::regression::LinearRegression> model;
};

}
