#include <Storages/System/StorageSystemModels.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/CatBoostModel.h>


namespace DB
{

NamesAndTypesList StorageSystemModels::getNamesAndTypes()
{
    return {
        { "name", std::make_shared<DataTypeString>() },
        { "status", std::make_shared<DataTypeEnum8>(getStatusEnumAllPossibleValues()) },
        { "origin", std::make_shared<DataTypeString>() },
        { "type", std::make_shared<DataTypeString>() },
        { "loading_start_time", std::make_shared<DataTypeDateTime>() },
        { "loading_duration", std::make_shared<DataTypeFloat32>() },
        //{ "creation_time", std::make_shared<DataTypeDateTime>() },
        { "last_exception", std::make_shared<DataTypeString>() },
    };
}

void StorageSystemModels::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto & external_models_loader = context->getExternalModelsLoader();
    auto load_results = external_models_loader.getLoadResults();

    for (const auto & load_result : load_results)
    {
        res_columns[0]->insert(load_result.name);
        res_columns[1]->insert(static_cast<Int8>(load_result.status));
        res_columns[2]->insert(load_result.config ? load_result.config->path : "");

        if (load_result.object)
        {
            const auto model_ptr = std::static_pointer_cast<const IMLModel>(load_result.object);
            res_columns[3]->insert(model_ptr->getTypeName());
        }
        else
        {
            res_columns[3]->insertDefault();
        }

        res_columns[4]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(load_result.loading_start_time)));
        res_columns[5]->insert(std::chrono::duration_cast<std::chrono::duration<float>>(load_result.loading_duration).count());

        if (load_result.exception)
            res_columns[6]->insert(getExceptionMessage(load_result.exception, false));
        else
            res_columns[6]->insertDefault();
    }
}

}
