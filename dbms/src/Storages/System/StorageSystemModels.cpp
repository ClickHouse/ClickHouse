#include <Storages/System/StorageSystemModels.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalModels.h>
#include <Dictionaries/CatBoostModel.h>
namespace DB
{

NamesAndTypesList StorageSystemModels::getNamesAndTypes()
{
    return {
        { "name", std::make_shared<DataTypeString>() },
        { "origin", std::make_shared<DataTypeString>() },
        { "type", std::make_shared<DataTypeString>() },
        { "creation_time", std::make_shared<DataTypeDateTime>() },
        { "last_exception", std::make_shared<DataTypeString>() },
    };
}

void StorageSystemModels::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    const auto & external_models = context.getExternalModels();
    auto objects_map = external_models.getObjectsMap();
    const auto & models = objects_map.get();

    for (const auto & model_info : models)
    {
        res_columns[0]->insert(model_info.first);
        res_columns[1]->insert(model_info.second.origin);

        if (model_info.second.loadable)
        {
            const auto model_ptr = std::static_pointer_cast<IModel>(model_info.second.loadable);

            res_columns[2]->insert(model_ptr->getTypeName());
            res_columns[3]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(model_ptr->getCreationTime())));
        }
        else
        {
            res_columns[2]->insertDefault();
            res_columns[3]->insertDefault();
        }

        if (model_info.second.exception)
        {
            try
            {
                std::rethrow_exception(model_info.second.exception);
            }
            catch (...)
            {
                res_columns[4]->insert(getCurrentExceptionMessage(false));
            }
        }
        else
            res_columns[4]->insertDefault();
    }
}

}
