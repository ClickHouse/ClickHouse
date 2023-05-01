#include <Storages/System/StorageSystemModels.h>
#include <Common/ExternalModelInfo.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/Context.h>
#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>


namespace DB
{

NamesAndTypesList StorageSystemModels::getNamesAndTypes()
{
    return {
        { "model_path", std::make_shared<DataTypeString>() },
        { "type", std::make_shared<DataTypeString>() },
        { "loading_start_time", std::make_shared<DataTypeDateTime>() },
        { "loading_duration", std::make_shared<DataTypeFloat32>() },
    };
}

void StorageSystemModels::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto bridge_helper = std::make_unique<CatBoostLibraryBridgeHelper>(context);
    ExternalModelInfos infos = bridge_helper->listModels();

    for (const auto & info : infos)
    {
        res_columns[0]->insert(info.model_path);
        res_columns[1]->insert(info.model_type);
        res_columns[2]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(info.loading_start_time)));
        res_columns[3]->insert(std::chrono::duration_cast<std::chrono::duration<float>>(info.loading_duration).count());
    }
}

}
