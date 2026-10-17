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

ColumnsDescription StorageSystemModels::getColumnsDescription()
{
    return ColumnsDescription
    {
        { "model_path", std::make_shared<DataTypeString>(), "Path to trained model."},
        { "type", std::make_shared<DataTypeString>(), "Model type. Now catboost only."},
        { "loading_start_time", std::make_shared<DataTypeDateTime>(), "The time when the loading of the model started."},
        { "loading_duration", std::make_shared<DataTypeFloat32>(), "How much time did it take to load the model."},
    };
}

void StorageSystemModels::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
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
