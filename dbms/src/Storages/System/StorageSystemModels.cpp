#include <Storages/System/StorageSystemModels.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalModels.h>
#include <Dictionaries/CatBoostModel.h>
namespace DB
{

StorageSystemModels::StorageSystemModels(const std::string & name)
        : name{name},
          columns{
                  { "name", std::make_shared<DataTypeString>() },
                  { "origin", std::make_shared<DataTypeString>() },
                  { "type", std::make_shared<DataTypeString>() },
                  { "creation_time", std::make_shared<DataTypeDateTime>() },
                  { "last_exception", std::make_shared<DataTypeString>() }
          }
{
}


BlockInputStreams StorageSystemModels::read(
        const Names & column_names,
        const SelectQueryInfo &,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        const size_t,
        const unsigned)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    ColumnWithTypeAndName col_name{ColumnString::create(), std::make_shared<DataTypeString>(), "name"};
    ColumnWithTypeAndName col_origin{ColumnString::create(), std::make_shared<DataTypeString>(), "origin"};
    ColumnWithTypeAndName col_type{ColumnString::create(), std::make_shared<DataTypeString>(), "type"};

    ColumnWithTypeAndName col_creation_time{ColumnUInt32::create(), std::make_shared<DataTypeDateTime>(), "creation_time"};
    ColumnWithTypeAndName col_last_exception{ColumnString::create(), std::make_shared<DataTypeString>(), "last_exception"};

    const auto & external_models = context.getExternalModels();
    auto objects_map = external_models.getObjectsMap();
    const auto & models = objects_map.get();

    for (const auto & model_info : models)
    {
        col_name.column->insert(model_info.first);
        col_origin.column->insert(model_info.second.origin);

        if (model_info.second.loadable)
        {
            const auto model_ptr = std::static_pointer_cast<IModel>(model_info.second.loadable);

            col_type.column->insert(model_ptr->getTypeName());
            col_creation_time.column->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(model_ptr->getCreationTime())));
        }
        else
        {
            col_type.column->insertDefault();
            col_creation_time.column->insertDefault();
        }

        if (model_info.second.exception)
        {
            try
            {
                std::rethrow_exception(model_info.second.exception);
            }
            catch (...)
            {
                col_last_exception.column->insert(getCurrentExceptionMessage(false));
            }
        }
        else
            col_last_exception.column->insertDefault();
    }

    Block block{
            col_name,
            col_origin,
            col_type,
            col_creation_time,
            col_last_exception
    };

    return BlockInputStreams{1, std::make_shared<OneBlockInputStream>(block)};
}

}
