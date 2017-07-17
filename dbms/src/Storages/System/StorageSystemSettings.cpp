#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemSettings.h>


namespace DB
{


StorageSystemSettings::StorageSystemSettings(const std::string & name_)
    : name(name_)
    , columns{
        { "name",             std::make_shared<DataTypeString>()    },
        { "value",            std::make_shared<DataTypeString>()    },
        { "changed",         std::make_shared<DataTypeUInt8>()    },
    }
{
}


BlockInputStreams StorageSystemSettings::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    const Settings & settings = context.getSettingsRef();

    ColumnWithTypeAndName col_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name"};
    ColumnWithTypeAndName col_value{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "value"};
    ColumnWithTypeAndName col_changed{std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "changed"};

#define ADD_SETTING(TYPE, NAME, DEFAULT) \
    col_name.column->insert(String(#NAME)); \
    col_value.column->insert(settings.NAME.toString()); \
    col_changed.column->insert(UInt64(settings.NAME.changed));

    APPLY_FOR_SETTINGS(ADD_SETTING)
#undef ADD_SETTING

#define ADD_LIMIT(TYPE, NAME, DEFAULT) \
    col_name.column->insert(String(#NAME)); \
    col_value.column->insert(settings.limits.NAME.toString()); \
    col_changed.column->insert(UInt64(settings.limits.NAME.changed));

    APPLY_FOR_LIMITS(ADD_LIMIT)
#undef ADD_LIMIT

    Block block{
        col_name,
        col_value,
        col_changed,
    };

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
