#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Settings.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Common/config_build.h>

namespace DB
{


StorageSystemBuildOptions::StorageSystemBuildOptions(const std::string & name_)
    : name(name_)
    , columns{
        { "name",             std::make_shared<DataTypeString>()    },
        { "value",            std::make_shared<DataTypeString>()    },
    }
{
}

StoragePtr StorageSystemBuildOptions::create(const std::string & name_)
{
    return make_shared(name_);
}


BlockInputStreams StorageSystemBuildOptions::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    ColumnWithTypeAndName col_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name"};
    ColumnWithTypeAndName col_value{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "value"};

    for (auto it = auto_config_build.begin(); it != auto_config_build.end(); ++it) {
        col_name.column->insert(String(*it));
        ++it;
        if (it == auto_config_build.end())
            break;
        col_value.column->insert(String(*it));
    }

    Block block{
        col_name,
        col_value,
    };

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
