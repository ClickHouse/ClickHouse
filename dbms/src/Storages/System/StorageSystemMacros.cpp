#include <Common/Macros.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemMacros.h>
#include <Interpreters/Context.h>


namespace DB
{


StorageSystemMacros::StorageSystemMacros(const std::string & name_)
        : name(name_)
{
    setColumns(ColumnsDescription({
            {"macro", std::make_shared<DataTypeString>()},
            {"substitution", std::make_shared<DataTypeString>()},
    }));
}


BlockInputStreams StorageSystemMacros::read(
        const Names & column_names,
        const SelectQueryInfo &,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        const size_t /*max_block_size*/,
        const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    auto macros = context.getMacros();

    for (const auto & macro : macros->getMacroMap())
    {
        res_columns[0]->insert(macro.first);
        res_columns[1]->insert(macro.second);
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


}
