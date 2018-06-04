#pragma once

#include <Storages/IStorage.h>
#include <Core/Defines.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{

class StorageCatBoostPool : public ext::shared_ptr_helper<StorageCatBoostPool>, public IStorage
{
public:
    std::string getName() const override { return "CatBoostPool"; }

    std::string getTableName() const override { return table_name; }

    BlockInputStreams read(const Names & column_names,
                           const SelectQueryInfo & query_info,
                           const Context & context,
                           QueryProcessingStage::Enum & processed_stage,
                           size_t max_block_size,
                           unsigned threads) override;

private:
    String table_name;

    String column_description_file_name;
    String data_description_file_name;
    Block sample_block;

    enum class DatasetColumnType
    {
        Target,
        Num,
        Categ,
        Auxiliary,
        DocId,
        Weight,
        Baseline
    };

    using ColumnTypesMap = std::map<std::string, DatasetColumnType>;

    ColumnTypesMap getColumnTypesMap() const
    {
        return
        {
                {"Target", DatasetColumnType::Target},
                {"Num", DatasetColumnType::Num},
                {"Categ", DatasetColumnType::Categ},
                {"Auxiliary", DatasetColumnType::Auxiliary},
                {"DocId", DatasetColumnType::DocId},
                {"Weight", DatasetColumnType::Weight},
                {"Baseline", DatasetColumnType::Baseline},
        };
    }

    std::string getColumnTypesString(const ColumnTypesMap & columnTypesMap);

    struct ColumnDescription
    {
        std::string column_name;
        std::string alias;
        DatasetColumnType column_type;

        ColumnDescription() : column_type(DatasetColumnType::Num) {}
        ColumnDescription(std::string column_name, std::string alias, DatasetColumnType column_type)
                : column_name(std::move(column_name)), alias(std::move(alias)), column_type(column_type) {}
    };

    std::vector<ColumnDescription> columns_description;

    void checkDatasetDescription();
    void parseColumnDescription();
    void createSampleBlockAndColumns();

protected:
    StorageCatBoostPool(const Context & context, String column_description_file_name, String data_description_file_name);
};

}
