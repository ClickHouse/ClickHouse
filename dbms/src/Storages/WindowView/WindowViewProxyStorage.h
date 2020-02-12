#pragma once

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/IStorage.h>

namespace DB
{

class WindowViewProxyStorage : public IStorage
{
public:
    WindowViewProxyStorage(const StorageID & table_id_, StoragePtr parent_storage_, BlockInputStreams streams_, QueryProcessingStage::Enum to_stage_)
    : IStorage(table_id_)
    , parent_storage(std::move(parent_storage_))
    , streams(std::move(streams_))
    , to_stage(to_stage_) {}

public:
    std::string getName() const override { return "WindowViewProxyStorage(" + parent_storage->getName() + ")"; }

    bool isRemote() const override { return parent_storage->isRemote(); }
    bool supportsSampling() const override { return parent_storage->supportsSampling(); }
    bool supportsFinal() const override { return parent_storage->supportsFinal(); }
    bool supportsPrewhere() const override { return parent_storage->supportsPrewhere(); }
    bool supportsReplication() const override { return parent_storage->supportsReplication(); }
    bool supportsDeduplication() const override { return parent_storage->supportsDeduplication(); }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context & /*context*/) const override { return to_stage; }

    BlockInputStreams read(
            const Names & /*column_names*/,
            const SelectQueryInfo & /*query_info*/,
            const Context & /*context*/,
            QueryProcessingStage::Enum /*processed_stage*/,
            size_t /*max_block_size*/,
            unsigned /*num_streams*/) override
    {
        return streams;
    }

    bool supportsIndexForIn() const override { return parent_storage->supportsIndexForIn(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override { return parent_storage->mayBenefitFromIndexForIn(left_in_operand, query_context); }
    ASTPtr getPartitionKeyAST() const override { return parent_storage->getPartitionKeyAST(); }
    ASTPtr getSortingKeyAST() const override { return parent_storage->getSortingKeyAST(); }
    ASTPtr getPrimaryKeyAST() const override { return parent_storage->getPrimaryKeyAST(); }
    ASTPtr getSamplingKeyAST() const override { return parent_storage->getSamplingKeyAST(); }
    Names getColumnsRequiredForPartitionKey() const override { return parent_storage->getColumnsRequiredForPartitionKey(); }
    Names getColumnsRequiredForSortingKey() const override { return parent_storage->getColumnsRequiredForSortingKey(); }
    Names getColumnsRequiredForPrimaryKey() const override { return parent_storage->getColumnsRequiredForPrimaryKey(); }
    Names getColumnsRequiredForSampling() const override { return parent_storage->getColumnsRequiredForSampling(); }
    Names getColumnsRequiredForFinal() const override { return parent_storage->getColumnsRequiredForFinal(); }

    const ColumnsDescription & getColumns() const override { return parent_storage->getColumns(); }

    void setColumns(ColumnsDescription columns_) override { return parent_storage->setColumns(columns_); }

    NameAndTypePair getColumn(const String & column_name) const override { return parent_storage->getColumn(column_name); }

    bool hasColumn(const String & column_name) const override { return parent_storage->hasColumn(column_name); }

private:
    StoragePtr parent_storage;
    BlockInputStreams streams;
    QueryProcessingStage::Enum to_stage;
};
}
