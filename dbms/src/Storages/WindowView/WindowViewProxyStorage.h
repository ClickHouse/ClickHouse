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
    WindowViewProxyStorage(StoragePtr storage_, BlockInputStreams streams_, QueryProcessingStage::Enum to_stage_)
    : storage(std::move(storage_))
    , streams(std::move(streams_))
    , to_stage(to_stage_) {}

public:
    std::string getName() const override { return "WindowViewProxyStorage(" + storage->getName() + ")"; }
    std::string getTableName() const override { return storage->getTableName(); }

    bool isRemote() const override { return storage->isRemote(); }
    bool supportsSampling() const override { return storage->supportsSampling(); }
    bool supportsFinal() const override { return storage->supportsFinal(); }
    bool supportsPrewhere() const override { return storage->supportsPrewhere(); }
    bool supportsReplication() const override { return storage->supportsReplication(); }
    bool supportsDeduplication() const override { return storage->supportsDeduplication(); }

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

    bool supportsIndexForIn() const override { return storage->supportsIndexForIn(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override { return storage->mayBenefitFromIndexForIn(left_in_operand, query_context); }
    ASTPtr getPartitionKeyAST() const override { return storage->getPartitionKeyAST(); }
    ASTPtr getSortingKeyAST() const override { return storage->getSortingKeyAST(); }
    ASTPtr getPrimaryKeyAST() const override { return storage->getPrimaryKeyAST(); }
    ASTPtr getSamplingKeyAST() const override { return storage->getSamplingKeyAST(); }
    Names getColumnsRequiredForPartitionKey() const override { return storage->getColumnsRequiredForPartitionKey(); }
    Names getColumnsRequiredForSortingKey() const override { return storage->getColumnsRequiredForSortingKey(); }
    Names getColumnsRequiredForPrimaryKey() const override { return storage->getColumnsRequiredForPrimaryKey(); }
    Names getColumnsRequiredForSampling() const override { return storage->getColumnsRequiredForSampling(); }
    Names getColumnsRequiredForFinal() const override { return storage->getColumnsRequiredForFinal(); }

    const ColumnsDescription & getColumns() const override { return storage->getColumns(); }

    void setColumns(ColumnsDescription columns_) override { return storage->setColumns(columns_); }

    NameAndTypePair getColumn(const String & column_name) const override { return storage->getColumn(column_name); }

    bool hasColumn(const String & column_name) const override { return storage->hasColumn(column_name); }

private:
    StoragePtr storage;
    BlockInputStreams streams;
    QueryProcessingStage::Enum to_stage;
    NameAndTypePair column_end;
};
}
