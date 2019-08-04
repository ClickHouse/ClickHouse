#pragma once

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/IStorage.h>


namespace DB
{
/// The table has all the properties of another storage,
///  but will read single prepared block instead.
/// Used in PushingToViewsBlockOutputStream for generating alias columns
/// NOTE: Some of the properties seems redundant.
class StorageBlock : public ext::shared_ptr_helper<StorageBlock>, public IStorage
{
public:
    std::string getName() const override { return storage->getName(); }
    std::string getTableName() const override { return storage->getTableName(); }
    std::string getDatabaseName() const override { return storage->getDatabaseName(); }
    bool isRemote() const override { return storage->isRemote(); }
    bool supportsSampling() const override { return storage->supportsSampling(); }
    bool supportsFinal() const override { return storage->supportsFinal(); }
    bool supportsPrewhere() const override { return storage->supportsPrewhere(); }
    bool supportsReplication() const override { return storage->supportsReplication(); }
    bool supportsDeduplication() const override { return storage->supportsDeduplication(); }
    bool supportsIndexForIn() const override { return storage->supportsIndexForIn(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override
    {
        return storage->mayBenefitFromIndexForIn(left_in_operand, query_context);
    }
    ASTPtr getPartitionKeyAST() const override { return storage->getPartitionKeyAST(); }
    ASTPtr getSortingKeyAST() const override { return storage->getSortingKeyAST(); }
    ASTPtr getPrimaryKeyAST() const override { return storage->getPrimaryKeyAST(); }
    ASTPtr getSamplingKeyAST() const override { return storage->getSamplingKeyAST(); }
    Names getColumnsRequiredForPartitionKey() const override { return storage->getColumnsRequiredForPartitionKey(); }
    Names getColumnsRequiredForSortingKey() const override { return storage->getColumnsRequiredForSortingKey(); }
    Names getColumnsRequiredForPrimaryKey() const override { return storage->getColumnsRequiredForPrimaryKey(); }
    Names getColumnsRequiredForSampling() const override { return storage->getColumnsRequiredForSampling(); }
    Names getColumnsRequiredForFinal() const override { return storage->getColumnsRequiredForFinal(); }

    BlockInputStreams read(const Names &, const SelectQueryInfo &, const Context &, QueryProcessingStage::Enum, size_t, unsigned) override
    {
        return {std::make_shared<OneBlockInputStream>(std::move(block))};
    }

private:
    Block block;
    StoragePtr storage;

protected:
    StorageBlock(Block block_, StoragePtr storage_)
        : IStorage{storage_->getColumns()}, block(std::move(block_)), storage(storage_)
    {
    }
};

}
