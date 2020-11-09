#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <Storages/StorageProxy.h>
#include <Databases/MySQL/DatabaseMaterializeMySQL.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class StorageMaterializeMySQL final : public ext::shared_ptr_helper<StorageMaterializeMySQL>, public StorageProxy
{
    friend struct ext::shared_ptr_helper<StorageMaterializeMySQL>;
public:
    String getName() const override { return "MaterializeMySQL"; }

    StorageMaterializeMySQL(const StoragePtr & nested_storage_, const DatabaseMaterializeMySQL * database_);

    Pipe read(
        const Names & column_names, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info,
        const Context & context, QueryProcessingStage::Enum processed_stage, size_t max_block_size, unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr &, const StorageMetadataPtr &, const Context &) override { throwNotAllowed(); }

    NamesAndTypesList getVirtuals() const override;
    ColumnSizeByName getColumnSizes() const override;

private:
    StoragePtr getNested() const override { return nested_storage; }
    [[noreturn]] void throwNotAllowed() const
    {
        throw Exception("This method is not allowed for MaterializeMySQL", ErrorCodes::NOT_IMPLEMENTED);
    }

    StoragePtr nested_storage;
    const DatabaseMaterializeMySQL * database;
};

}

#endif
