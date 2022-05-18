#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/*
 * Implements `foundationdb` system table, which allows you to view the metadata
 * in FoundationDB for debugging purposes.
 *
 * Metadata can be selected by its type and namespace. The type can be one of
 * database, table, part or config. The namespace refers to the object to which
 * the metadata belongs.
 *
 * For example, a part belongs to a table, you need the database name and table
 * name to find the part metadata:
 *
 *    SELECT * FROM system.foundationdb WHERE type = 'part' and key[1] = 'table uuid' and key[2] = 'part name' 
 *
 * The metadata will be converted to a readable form.
 */
class StorageSystemFoundationDB final : public IStorageSystemOneBlock<StorageSystemFoundationDB>
{
public:
    std::string getName() const override { return "SystemFoundationDB"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};
}
