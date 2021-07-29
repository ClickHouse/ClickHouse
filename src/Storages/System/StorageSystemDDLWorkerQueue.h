#pragma once

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Interpreters/DDLWorker.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{
class Context;


/** System table "distributed_ddl_queue" with list of queries that are currently in the DDL worker queue.
  */
class StorageSystemDDLWorkerQueue final : public shared_ptr_helper<StorageSystemDDLWorkerQueue>,
                                          public IStorageSystemOneBlock<StorageSystemDDLWorkerQueue>
{
    friend struct shared_ptr_helper<StorageSystemDDLWorkerQueue>;
    Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemDDLWorkerQueue"; }

    static NamesAndTypesList getNamesAndTypes();
};
}
