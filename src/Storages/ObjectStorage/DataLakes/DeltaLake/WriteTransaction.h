#pragma once
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include "delta_kernel_ffi.hpp"

namespace DeltaLake
{

class WriteTransaction
{
public:
    explicit WriteTransaction(DeltaLake::KernelHelperPtr kernel_helper_);

    const std::string & getDataPath() const { return kernel_helper->getDataPath(); }

    /// Create a transcation.
    void create();

    /// Commit the transaction.
    struct CommitFile
    {
        std::string file_name;
        size_t size;
        DB::Map paritition_values;
    };
    void commit(const std::vector<CommitFile> & files);

    const DB::NamesAndTypesList & getWriteSchema() const { return write_schema; }

    void validateSchema(const DB::Block & header) const;

private:
    using KernelTransaction = DeltaLake::KernelPointerWrapper<ffi::ExclusiveTransaction, ffi::free_transaction>;
    using KernelExternEngine = DeltaLake::KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelWriteContext = DeltaLake::KernelPointerWrapper<ffi::SharedWriteContext, ffi::free_write_context>;
    using KernelEngineData = DeltaLake::KernelPointerWrapper<ffi::ExclusiveEngineData, ffi::free_engine_data>;

    const DeltaLake::KernelHelperPtr kernel_helper;
    const LoggerPtr log;
    std::string write_path;

    KernelExternEngine engine;
    KernelTransaction transaction;
    KernelWriteContext write_context;
    DB::NamesAndTypesList write_schema;
};

using WriteTransactionPtr = std::shared_ptr<WriteTransaction>;

}
