#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include "delta_kernel_ffi.hpp"

namespace DeltaLake
{

class WriteTransaction
{
public:
    explicit WriteTransaction(DeltaLake::KernelHelperPtr kernel_helper_);

    const std::string & getDataPath() const;

    /// Create a transcation.
    void create();

    struct CommitFile
    {
        std::string file_name;
        size_t size_bytes;
        size_t size_rows;
        DB::Map paritition_values;
    };

    /// Commit written files to DeltaLake.
    void commit(const std::vector<CommitFile> & files);

    /// Validate if schema is consistent with the write schema of the transaction.
    void validateSchema(const DB::Block & header) const;

private:
    using KernelTransaction = DeltaLake::KernelPointerWrapper<ffi::ExclusiveTransaction, ffi::free_transaction>;
    using KernelExternEngine = DeltaLake::KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelWriteContext = DeltaLake::KernelPointerWrapper<ffi::SharedWriteContext, ffi::free_write_context>;
    using KernelEngineData = DeltaLake::KernelPointerWrapper<ffi::ExclusiveEngineData, ffi::free_engine_data>;

    const DeltaLake::KernelHelperPtr kernel_helper;
    const LoggerPtr log;
    std::string write_path;
    std::string path_prefix;

    KernelExternEngine engine;
    KernelTransaction transaction;
    KernelWriteContext write_context;
    DB::NamesAndTypesList write_schema;

    void assertTransactionCreated() const;
};

using WriteTransactionPtr = std::shared_ptr<WriteTransaction>;

}

#endif
