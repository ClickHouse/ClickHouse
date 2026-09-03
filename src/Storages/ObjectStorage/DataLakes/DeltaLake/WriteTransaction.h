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
    WriteTransaction(DeltaLake::KernelHelperPtr kernel_helper_, DB::NamesAndTypesList table_schema_);

    const std::string & getDataPath() const;

    /// Create a transaction for the target table using the schema passed at construction; see the
    /// implementation for how partitioned vs unpartitioned tables derive the write context.
    void create(const DB::Names & partition_columns);

    /// Create a brand-new Delta table by writing the initial commit. Throws if `_delta_log` already has commits.
    void createTable();

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

    /// The Delta table's write schema (authoritative types and nullability, one entry per
    /// column). Nullable Delta columns are wrapped in `DataTypeNullable`.
    const DB::NamesAndTypesList & getWriteSchema() const;

private:
    using KernelTransaction = DeltaLake::KernelPointerWrapper<ffi::ExclusiveTransaction, ffi::free_transaction>;
    using KernelExternEngine = DeltaLake::KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelWriteContext = DeltaLake::KernelPointerWrapper<ffi::SharedWriteContext, ffi::free_write_context>;
    using KernelEngineData = DeltaLake::KernelPointerWrapper<ffi::ExclusiveEngineData, ffi::free_engine_data>;

    const DeltaLake::KernelHelperPtr kernel_helper;
    /// Table logical schema, provided at construction. Used to create the table and, for partitioned
    /// tables, as the write schema (the kernel exposes no partitioned write context via FFI).
    const DB::NamesAndTypesList table_schema;
    const LoggerPtr log;
    std::string write_path;
    std::string path_prefix;

    KernelExternEngine engine;
    KernelTransaction transaction;
    KernelWriteContext unpartitioned_write_context;
    DB::NamesAndTypesList write_schema;

    void assertTransactionCreated() const;
};

using WriteTransactionPtr = std::shared_ptr<WriteTransaction>;

}

#endif
