-- Tags: no-fasttest, no-msan
-- ^ no-fasttest: azure/s3/hdfs table functions are gated on build flags
-- ^ no-msan: `delta-kernel-rs` is disabled under MSan, so `DeltaLakeAzure` is unavailable

-- Regression test for STID 4283-5f31:
-- BuzzHouse hit `Logical error: Expected 3 to 10 arguments in table function azureBlobStorage, got 1`
-- via `CREATE TABLE ... ENGINE = DeltaLakeAzure(<single-arg>)`. The defensive arg-count check
-- inside `addStructureAndFormatToArgsIfNeededAzure` (and its S3/HDFS/file-like siblings) used
-- `LOGICAL_ERROR`, which `abortOnFailedAssertion` aborts in debug builds and is treated as a
-- server bug.
--
-- The same anti-pattern existed in 4 places:
--   src/Storages/ObjectStorage/Azure/Configuration.cpp  - addStructureAndFormatToArgsIfNeededAzure
--   src/Storages/ObjectStorage/HDFS/Configuration.cpp   - addStructureAndFormatToArgsIfNeededHDFS
--   src/Storages/ObjectStorage/S3/Configuration.cpp     - addStructureAndFormatToArgsIfNeededS3
--   src/TableFunctions/ITableFunctionFileLike.h         - updateStructureAndFormatArgumentsIfNeeded
--
-- All four now throw `NUMBER_OF_ARGUMENTS_DOESNT_MATCH` instead, matching the upstream
-- `*StorageParsedArguments::fromAST` validation and giving users a graceful, well-typed error.

-- ---- Table function paths ----

-- file table function: 0 args / too many args goes through ITableFunctionFileLike
SELECT * FROM file(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM file('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- url table function uses the same ITableFunctionFileLike base
SELECT * FROM url(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- s3 table function: 0 args / too many args (>10) goes through S3StorageParsedArguments::fromAST
SELECT * FROM s3(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM s3('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- hdfs table function: 0 args / too many args (>4) goes through HDFSStorageParsedArguments::fromAST
SELECT * FROM hdfs(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM hdfs('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- azureBlobStorage table function: 0 args / too many args (>10)
SELECT * FROM azureBlobStorage(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM azureBlobStorage('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- ---- Engine paths (CREATE TABLE) ----
--
-- These exercise the CREATE TABLE codepath through
-- `InterpreterCreateQuery::doCreateTable` -> `StorageFactory::get` ->
-- `StorageObjectStorage::addInferredEngineArgsToCreateQuery` ->
-- `addStructureAndFormatToArgsIfNeeded*`.
--
-- For zero / too-many args the upstream `*StorageParsedArguments::fromAST` validation
-- catches it first. For the BuzzHouse trigger (`DeltaLakeAzure(<single-arg>)`),
-- `AzureStorageParsedArguments::fromAST` accepts a single argument as the
-- "lightweight loading" case, the DataLake configuration forces `format = "Parquet"`
-- so format detection is skipped, and execution reaches
-- `addStructureAndFormatToArgsIfNeededAzure`. With the fix, all paths report
-- `NUMBER_OF_ARGUMENTS_DOESNT_MATCH` instead of `LOGICAL_ERROR`.

CREATE TABLE t_engine_s3_zero (x UInt32) ENGINE = S3(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_s3_many (x UInt32) ENGINE = S3('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

CREATE TABLE t_engine_hdfs_zero (x UInt32) ENGINE = HDFS(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_hdfs_many (x UInt32) ENGINE = HDFS('a', 'b', 'c', 'd', 'e', 'f'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

CREATE TABLE t_engine_azure_zero (x UInt32) ENGINE = AzureBlobStorage(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_azure_many (x UInt32) ENGINE = AzureBlobStorage('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- `DeltaLakeAzure` engine path: explicit coverage for the engine that surfaced the
-- original BuzzHouse failure. `AzureStorageParsedArguments::fromAST` rejects the
-- empty arg list, exercising the CREATE TABLE codepath end-to-end. The 1-arg
-- BuzzHouse trigger that reaches `addStructureAndFormatToArgsIfNeededAzure` itself
-- requires a real Azure backend (the storage construction proceeds further), so it
-- is documented in the comment above and exercised post-merge by the BuzzHouse
-- fuzzer rather than directly here.
CREATE TABLE t_engine_delta_azure_zero (x UInt32) ENGINE = DeltaLakeAzure(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_delta_azure_many (x UInt32) ENGINE = DeltaLakeAzure('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 'ok';
