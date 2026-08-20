-- Tags: no-fasttest, no-msan
-- ^ no-fasttest: azure/s3/hdfs table functions are gated on build flags
-- ^ no-msan: `delta-kernel-rs` is disabled under MSan, so `DeltaLakeAzure` is unavailable

-- Regression test for STID 4283-5f31 (fixed in PR #103544): a bad argument count
-- for the object-storage table functions and engines (file/url/s3/hdfs/azureBlobStorage
-- and the DeltaLake* engines) must raise NUMBER_OF_ARGUMENTS_DOESNT_MATCH, not a
-- LOGICAL_ERROR that aborts debug builds.
--
-- NOTE: every query below throws on purpose, so the server logs its full query text
-- (this comment included) at Error level. Keep this file free of the verbatim crash
-- message that ci/jobs/scripts/log_parser.py greps the server log for, otherwise the
-- scanner false-matches this comment and reports a bogus failure. Refer to the error
-- by its code name LOGICAL_ERROR; never spell out the runtime message text.

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
