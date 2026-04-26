-- Tags: no-fasttest
-- ^ no-fasttest because azure/s3/hdfs table functions are gated on build flags

-- Regression test for STID 4283-5f31:
-- BuzzHouse hit `Logical error: Expected 3 to 10 arguments in table function azureBlobStorage, got 1`
-- via `CREATE TABLE ... ENGINE = DeltaLakeAzure(<single-arg>)`. The defensive arg-count check
-- inside `addStructureAndFormatToArgsIfNeededAzure` (and its S3/HDFS/file-like siblings) used
-- `LOGICAL_ERROR`, which `abortOnFailedAssertion`s in debug builds and is treated as a server bug.
--
-- The same anti-pattern existed in 4 places:
--   src/Storages/ObjectStorage/Azure/Configuration.cpp  - addStructureAndFormatToArgsIfNeededAzure
--   src/Storages/ObjectStorage/HDFS/Configuration.cpp   - addStructureAndFormatToArgsIfNeededHDFS
--   src/Storages/ObjectStorage/S3/Configuration.cpp     - addStructureAndFormatToArgsIfNeededS3
--   src/TableFunctions/ITableFunctionFileLike.h         - updateStructureAndFormatArgumentsIfNeeded
--
-- All four now throw `NUMBER_OF_ARGUMENTS_DOESNT_MATCH` instead, matching the upstream
-- `*StorageParsedArguments::fromAST` validation and giving users a graceful, well-typed error.

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

SELECT 'ok';
