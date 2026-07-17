#pragma once

#include <Storages/BigQuery/BigQueryClient.h>
#include <Storages/BigQuery/BigQueryConfiguration.h>
#include <Storages/BigQuery/BigQuerySchema.h>
#include <Storages/IStorage.h>
#include <Common/logger_useful.h>

#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

/// Reads from and writes to a Google BigQuery table over the BigQuery v2 REST API.
/// Reading uses `tabledata.list` (works for native tables, not for views),
/// writing uses `tabledata.insertAll` (streaming inserts).
class StorageBigQuery final : public IStorage
{
public:
    StorageBigQuery(
        const StorageID & table_id_,
        BigQueryConfiguration configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        /// The `bigquery` table function fetches the schema during analysis; it passes the resulting
        /// snapshot (and the token provider used to fetch it) so that execution reuses the same schema
        /// and access token instead of issuing a second `tables.get` and minting a second token.
        std::shared_ptr<BigQueryTokenProvider> token_provider_ = nullptr,
        std::optional<BigQueryFields> prefetched_fields_ = std::nullopt);

    std::string getName() const override { return "BigQuery"; }
    bool isRemote() const override { return true; }
    /// A write-capable external database, like `MySQL` / `PostgreSQL`. This exempts `INSERT`s from the
    /// server-wide `disable_insertion_and_mutation` setting, since they do not create merge tasks on the
    /// local replica.
    bool isExternalDatabase() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    static BigQueryConfiguration getConfiguration(ASTs & engine_args, ContextPtr context, const StorageID * table_id = nullptr);

    /// Fetches the table schema from BigQuery (`tables.get`). An optional token provider lets the
    /// caller reuse a cached access token across requests.
    static BigQueryFields fetchTableSchema(
        const BigQueryConfiguration & configuration,
        ContextPtr context,
        const std::shared_ptr<BigQueryTokenProvider> & token_provider = nullptr);

private:
    /// The BigQuery schema is fetched lazily on the first read or write (not at server startup),
    /// and the declared columns are validated against it.
    const BigQueryFields & getFields(ContextPtr query_context) const;

    BigQueryConfiguration configuration;
    /// Shared across queries so that an OAuth access token is minted once and reused until it expires,
    /// instead of on every read and write (which each construct their own `BigQueryClient`).
    std::shared_ptr<BigQueryTokenProvider> token_provider;

    mutable std::mutex fields_mutex;
    mutable std::optional<BigQueryFields> fields;

    LoggerPtr log;
};

}
