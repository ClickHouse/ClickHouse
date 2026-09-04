#pragma once

#include "config.h"

#if USE_LANCE

#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/ISource.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceScanDescription.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/StopToken.h>

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>

namespace arrow
{
class RecordBatch;
class Schema;
}

namespace DB::Lance
{

/// Per-read cancellation scope. Query cancellation propagates into this scope,
/// while stopping one read never cancels sibling reads in the same query.
class ReadCancellation final
{
public:
    explicit ReadCancellation(const ContextPtr & context);

    const CancelHandlePtr & handle() const { return cancel_handle; }
    void requestCancel() noexcept;

private:
    CancelHandlePtr cancel_handle;
    std::unique_ptr<StopCallback> query_cancel_callback;
};

using ReadCancellationPtr = std::shared_ptr<ReadCancellation>;

class BatchProvider
{
public:
    virtual ~BatchProvider() = default;

    virtual std::optional<Scan::Batch> nextBatch() = 0;
    virtual void releaseBatch(UInt64 bytes) noexcept = 0;
    virtual void requestCancel() noexcept = 0;
    virtual const std::shared_ptr<arrow::Schema> & schema() const = 0;
    virtual Scan::Stats stats() const noexcept = 0;
};

class ScanCoordinator final : public std::enable_shared_from_this<ScanCoordinator>
{
public:
    enum class State : UInt8
    {
        Running,
        Ended,
        Failed,
        Cancelled,
        Closed,
    };

    class Batch
    {
    public:
        Batch(const Batch &) = delete;
        Batch & operator=(const Batch &) = delete;
        Batch(Batch && other) noexcept;
        Batch & operator=(Batch && other) noexcept;
        ~Batch();

        const std::shared_ptr<arrow::RecordBatch> & recordBatch() const { return record_batch; }
        UInt64 rows() const { return batch_rows; }
        UInt64 bytes() const { return batch_bytes; }

    private:
        friend class ScanCoordinator;
        Batch(
            std::shared_ptr<ScanCoordinator> coordinator_, std::shared_ptr<arrow::RecordBatch> record_batch_, UInt64 rows_, UInt64 bytes_);
        void release() noexcept;

        std::shared_ptr<ScanCoordinator> coordinator;
        std::shared_ptr<arrow::RecordBatch> record_batch;
        UInt64 batch_rows = 0;
        UInt64 batch_bytes = 0;
    };

    static std::shared_ptr<ScanCoordinator>
    create(DatasetHandle dataset, ScanDescription scan_description, ReadCancellationPtr cancellation);
    static std::shared_ptr<ScanCoordinator> createWithProvider(
        std::unique_ptr<BatchProvider> provider,
        bool use_s3,
        std::optional<UInt64> row_limit = std::nullopt,
        ReadCancellationPtr cancellation = {});

    ~ScanCoordinator();

    std::optional<Batch> nextBatch();
    void cancel() noexcept;
    State state() const;
    const std::shared_ptr<arrow::Schema> & schema() const { return provider->schema(); }
    bool usesS3() const { return use_s3; }

private:
    ScanCoordinator(
        std::unique_ptr<BatchProvider> provider_, bool use_s3_, std::optional<UInt64> row_limit, ReadCancellationPtr cancellation_);

    UInt64 claimRows(UInt64 rows);
    void finishAtLimit() noexcept;
    void releaseBatch(UInt64 bytes) noexcept;
    void accountStatsOnce() noexcept;
    void refreshCurrentMetrics() noexcept;
    void clearCurrentMetrics() noexcept;

    ReadCancellationPtr cancellation;
    const bool use_s3;
    std::unique_ptr<BatchProvider> provider;
    const bool has_row_limit;
    std::atomic<UInt64> remaining_rows;
    mutable std::mutex state_mutex;
    State current_state = State::Running;
    std::exception_ptr first_exception;
    std::atomic_bool stats_accounted = false;
    std::mutex current_metrics_mutex;
    Scan::Stats reported_current_metrics;
};

struct ReadVirtualValues
{
    String path;
    StorageID storage_id = StorageID::createEmpty();
    std::optional<UInt64> snapshot_version;
};

class BatchSource final : public ISource
{
public:
    BatchSource(
        const Block & output_header,
        Block physical_header_,
        std::shared_ptr<ScanCoordinator> coordinator_,
        NamesAndTypesList requested_virtual_columns_,
        ReadVirtualValues virtual_values_,
        ContextPtr context_,
        FormatSettings format_settings_);

    String getName() const override { return "LanceBatchSource"; }
    Chunk generate() override;

protected:
    void onCancel() noexcept override;

private:
    Block physical_header;
    std::shared_ptr<ScanCoordinator> coordinator;
    NamesAndTypesList requested_virtual_columns;
    ReadVirtualValues virtual_values;
    ContextPtr context;
    FormatSettings format_settings;
    bool is_finished = false;
    bool reported_active = false;
    std::unique_ptr<ArrowColumnToCHColumn> converter;
};

class CountSource final : public ISource
{
public:
    class Provider
    {
    public:
        virtual ~Provider() = default;
        virtual std::optional<size_t> countRows() = 0;
        virtual void requestCancel() noexcept = 0;
    };

    CountSource(
        const Block & output_header_,
        DatasetHandle dataset_,
        ScanDescription scan_,
        ReadCancellationPtr cancellation_,
        NamesAndTypesList requested_virtual_columns_,
        ReadVirtualValues virtual_values_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_);

    CountSource(
        const Block & output_header_,
        std::unique_ptr<Provider> provider_,
        size_t max_block_size_,
        NamesAndTypesList requested_virtual_columns_ = {},
        ReadVirtualValues virtual_values_ = {},
        ContextPtr context_ = nullptr,
        std::optional<FormatSettings> format_settings_ = std::nullopt);

    String getName() const override { return "LanceCountSource"; }
    Chunk generate() override;

protected:
    void onCancel() noexcept override;

private:
    std::unique_ptr<Provider> provider;
    size_t max_block_size;
    Block physical_header;
    NamesAndTypesList requested_virtual_columns;
    ReadVirtualValues virtual_values;
    ContextPtr context;
    std::optional<FormatSettings> format_settings;
    std::optional<size_t> rows_remaining;
    bool is_finished = false;
};
}

#endif
