#pragma once

#include "config.h"

#if USE_LANCE

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>

#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

namespace DB
{
struct FormatSettings;
}

namespace arrow
{
class RecordBatch;
}

struct ch_lance_dataset;
struct ch_lance_scan;
struct ch_lance_cancel_handle;

namespace DB::Lance
{

namespace ErrorMapping
{
int toClickHouseErrorCode(UInt32 kind, UInt32 origin);
}

struct DatasetOptions
{
    String uri{};
    bool use_s3 = false;
    String s3_region{};
    String s3_endpoint{};
    String s3_access_key_id{};
    String s3_secret_access_key{};
    String s3_session_token{};
    String s3_role_arn{};
    String s3_role_session_name{};
    bool s3_use_environment_credentials = false;
    bool s3_no_sign_request = false;
    bool s3_allow_http = false;
    bool s3_virtual_hosted_style_request = false;
    /// S3 HTTP request timeout (object_store). 0 = library default.
    UInt64 s3_request_timeout_ms = 0;
    /// S3 connect timeout (object_store). 0 = library default.
    UInt64 s3_connect_timeout_ms = 0;

    /// Stable cache key for query-scoped dataset reuse (includes credentials fingerprint).
    /// Timeouts are intentionally excluded: they do not change dataset identity.
    String identityKey() const;
};

struct FragmentInfo
{
    UInt64 id = 0;
    /// nullopt if Lance did not report a row count for this fragment.
    std::optional<UInt64> num_rows;
    /// 0 if unknown; best-effort sum of data file sizes.
    UInt64 size_bytes = 0;
};

struct ScanDescription;
class Scan;

/// Query-scoped cooperative cancel token. Shared across open/plan/count/scan for one unit of work.
class CancelHandle
{
public:
    CancelHandle();
    ~CancelHandle();

    CancelHandle(const CancelHandle &) = delete;
    CancelHandle & operator=(const CancelHandle &) = delete;
    CancelHandle(CancelHandle && other) noexcept;
    CancelHandle & operator=(CancelHandle && other) noexcept;

    void requestCancel() noexcept;
    ch_lance_cancel_handle * raw() const { return handle; }

private:
    ch_lance_cancel_handle * handle = nullptr;
};

using CancelHandlePtr = std::shared_ptr<CancelHandle>;

/// Shared, copyable handle around a process-runtime-backed Lance dataset.
class DatasetHandle
{
public:
    DatasetHandle() = default;

    static DatasetHandle open(const DatasetOptions & options, const CancelHandlePtr & cancel = {});

    /// Ephemeral open that does not participate in query-session reuse.
    /// Used only when there is no query context (CREATE validation, unit tests).
    static DatasetHandle openEphemeral(const DatasetOptions & options, const CancelHandlePtr & cancel = {});

    explicit operator bool() const { return static_cast<bool>(impl); }

    const DatasetOptions & options() const;
    String identityKey() const;

    TableStateSnapshot currentSnapshot() const;
    NamesAndTypesList tableSchema(
        const TableStateSnapshot & snapshot,
        ContextPtr context,
        const CancelHandlePtr & cancel = {},
        std::unordered_set<String> * utf8_columns = nullptr) const;
    std::optional<size_t> totalRows(const TableStateSnapshot & snapshot, const CancelHandlePtr & cancel = {}) const;
    std::optional<size_t> countRows(
        const TableStateSnapshot & snapshot,
        const std::optional<String> & predicate,
        const std::vector<UInt64> & fragment_ids = {},
        const CancelHandlePtr & cancel = {}) const;
    std::optional<size_t> totalBytes() const;
    /// Lists fragments for the pinned snapshot version (exact checkout).
    std::vector<FragmentInfo> listFragments(const TableStateSnapshot & snapshot, const CancelHandlePtr & cancel = {}) const;
    Scan planScan(const ScanDescription & scan_description, const CancelHandlePtr & cancel = {}) const;

    ch_lance_dataset * raw() const;

private:
    struct Impl
    {
        explicit Impl(ch_lance_dataset * dataset_, DatasetOptions options_);
        ~Impl();

        Impl(const Impl &) = delete;
        Impl & operator=(const Impl &) = delete;

        ch_lance_dataset * dataset = nullptr;
        DatasetOptions options;
    };

    explicit DatasetHandle(std::shared_ptr<Impl> impl_) : impl(std::move(impl_)) {}

    std::shared_ptr<Impl> impl;
};

class Scan
{
public:
    Scan(const Scan &) = delete;
    Scan & operator=(const Scan &) = delete;
    Scan(Scan && other) noexcept;
    Scan & operator=(Scan && other) noexcept;
    ~Scan();

    /// Thread-safe cooperative cancel. Wakes a pending nextBatch; does not free the scan.
    void requestCancel() noexcept;

    std::shared_ptr<arrow::RecordBatch> nextBatch() const;

private:
    /// Holds the dataset alive for the stream lifetime (scan must outlive free of dataset).
    Scan(DatasetHandle dataset_, ch_lance_scan * scan_);
    friend class DatasetHandle;

    DatasetHandle dataset;
    ch_lance_scan * scan = nullptr;
};

void ensureRuntime(UInt32 worker_threads = 0);

struct RuntimeStats
{
    UInt64 open_dataset_calls = 0;
    UInt64 plan_scan_calls = 0;
    UInt64 next_batch_calls = 0;
    UInt64 runtime_initialized = 0;
};

RuntimeStats runtimeStats();

}

#endif
