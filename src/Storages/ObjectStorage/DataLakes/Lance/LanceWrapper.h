#pragma once

#include "config.h"

#if USE_LANCE

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

#include <memory>

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

namespace DB::Lance
{

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
};

struct SnapshotInfo
{
    UInt64 snapshot_id = 0;
    UInt64 schema_id = 0;
};

struct ScanDescription;
struct TableStateSnapshot;

class Scan
{
public:
    Scan(const Scan &) = delete;
    Scan & operator=(const Scan &) = delete;
    Scan(Scan && other) noexcept;
    Scan & operator=(Scan && other) noexcept;
    ~Scan();

    std::shared_ptr<arrow::RecordBatch> nextBatch() const;

private:
    explicit Scan(ch_lance_scan * scan_) : scan(scan_) {}
    friend class Dataset;

    ch_lance_scan * scan = nullptr;
};

class Dataset
{
public:
    Dataset(const Dataset &) = delete;
    Dataset & operator=(const Dataset &) = delete;
    Dataset(Dataset && other) noexcept;
    Dataset & operator=(Dataset && other) noexcept;
    ~Dataset();

    static Dataset open(const DatasetOptions & options);

    SnapshotInfo currentSnapshot() const;
    NamesAndTypesList tableSchema(const TableStateSnapshot & snapshot, ContextPtr context) const;
    std::optional<size_t> totalRows(const TableStateSnapshot & snapshot) const;
    std::optional<size_t> countRows(const TableStateSnapshot & snapshot, const std::optional<String> & predicate) const;
    std::optional<size_t> totalBytes() const;
    Scan planScan(const ScanDescription & scan_description) const;

private:
    explicit Dataset(ch_lance_dataset * dataset_) : dataset(dataset_) {}

    ch_lance_dataset * dataset = nullptr;
};

}

#endif
