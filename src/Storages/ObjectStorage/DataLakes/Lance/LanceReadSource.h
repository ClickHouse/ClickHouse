#pragma once

#include "config.h"

#if USE_LANCE

#include <Formats/FormatSettings.h>
#include <Processors/ISource.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceScanDescription.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>

#include <memory>
#include <mutex>
#include <optional>

namespace DB
{
struct FormatSettings;
struct ObjectInfo;
using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
}

namespace DB::Lance
{

class ReadSource final : public ISource
{
public:
    ReadSource(
        const Block & header,
        ObjectInfoPtr object_info_,
        DatasetHandle dataset_,
        ScanDescription scan_,
        CancelHandlePtr cancel_handle_,
        FormatSettings format_settings_);

    String getName() const override { return "LanceReadSource"; }
    Chunk generate() override;

protected:
    void onCancel() noexcept override;

private:
    ObjectInfoPtr object_info;
    DatasetHandle dataset;
    ScanDescription scan;
    FormatSettings format_settings;
    bool is_finished = false;
    /// Rows already emitted; used as a C++-side cap when scan.limit is set.
    size_t rows_emitted = 0;
    /// Shared with plan/count/next so onCancel interrupts any in-flight Lance work.
    CancelHandlePtr cancel_handle;
    /// Protects optional engagement of scan_handle for concurrent onCancel vs generate.
    /// nextBatch/requestCancel on the Scan itself are safe without holding this mutex.
    std::mutex scan_mutex;
    std::optional<Scan> scan_handle;
    std::unique_ptr<ArrowColumnToCHColumn> converter;
};

}

#endif
