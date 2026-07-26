#pragma once

#include "config.h"

#if USE_LANCE

#include <Formats/FormatSettings.h>
#include <Processors/ISource.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceScanDescription.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>

#include <memory>
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
        FormatSettings format_settings_);

    String getName() const override { return "LanceReadSource"; }
    Chunk generate() override;

private:
    ObjectInfoPtr object_info;
    DatasetHandle dataset;
    ScanDescription scan;
    FormatSettings format_settings;
    bool is_finished = false;
    std::optional<Scan> scan_handle;
    std::unique_ptr<ArrowColumnToCHColumn> converter;
};

}

#endif
