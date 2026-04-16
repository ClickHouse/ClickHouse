#include "config.h"

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/System/StorageSystemJemallocProfileText.h>

#if USE_JEMALLOC
#    include <Core/Settings.h>
#    include <Interpreters/Context.h>
#    include <Processors/Sources/JemallocProfileSource.h>
#    include <Common/Jemalloc.h>
#endif

namespace DB
{

#if USE_JEMALLOC
namespace Setting
{
    extern const SettingsJemallocProfileFormat jemalloc_profile_text_output_format;
    extern const SettingsBool jemalloc_profile_text_symbolize_with_inline;
    extern const SettingsBool jemalloc_profile_text_collapsed_use_count;
}
#endif

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

StorageSystemJemallocProfileText::StorageSystemJemallocProfileText(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription());
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemJemallocProfileText::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

ColumnsDescription StorageSystemJemallocProfileText::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"line", std::make_shared<DataTypeString>(), "Line from the symbolized jemalloc heap profile."},
    };
}

Pipe StorageSystemJemallocProfileText::read(
    [[maybe_unused]] const Names & column_names,
    [[maybe_unused]] const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    [[maybe_unused]] ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    [[maybe_unused]] const size_t max_block_size,
    const size_t /*num_streams*/)
{
#if USE_JEMALLOC
    storage_snapshot->check(column_names);

    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);

    /// Get the last flushed profile filename
    auto last_profile = std::string(Jemalloc::flushProfile("/tmp/jemalloc_clickhouse"));

    /// Get the output format from settings
    auto format = context->getSettingsRef()[Setting::jemalloc_profile_text_output_format];
    auto symbolize_with_inline = context->getSettingsRef()[Setting::jemalloc_profile_text_symbolize_with_inline];
    auto collapsed_use_count = context->getSettingsRef()[Setting::jemalloc_profile_text_collapsed_use_count];

    /// Create source that reads and processes the profile according to the format
    auto source = std::make_shared<JemallocProfileSource>(
        last_profile,
        std::make_shared<const Block>(std::move(header)),
        max_block_size,
        format,
        symbolize_with_inline,
        collapsed_use_count);

    return Pipe(std::move(source));
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "jemalloc is not enabled");
#endif
}

}
