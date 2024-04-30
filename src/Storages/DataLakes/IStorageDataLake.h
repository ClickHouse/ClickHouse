#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Common/logger_useful.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFactory.h>
#include "PartitionColumns.h"
#include <filesystem>


namespace DB
{

template <typename Storage, typename Name, typename MetadataParser>
class IStorageDataLake : public Storage
{
public:
    static constexpr auto name = Name::name;
    using Configuration = typename Storage::Configuration;

    template <class ...Args>
    static StoragePtr create(
        const Configuration & configuration_,
        ContextPtr context_,
        LoadingStrictnessLevel mode,
        const ColumnsDescription & columns_,
        Args && ...args)
    {
        std::unique_ptr<MetadataParser> metadata;
        Configuration read_configuration;
        Configuration base_configuration{configuration_};
        try
        {
            base_configuration.update(context_);
            metadata = std::make_unique<MetadataParser>(base_configuration, context_);
            read_configuration = getConfigurationForDataRead(*metadata, base_configuration, context_);
        }
        catch (...)
        {
            if (mode <= LoadingStrictnessLevel::CREATE)
                throw;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        return std::make_shared<IStorageDataLake<Storage, Name, MetadataParser>>(
            configuration_,
            read_configuration,
            context_,
            columns_.empty() && metadata ? ColumnsDescription(metadata->getTableSchema()) : columns_,
            std::forward<Args>(args)...);
    }

    template <class ...Args>
    explicit IStorageDataLake(
        const Configuration & base_configuration_,
        const Configuration & read_configuration_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        Args && ...args)
        : Storage(read_configuration_,
                  context_,
                  columns_,
                  std::forward<Args>(args)...)
        , base_configuration(base_configuration_)
        , log(getLogger(getName())) // NOLINT(clang-analyzer-optin.cplusplus.VirtualCall)
    {
    }

    String getName() const override { return name; }

    static ColumnsDescription getTableStructureFromData(
        Configuration & base_configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & local_context)
    {
        base_configuration.update(local_context);
        auto metadata = std::make_unique<MetadataParser>(base_configuration, local_context);
        auto schema = metadata->getTableSchema();
        if (!schema.empty())
        {
            return ColumnsDescription(schema);
        }
        else
        {
            auto read_configuration = getConfigurationForDataRead(*metadata, base_configuration, local_context);
            return Storage::getTableStructureFromData(read_configuration, format_settings, local_context);
        }
    }

    static Configuration getConfiguration(ASTs & engine_args, const ContextPtr & local_context)
    {
        return Storage::getConfiguration(engine_args, local_context, /* get_format_from_file */false);
    }

    Configuration updateConfigurationAndGetCopy(const ContextPtr & local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(local_context);
        return Storage::getConfiguration();
    }

    void updateConfiguration(const ContextPtr & local_context) override
    {
        std::lock_guard lock(configuration_update_mutex);
        updateConfigurationImpl(local_context);
    }

private:
    static Configuration getConfigurationForDataRead(
        MetadataParser & metadata_,
        const Configuration & base_configuration,
        const ContextPtr & local_context)
    {
        auto configuration{base_configuration};
        configuration.update(local_context);
        configuration.static_configuration = true;
        configuration.keys = metadata_.getFiles();
        configuration.connect(local_context);
        return configuration;
    }

    void updateConfigurationImpl(const ContextPtr & local_context)
    {
        const bool updated = base_configuration.update(local_context);

        auto metadata = MetadataParser(base_configuration, local_context);
        auto new_keys = metadata.getFiles();
        Storage::partition_columns = metadata.getPartitionColumns();

        if (!updated && new_keys == Storage::getConfiguration().keys)
            return;

        auto read_configuration = getConfigurationForDataRead(metadata, base_configuration, local_context);
        Storage::useConfiguration(read_configuration);
    }

    Configuration base_configuration;
    std::mutex configuration_update_mutex;
    LoggerPtr log;
};


template <typename DataLake>
static StoragePtr createDataLakeStorage(const StorageFactory::Arguments & args)
{
    auto configuration = DataLake::getConfiguration(args.engine_args, args.getLocalContext());

    /// Data lakes use parquet format, no need for schema inference.
    if (configuration.format == "auto")
        configuration.format = "Parquet";

    return DataLake::create(configuration, args.getContext(), args.mode,
                            args.columns, args.table_id, args.constraints,
                            args.comment, getFormatSettings(args.getContext()));
}

}

#endif
