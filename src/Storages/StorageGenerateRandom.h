#pragma once

#include <Storages/GenerateRandomSettings.h>
#include <optional>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <base/types.h>
#include <pcg_random.hpp>


namespace DB
{

/// Everything that tunes the random generator. `max_array_length` and `max_string_length` come from
/// the engine or table function arguments, the rest from `SETTINGS`.
struct GenerateRandomOptions
{
    UInt64 max_array_length = 10;
    UInt64 max_string_length = 10;
    /// `SettingFieldFloat` is `float`.
    Float32 null_ratio = 0.0625;
    UInt64 max_json_depth = 3;
    UInt64 max_json_keys_per_object = 8;
    /// If `fuzzy` is true, tries to generate more "interesting" values. E.g. small numbers are more
    /// likely, and strings sometimes are in datetime format.
    bool fuzzy = false;

    /// `null_ratio` expressed as a threshold out of 65536, the resolution of a null decision.
    UInt32 nullThreshold() const;
};

ColumnPtr fillColumnWithRandomData(
    DataTypePtr type, UInt64 limit, UInt64 max_array_length, UInt64 max_string_length, pcg64 & rng, bool fuzzy = false);

/* Generates random data for given schema.
 */
class StorageGenerateRandom final : public StorageWithCommonVirtualColumns
{
public:
    StorageGenerateRandom(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        const GenerateRandomOptions & options_,
        const std::optional<UInt64> & random_seed,
        const GenerateRandomSettings & settings_ = {});

    std::string getName() const override { return "GenerateRandom"; }

    static VirtualColumnsDescription createVirtuals();

    using StorageWithCommonVirtualColumns::read;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool supportsTransactions() const override { return true; }
    bool supportsTruncate() const override { return false; }

    /// `JSON`, `Dynamic` and every type containing them are generated as well.
    bool supportsColumnsWithDynamicStructure() const override { return true; }

    SettingDescriptions getTableSettings(ContextPtr query_context) const override;

private:
    GenerateRandomOptions options;
    UInt64 random_seed = 0;
    /// The settings the table was created with. `options` holds what the engine reads from them; this is
    /// what it reports, so a setting states its own value rather than the one it was folded into.
    GenerateRandomSettings settings;
};

}
