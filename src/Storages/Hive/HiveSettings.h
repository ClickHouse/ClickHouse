#pragma once

#include <Common/config.h>

#if USE_HIVE

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/BaseSettings.h>
#include <Core/Settings.h>

namespace DB
{
class ASTStorage;

#define HIVE_RELATED_SETTINGS(M) \
    M(Char, hive_text_field_delimeter, '\x01', "How to split one row of hive data with format text", 0) \
    M(Bool, enable_orc_stripe_minmax_index, false, "Enable using ORC stripe level minmax index.", 0) \
    M(Bool, enable_parquet_rowgroup_minmax_index, false, "Enable using Parquet row-group level minmax index.", 0) \
    M(Bool, enable_orc_file_minmax_index, true, "Enable using ORC file level minmax index.", 0)

#define LIST_OF_HIVE_SETTINGS(M) \
    HIVE_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(HiveSettingsTraits, LIST_OF_HIVE_SETTINGS)


/** Settings for the Hive engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
class HiveSettings : public BaseSettings<HiveSettingsTraits>
{
public:
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    void loadFromQuery(ASTStorage & storage_def);
};
}

#endif
