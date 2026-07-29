#include <gtest/gtest.h>

#include <algorithm>

#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

namespace
{

Poco::AutoPtr<Poco::Util::XMLConfiguration> configFromString(const std::string & xml)
{
    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    return new Poco::Util::XMLConfiguration(document);
}

}

/// `total_memory_profiler_sample_probability` is an alias of the server setting
/// `total_memory_tracker_sample_probability`. Loading the config via the alias must
/// populate the canonical setting.
TEST(ServerSettingsAlias, LoadViaAlias)
{
    auto config = configFromString(R"(<clickhouse>
    <total_memory_profiler_sample_probability>0.5</total_memory_profiler_sample_probability>
</clickhouse>)");

    ServerSettings settings;
    settings.loadSettingsFromConfig(*config);

    ASSERT_EQ(settings.get("total_memory_tracker_sample_probability").safeGet<Float64>(), 0.5);
}

/// The canonical name must keep working (regression guard for the alias-loading loop).
TEST(ServerSettingsAlias, LoadViaCanonicalName)
{
    auto config = configFromString(R"(<clickhouse>
    <total_memory_tracker_sample_probability>0.25</total_memory_tracker_sample_probability>
</clickhouse>)");

    ServerSettings settings;
    settings.loadSettingsFromConfig(*config);

    ASSERT_EQ(settings.get("total_memory_tracker_sample_probability").safeGet<Float64>(), 0.25);
}

/// Specifying both the canonical name and its alias in the config is ambiguous and must be rejected.
TEST(ServerSettingsAlias, RejectBothCanonicalAndAlias)
{
    auto config = configFromString(R"(<clickhouse>
    <total_memory_tracker_sample_probability>0.5</total_memory_tracker_sample_probability>
    <total_memory_profiler_sample_probability>0.5</total_memory_profiler_sample_probability>
</clickhouse>)");

    ServerSettings settings;
    ASSERT_THROW(settings.loadSettingsFromConfig(*config), Exception);
}

/// A deployment that keeps the shipped main config (which does not set the canonical name, see
/// `programs/server/config.xml` and `programs/server/config.yaml.example`) and adds the alias in an
/// override file must load the alias: the two config layers are merged before settings are loaded.
TEST(ServerSettingsAlias, LoadViaAliasFromOverrideLayer)
{
    auto main_config = configFromString(R"(<clickhouse>
    <total_memory_profiler_step>4194304</total_memory_profiler_step>
</clickhouse>)");
    auto override_config = configFromString(R"(<clickhouse>
    <total_memory_profiler_sample_probability>0.5</total_memory_profiler_sample_probability>
</clickhouse>)");

    Poco::AutoPtr<Poco::Util::LayeredConfiguration> layered = new Poco::Util::LayeredConfiguration;
    /// Overrides have higher priority, exactly as `config.d`/`*.yaml` overrides do in the server.
    layered->add(override_config, /* priority= */ 0);
    layered->add(main_config, /* priority= */ 1);

    ServerSettings settings;
    settings.loadSettingsFromConfig(*layered);

    ASSERT_EQ(settings.get("total_memory_tracker_sample_probability").safeGet<Float64>(), 0.5);
}

/// If the main config still sets the canonical name, adding the alias in an override is ambiguous
/// and must be rejected rather than silently picking one of the two values.
TEST(ServerSettingsAlias, RejectAliasInOverrideLayerAndCanonicalInMainConfig)
{
    auto main_config = configFromString(R"(<clickhouse>
    <total_memory_tracker_sample_probability>0</total_memory_tracker_sample_probability>
</clickhouse>)");
    auto override_config = configFromString(R"(<clickhouse>
    <total_memory_profiler_sample_probability>0.5</total_memory_profiler_sample_probability>
</clickhouse>)");

    Poco::AutoPtr<Poco::Util::LayeredConfiguration> layered = new Poco::Util::LayeredConfiguration;
    layered->add(override_config, /* priority= */ 0);
    layered->add(main_config, /* priority= */ 1);

    ServerSettings settings;
    ASSERT_THROW(settings.loadSettingsFromConfig(*layered), Exception);
}

/// The alias must be enumerable and resolvable, so that user-facing surfaces (e.g. `system.documentation`)
/// can render it as an alias of its canonical setting, consistently with `Settings` and `MergeTreeSettings`.
TEST(ServerSettingsAlias, EnumeratedAndResolvable)
{
    ServerSettings settings;

    const auto aliases = settings.getAllAliasNames();
    ASSERT_NE(
        std::find(aliases.begin(), aliases.end(), std::string_view{"total_memory_profiler_sample_probability"}),
        aliases.end());

    ASSERT_EQ(ServerSettings::resolveName("total_memory_profiler_sample_probability"), "total_memory_tracker_sample_probability");
    /// A canonical (non-alias) name resolves to itself.
    ASSERT_EQ(ServerSettings::resolveName("total_memory_tracker_sample_probability"), "total_memory_tracker_sample_probability");
}
