#include <gtest/gtest.h>

#include <Core/Field.h>
#include <Core/ServerSettings.h>
#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
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
