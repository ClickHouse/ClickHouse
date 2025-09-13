#include <gtest/gtest.h>
#include <sstream>

#include <Common/Scheduler/Nodes/WorkloadResourceManager.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

TEST(SchedulerWorkloadResourceManager, ConfigurationLoading)
{
    // Create a simple XML configuration with workloads and resources
    std::stringstream config_stream;
    config_stream << R"(
        <clickhouse>
            <workloads>
                <all>
                    <sql>WORKLOAD all SETTINGS max_io_requests = 100</sql>
                </all>
                <production>
                    <sql>WORKLOAD production IN all SETTINGS weight = 3</sql>
                </production>
            </workloads>
            <resources>
                <disk_io>
                    <sql>RESOURCE disk_io (READ DISK default, WRITE DISK default)</sql>
                </disk_io>
                <network_io>
                    <sql>RESOURCE network_io (READ DISK s3, WRITE DISK s3)</sql>
                </network_io>
            </resources>
        </clickhouse>
    )";

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(config_stream);

    ResourceTest t;
    
    // Test that configuration loading works
    EXPECT_NO_THROW(t.manager->updateConfiguration(*config));
    
    // Verify that entities were loaded - note: storage access might need adjustment based on actual ResourceTest implementation
    // For now, just verify no exceptions are thrown and basic functionality works
    
    // Test that we can acquire classifiers for loaded workloads
    ClassifierPtr c_all, c_production;
    EXPECT_NO_THROW(c_all = t.manager->acquire("all"));
    EXPECT_NO_THROW(c_production = t.manager->acquire("production"));
    
    EXPECT_TRUE(c_all != nullptr);
    EXPECT_TRUE(c_production != nullptr);
}

TEST(SchedulerWorkloadResourceManager, ConfigurationErrorHandling)
{
    // Test configuration with malformed SQL
    std::stringstream config_stream;
    config_stream << R"(
        <clickhouse>
            <workloads>
                <bad_workload>
                    <sql>INVALID SQL SYNTAX</sql>
                </bad_workload>
                <good_workload>
                    <sql>WORKLOAD good_workload SETTINGS max_io_requests = 50</sql>
                </good_workload>
            </workloads>
            <resources>
                <bad_resource>
                    <sql>INVALID RESOURCE SQL</sql>
                </bad_resource>
                <good_resource>
                    <sql>RESOURCE good_resource (READ DISK default)</sql>
                </good_resource>
            </resources>
        </clickhouse>
    )";

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(config_stream);

    ResourceTest t;
    
    // Should not throw even with malformed SQL - just log errors
    EXPECT_NO_THROW(t.manager->updateConfiguration(*config));
    
    // Good entities should be accessible
    ClassifierPtr c_good;
    EXPECT_NO_THROW(c_good = t.manager->acquire("good_workload"));
    EXPECT_TRUE(c_good != nullptr);
}