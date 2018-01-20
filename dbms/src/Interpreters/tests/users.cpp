#include <Common/ConfigProcessor/ConfigProcessor.h>
#include <Interpreters/SecurityManager.h>

#include <boost/filesystem.hpp>

#include <vector>
#include <string>
#include <tuple>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
#include <cstdlib>

namespace
{

namespace fs = boost::filesystem;

struct TestEntry
{
    std::string user_name;
    std::string database_name;
    bool is_allowed;
};

using TestEntries = std::vector<TestEntry>;

struct TestDescriptor
{
    const char * config_content;
    TestEntries entries;
};

using TestSet = std::vector<TestDescriptor>;

/// Tests description.

TestSet test_set =
{
    {
        "<?xml version=\"1.0\"?><yandex>"
        "    <profiles><default></default></profiles>"
        "    <users>"
        "        <default>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "            <allow_databases>"
        "              <database>default</database>"
        "              <database>test</database>"
        "            </allow_databases>"
        "        </default>"
        "        <web>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "        </web>"
        "    </users>"
        "    <quotas><default></default></quotas>"
        "</yandex>",

        {
            { "default", "default", true },
            { "default", "test", true },
            { "default", "stats", false },
            { "web", "default", true },
            { "web", "test", true },
            { "web", "stats", true },
            { "analytics", "default", false },
            { "analytics", "test", false },
            { "analytics", "stats", false }
        }
    },

    {
        "<?xml version=\"1.0\"?><yandex>"
        "    <profiles><default></default></profiles>"
        "    <users>"
        "        <default>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "            <allow_databases>"
        "              <database>default</database>"
        "            </allow_databases>"
        "        </default>"
        "        <web>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "        </web>"
        "    </users>"
        "    <quotas><default></default></quotas>"
        "</yandex>",

        {
            { "default", "default", true },
            { "default", "test", false },
            { "default", "stats", false },
            { "web", "default", true },
            { "web", "test", true },
            { "web", "stats", true },
            { "analytics", "default", false },
            { "analytics", "test", false },
            { "analytics", "stats", false }
        }
    },

    {
        "<?xml version=\"1.0\"?><yandex>"
        "    <profiles><default></default></profiles>"
        "    <users>"
        "        <default>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "            <allow_databases>"
        "            </allow_databases>"
        "        </default>"
        "        <web>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "        </web>"
        "    </users>"
        "    <quotas><default></default></quotas>"
        "</yandex>",

        {
            { "default", "default", true },
            { "default", "test", true },
            { "default", "stats", true },
            { "web", "default", true },
            { "web", "test", true },
            { "web", "stats", true },
            { "analytics", "default", false },
            { "analytics", "test", false },
            { "analytics", "stats", false }
        }
    },

    {
        "<?xml version=\"1.0\"?><yandex>"
        "    <profiles><default></default></profiles>"
        "    <users>"
        "        <default>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "            <allow_databases>"
        "              <database>default</database>"
        "            </allow_databases>"
        "        </default>"
        "        <web>"
        "            <password></password><profile>default</profile><quota>default</quota>"
        "            <allow_databases>"
        "              <database>test</database>"
        "            </allow_databases>"
        "        </web>"
        "    </users>"
        "    <quotas><default></default></quotas>"
        "</yandex>",

        {
            { "default", "default", true },
            { "default", "test", false },
            { "default", "stats", false },
            { "web", "default", false },
            { "web", "test", true },
            { "web", "stats", false },
            { "analytics", "default", false },
            { "analytics", "test", false },
            { "analytics", "stats", false }
        }
    }
};

std::string createTmpPath(const std::string & filename);
void createFile(const std::string & filename, const char * data);
void runOneTest(const TestDescriptor & test_descriptor);
auto runTestSet(const TestSet & test_set);

std::string createTmpPath(const std::string & filename)
{
    char pattern[] = "/tmp/fileXXXXXX";
    char * dir = mkdtemp(pattern);
    if (dir == nullptr)
        throw std::runtime_error("Could not create directory");

    return std::string(dir) + "/" + filename;
}

void createFile(const std::string & filename, const char * data)
{
    std::ofstream ofs(filename.c_str());
    if (!ofs.is_open())
        throw std::runtime_error("Could not open file " + filename);
    ofs << data;
}

void runOneTest(const TestDescriptor & test_descriptor)
{
    const auto path_name = createTmpPath("users.xml");
    createFile(path_name, test_descriptor.config_content);

    ConfigurationPtr config;

    try
    {
        config = ConfigProcessor(path_name).loadConfig().configuration;
    }
    catch (const Poco::Exception & ex)
    {
        std::ostringstream os;
        os << "Error: " << ex.what() << ": " << ex.displayText();
        throw std::runtime_error(os.str());
    }

    DB::SecurityManager security_manager;

    try
    {
        security_manager.loadFromConfig(*config);
    }
    catch (const Poco::Exception & ex)
    {
        std::ostringstream os;
        os << "Error: " << ex.what() << ": " << ex.displayText();
        throw std::runtime_error(os.str());
    }

    for (const auto & entry : test_descriptor.entries)
    {
        bool res;

        try
        {
            res = security_manager.hasAccessToDatabase(entry.user_name, entry.database_name);
        }
        catch (const Poco::Exception &)
        {
            res = false;
        }

        if (res != entry.is_allowed)
        {
            auto to_string = [](bool access){ return (access ? "'granted'" : "'denied'"); };
            std::ostringstream os;
            os << "(user=" << entry.user_name << ", database=" << entry.database_name << "): ";
            os << "Expected " << to_string(entry.is_allowed) << " but got " << to_string(res);
            throw std::runtime_error(os.str());
        }
    }

    fs::remove_all(fs::path(path_name).parent_path().string());
}

auto runTestSet(const TestSet & test_set)
{
    size_t test_num = 1;
    size_t failure_count = 0;

    for (const auto & test_descriptor : test_set)
    {
        try
        {
            runOneTest(test_descriptor);
            std::cout << "Test " << test_num << " passed\n";
        }
        catch (const std::runtime_error & ex)
        {
            std::cerr << "Test " << test_num << " failed with reason: " << ex.what() << "\n";
            ++failure_count;
        }
        catch (...)
        {
            std::cerr << "Test " << test_num << " failed with unknown reason\n";
            ++failure_count;
        }

        ++test_num;
    }

    return std::make_tuple(test_set.size(), failure_count);
}

}

int main()
{
    size_t test_count;
    size_t failure_count;

    std::tie(test_count, failure_count) = runTestSet(test_set);

    std::cout << (test_count - failure_count) << " test(s) passed out of " << test_count << "\n";

    return (failure_count == 0) ? 0 : EXIT_FAILURE;
}
