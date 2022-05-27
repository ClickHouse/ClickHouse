#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>

#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Common/Config/ConfigProcessor.h>

using namespace DB;

static int regAggregateFunctions = 0;

void tryRegisterAggregateFunctions()
{
    if (!regAggregateFunctions)
    {
        registerAggregateFunctions();
        regAggregateFunctions = 1;
    }
}

static ConfigProcessor::LoadedConfig loadConfiguration(const std::string & config_path)
{
    ConfigProcessor config_processor(config_path, true, true);
    ConfigProcessor::LoadedConfig config = config_processor.loadConfig(false);
    return config;
}

static ConfigProcessor::LoadedConfig loadConfigurationFromString(std::string & s)
{
    char tmp_file[19];
    strcpy(tmp_file, "/tmp/rollup-XXXXXX");
    int fd = mkstemp(tmp_file);
    if (fd == -1)
    {
        throw std::runtime_error(strerror(errno));
    }
    try {
        if (write(fd, s.c_str(), s.size()) < s.size())
        {
            throw std::runtime_error("unable write to temp file");
        }
        if (write(fd, "\n", 1) != 1)
        {
            throw std::runtime_error("unable write to temp file");
        }
        close(fd);
        auto config_path = std::string(tmp_file) + ".xml";
        if (std::rename(tmp_file, config_path.c_str()))
        {
            int err = errno;
            remove(tmp_file);
            throw std::runtime_error(strerror(err));
        }
        ConfigProcessor::LoadedConfig config = loadConfiguration(config_path);
        remove(tmp_file);
        return config;
    }
    catch (...)
    {
        remove(tmp_file);
        throw;
    }
}

static Graphite::Params setGraphitePatterns(ContextMutablePtr context, ConfigProcessor::LoadedConfig & config)
{
    context->setConfig(config.configuration);

    Graphite::Params params;
    setGraphitePatternsFromConfig(context, "graphite_rollup", params);

    return params;
}

struct  PatternForCheck
{
    Graphite::RuleType rule_type;
    std::string regexp_str;
    String function;
    Graphite::Retentions retentions;
};


bool checkRule(const Graphite::Pattern & pattern, const struct PatternForCheck & pattern_check,
    const std::string & typ, const std::string & path, std::string & message)
{
    bool rule_type_eq = (pattern.rule_type == pattern_check.rule_type);
    bool regexp_eq = (pattern.regexp_str == pattern_check.regexp_str);
    bool function_eq = (pattern.function == nullptr && pattern_check.function.empty())
                    || (pattern.function != nullptr && pattern.function->getName() == pattern_check.function);
    bool retentions_eq = (pattern.retentions == pattern_check.retentions);

    if (rule_type_eq && regexp_eq && function_eq && retentions_eq)
        return true;

    message = typ + " rollup rule mismatch for '" + path + "'," +
        (rule_type_eq ? "" : "rule_type ") +
        (regexp_eq ? "" : "regexp ") +
        (function_eq ? "" : "function ") +
        (retentions_eq ? "" : "retentions ");
    return false;
}

std::ostream & operator<<(std::ostream & stream, const PatternForCheck & a)
{
    stream << "{ rule_type = " << ruleTypeStr(a.rule_type);
    if (!a.regexp_str.empty())
        stream << ", regexp = '" << a.regexp_str << "'";
    if (!a.function.empty())
        stream << ", function = " << a.function;
    if (!a.retentions.empty())
    {
        stream << ",\n  retentions = {\n";
        for (size_t i = 0; i < a.retentions.size(); i++)
        {
            stream << "    { " << a.retentions[i].age << ", " << a.retentions[i].precision << " }";
            if (i < a.retentions.size() - 1)
                stream << ",";
            stream << "\n";
        }
        stream << "  }\n";
    }
    else
        stream << " ";

    stream << "}";
    return stream;
}

struct PatternsForPath
{
    std::string path;
    PatternForCheck retention_want;
    PatternForCheck aggregation_want;
};

TEST(GraphiteTest, testSelectPattern)
{
    tryRegisterAggregateFunctions();

    using namespace std::literals;

    std::string
        xml(R"END(<clickhouse>
<graphite_rollup>
    <pattern>
        <regexp>\.sum$</regexp>
        <function>sum</function>
    </pattern>
    <pattern>
        <regexp>^((.*)|.)sum\?</regexp>
        <function>sum</function>
    </pattern>
    <pattern>
        <regexp>\.max$</regexp>
        <function>max</function>
    </pattern>
    <pattern>
        <regexp>^((.*)|.)max\?</regexp>
        <function>max</function>
    </pattern>
    <pattern>
        <regexp>\.min$</regexp>
        <function>min</function>
    </pattern>
    <pattern>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
    </pattern>
    <pattern>
        <regexp>\.(count|sum|sum_sq)$</regexp>
        <function>sum</function>
    </pattern>
    <pattern>
        <regexp>^((.*)|.)(count|sum|sum_sq)\?</regexp>
        <function>sum</function>
    </pattern>
    <pattern>
        <regexp>^retention\.</regexp>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </pattern>
    <default>
        <function>avg</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
</clickhouse>
)END");

    // Retentions must be ordered by 'age' descending.
    std::vector<struct PatternsForPath> tests
    {
        {
            "test.sum",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(\.sum$)END", "sum", { } }
        },
        {
            "val.sum?env=test&tag=Fake3",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)sum\?)END", "sum", { } }
        },
        {
            "test.max",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(\.max$)END", "max", { } },
        },
        {
            "val.max?env=test&tag=Fake4",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)max\?)END", "max", { } },
        },
        {
            "test.min",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(\.min$)END", "min", { } },
        },
        {
            "val.min?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)min\?)END", "min", { } },
        },
        {
            "retention.count",
            { Graphite::RuleTypeAll, R"END(^retention\.)END", "", { { 86400, 3600 }, { 0, 60 } } }, // ^retention
            { Graphite::RuleTypeAll, R"END(\.(count|sum|sum_sq)$)END", "sum", { } },
        },
        {
            "val.retention.count?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "val.count?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "test.p95",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "val.p95?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "default",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "val.default?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        }
    };

    auto config = loadConfigurationFromString(xml);
    ContextMutablePtr context = getContext().context;
    Graphite::Params params = setGraphitePatterns(context, config);

    for (const auto & t : tests)
    {
        auto rule = DB::Graphite::selectPatternForPath(params, t.path);
        std:: string message;
        if (!checkRule(*rule.first, t.retention_want, "retention", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.first << "\n, want\n" << t.retention_want << "\n";
        if (!checkRule(*rule.second, t.aggregation_want, "aggregation", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.second << "\n, want\n" << t.aggregation_want << "\n";
    }
}


namespace DB::Graphite
{
    std::string buildTaggedRegex(std::string regexp_str);
}

struct RegexCheck
{
    std::string regex;
    std::string regex_want;
    std::string match;
    std::string nomatch;
};

TEST(GraphiteTest, testBuildTaggedRegex)
{
    std::vector<struct RegexCheck> tests
    {
        {
            "cpu\\.loadavg;project=DB.*;env=st.*",
            R"END(^cpu\.loadavg\?(.*&)?env=st.*&(.*&)?project=DB.*(&.*)?$)END",
            R"END(cpu.loadavg?env=staging&project=DBAAS)END",
            R"END(cpu.loadavg?env=staging&project=D)END"
        },
        {
            R"END(project=DB.*;env=staging;)END",
            R"END([\?&]env=staging&(.*&)?project=DB.*(&.*)?$)END",
            R"END(cpu.loadavg?env=staging&project=DBPG)END",
            R"END(cpu.loadavg?env=stagingN&project=DBAAS)END"
        },
        {
            "env=staging;",
            R"END([\?&]env=staging(&.*)?$)END",
            R"END(cpu.loadavg?env=staging&project=DPG)END",
            R"END(cpu.loadavg?env=stagingN)END"
        },
        {
            " env = staging ;", // spaces are allowed,
            R"END([\?&] env = staging (&.*)?$)END",
            R"END(cpu.loadavg? env = staging &project=DPG)END",
            R"END(cpu.loadavg?env=stagingN)END"
        },
        {
            "name;",
            R"END(^name\?)END",
            R"END(name?env=staging&project=DPG)END",
            R"END(nameN?env=stagingN)END",
        },
        {
            "name",
            R"END(^name\?)END",
            R"END(name?env=staging&project=DPG)END",
            R"END(nameN?env=stagingN)END",
        }
    };
    for (const auto & t : tests)
    {
        auto s = DB::Graphite::buildTaggedRegex(t.regex);
        EXPECT_EQ(t.regex_want, s) << "result for '" << t.regex_want << "' mismatch";
        auto regexp = OptimizedRegularExpression(s);
        EXPECT_TRUE(regexp.match(t.match.data(), t.match.size())) << t.match << " match for '" << s << "' failed";
        EXPECT_FALSE(regexp.match(t.nomatch.data(), t.nomatch.size())) << t.nomatch << " ! match for '" << s << "' failed";
    }
}

TEST(GraphiteTest, testSelectPatternTyped)
{
    tryRegisterAggregateFunctions();

    using namespace std::literals;

    std::string
        xml(R"END(<clickhouse>
<graphite_rollup>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>\.sum$</regexp>
        <function>sum</function>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)sum\?</regexp>
        <function>sum</function>
    </pattern>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>\.max$</regexp>
        <function>max</function>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)max\?</regexp>
        <function>max</function>
    </pattern>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>\.min$</regexp>
        <function>min</function>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
    </pattern>
    <pattern>
    <rule_type>plain</rule_type>
     <regexp>\.(count|sum|sum_sq)$</regexp>
     <function>sum</function>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)(count|sum|sum_sq)\?</regexp>
        <function>sum</function>
    </pattern>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>^retention\.</regexp>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[[\?&]retention=hour(&.*)?$]]></regexp>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>retention=10min;env=staging</regexp>
        <retention>
            <age>0</age>
            <precision>600</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>retention=10min;env=[A-Za-z-]+rod[A-Za-z-]+</regexp>
        <retention>
            <age>0</age>
            <precision>600</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>cpu\.loadavg</regexp>
        <retention>
            <age>0</age>
            <precision>600</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </pattern>
    <default>
        <function>avg</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
</clickhouse>
)END");

    // Retentions must be ordered by 'age' descending.
    std::vector<PatternsForPath> tests
    {
        {
            "test.sum",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypePlain, R"END(\.sum$)END", "sum", { } }
        },
        {
            "val.sum?env=test&tag=Fake3",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)sum\?)END", "sum", { } }
        },
        {
            "test.max",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypePlain, R"END(\.max$)END", "max", { } },
        },
        {
            "val.max?env=test&tag=Fake4",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)max\?)END", "max", { } },
        },
        {
            "test.min",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypePlain, R"END(\.min$)END", "min", { } },
        },
        {
            "val.min?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)min\?)END", "min", { } },
        },
        {
            "retention.count",
            { Graphite::RuleTypePlain, R"END(^retention\.)END", "", { { 86400, 3600 }, { 0, 60 } } }, // ^retention
            { Graphite::RuleTypePlain, R"END(\.(count|sum|sum_sq)$)END", "sum", { } },
        },
        {
            "val.count?env=test&retention=hour&tag=Fake5",
            { Graphite::RuleTypeTagged, R"END([\?&]retention=hour(&.*)?$)END", "", { { 86400, 3600 }, { 0, 60 } } }, // tagged retention=hour
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "val.count?env=test&retention=hour",
            { Graphite::RuleTypeTagged, R"END([\?&]retention=hour(&.*)?$)END", "", { { 86400, 3600 }, { 0, 60 } } }, // tagged retention=hour
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "val.count?env=staging&retention=10min",
            { Graphite::RuleTypeTagged, R"END([\?&]env=staging&(.*&)?retention=10min(&.*)?$)END", "", { { 86400, 3600 }, { 0, 600 } } }, // retention=10min ; env=staging
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "val.count?env=production&retention=10min",
            { Graphite::RuleTypeTagged, R"END([\?&]env=[A-Za-z-]+rod[A-Za-z-]+&(.*&)?retention=10min(&.*)?$)END", "", { { 86400, 3600 }, { 0, 600 } } }, // retention=10min ; env=[A-Za-z-]+rod[A-Za-z-]+
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "val.count?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "cpu.loadavg?env=test&tag=FakeNo",
            { Graphite::RuleTypeTagged, R"END(^cpu\.loadavg\?)END", "", { { 86400, 3600 }, { 0, 600 } } }, // name=cpu\.loadavg
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } },
        },
        {
            "test.p95",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "val.p95?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "default",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "val.default?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        }
    };

    auto config = loadConfigurationFromString(xml);
    ContextMutablePtr context = getContext().context;
    Graphite::Params params = setGraphitePatterns(context, config);

    for (const auto & t : tests)
    {
        auto rule = DB::Graphite::selectPatternForPath(params, t.path);
        std:: string message;
        if (!checkRule(*rule.first, t.retention_want, "retention", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.first << "\n, want\n" << t.retention_want << "\n";
        if (!checkRule(*rule.second, t.aggregation_want, "aggregation", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.second << "\n, want\n" << t.aggregation_want << "\n";
    }
}
