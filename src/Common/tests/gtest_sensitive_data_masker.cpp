#include <Common/Exception.h>
#include <Common/SensitiveDataMasker.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/XML/XMLException.h>

#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wundef"

#include <gtest/gtest.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_COMPILE_REGEXP;
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int INVALID_CONFIG_PARAMETER;
}
}


TEST(Common, SensitiveDataMasker)
{

    Poco::AutoPtr<Poco::Util::XMLConfiguration> empty_xml_config = new Poco::Util::XMLConfiguration();
    DB::SensitiveDataMasker masker(*empty_xml_config, "");
    masker.addMaskingRule("all a letters", "a+", "--a--", /*throw_on_match=*/false);
    masker.addMaskingRule("all b letters", "b+", "--b--", /*throw_on_match=*/false);
    masker.addMaskingRule("all d letters", "d+", "--d--", /*throw_on_match=*/false);
    masker.addMaskingRule("all x letters", "x+", "--x--", /*throw_on_match=*/false);
    masker.addMaskingRule("rule \"d\" result", "--d--", "*****", /*throw_on_match=*/false); // RE2 regexps are applied one-by-one in order
    std::string x = "aaaaaaaaaaaaa   bbbbbbbbbb cccc aaaaaaaaaaaa d ";
    EXPECT_EQ(masker.wipeSensitiveData(x), 5);
    EXPECT_EQ(x, "--a--   --b-- cccc --a-- ***** ");
#ifndef NDEBUG
    masker.printStats();
#endif
    EXPECT_EQ(masker.wipeSensitiveData(x), 3);
    EXPECT_EQ(x, "----a----   ----b---- cccc ----a---- ***** ");
#ifndef NDEBUG
    masker.printStats();
#endif

    DB::SensitiveDataMasker masker2(*empty_xml_config, "");
    masker2.addMaskingRule("hide root password", "qwerty123", "******", /*throw_on_match=*/false);
    masker2.addMaskingRule("hide SSN", "[0-9]{3}-[0-9]{2}-[0-9]{4}", "000-00-0000", /*throw_on_match=*/false);
    masker2.addMaskingRule("hide email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}", "hidden@hidden.test", /*throw_on_match=*/false);

    std::string query = "SELECT id FROM mysql('localhost:3308', 'database', 'table', 'root', 'qwerty123') WHERE ssn='123-45-6789' or "
                        "email='JonhSmith@secret.domain.test'";
    EXPECT_EQ(masker2.wipeSensitiveData(query), 3);
    EXPECT_EQ(
        query,
        "SELECT id FROM mysql('localhost:3308', 'database', 'table', 'root', '******') WHERE "
        "ssn='000-00-0000' or email='hidden@hidden.test'");

    DB::SensitiveDataMasker maskerbad(*empty_xml_config, "");

    // gtest has not good way to check exception content, so just do it manually (see https://github.com/google/googletest/issues/952 )
    try
    {
        maskerbad.addMaskingRule("bad regexp", "**", "", /*throw_on_match=*/false);
        ADD_FAILURE() << "addMaskingRule() should throw an error" << std::endl;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(
            std::string(e.what()),
            "SensitiveDataMasker: cannot compile re2: **, error: no argument for repetition operator: *. Look at "
            "https://github.com/google/re2/wiki/Syntax for reference.");
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
    }
    /* catch (...) { // not needed, gtest will react unhandled exception
        FAIL() << "ERROR: Unexpected exception thrown: " << std::current_exception << std::endl; // std::current_exception is part of C++11x
    } */

    EXPECT_EQ(maskerbad.rulesCount(), 0);
    EXPECT_EQ(maskerbad.wipeSensitiveData(x), 0);

    try
    {
        std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            xml_isteam(R"END(<clickhouse>
    <query_masking_rules>
        <rule>
            <name>test</name>
            <regexp>abc</regexp>
        </rule>
        <rule>
            <name>test</name>
            <regexp>abc</regexp>
        </rule>
    </query_masking_rules>
</clickhouse>)END");

        Poco::AutoPtr<Poco::Util::XMLConfiguration> xml_config = new Poco::Util::XMLConfiguration(xml_isteam);
        DB::SensitiveDataMasker masker_xml_based_exception_check(*xml_config, "query_masking_rules");

        ADD_FAILURE() << "XML should throw an error on bad XML" << std::endl;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(
            std::string(e.what()),
            "query_masking_rules configuration contains more than one rule named 'test'.");
        EXPECT_EQ(e.code(), DB::ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    try
    {
        std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            xml_isteam(R"END(<clickhouse>
    <query_masking_rules>
        <rule><name>test</name></rule>
    </query_masking_rules>
</clickhouse>)END");

        Poco::AutoPtr<Poco::Util::XMLConfiguration> xml_config = new Poco::Util::XMLConfiguration(xml_isteam);
        DB::SensitiveDataMasker masker_xml_based_exception_check(*xml_config, "query_masking_rules");

        ADD_FAILURE() << "XML should throw an error on bad XML" << std::endl;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(
            std::string(e.what()),
            "query_masking_rules configuration, rule 'test' has no <regexp> node or <regexp> is empty.");
        EXPECT_EQ(e.code(), DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    }

    try
    {
        std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            xml_isteam(R"END(<clickhouse>
    <query_masking_rules>
        <rule><name>test</name><regexp>())(</regexp></rule>
    </query_masking_rules>
</clickhouse>)END");

        Poco::AutoPtr<Poco::Util::XMLConfiguration> xml_config = new Poco::Util::XMLConfiguration(xml_isteam);
        DB::SensitiveDataMasker masker_xml_based_exception_check(*xml_config, "query_masking_rules");

        ADD_FAILURE() << "XML should throw an error on bad XML" << std::endl;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(
            std::string(e.message()),
            "SensitiveDataMasker: cannot compile re2: ())(, error: unexpected ): ())(. Look at https://github.com/google/re2/wiki/Syntax for reference.: while adding query masking rule 'test'."
        );
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
    }

    {
        std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            xml_isteam(R"END(
<clickhouse>
    <query_masking_rules>
        <rule>
            <name>hide SSN</name><!-- by default: it will use xml path, like query_masking_rules.rule[1] -->
            <regexp>[0-9]{3}-[0-9]{2}-[0-9]{4}</regexp><!-- mandatory -->
            <replace>000-00-0000</replace><!-- by default - six asterisks (******) -->
        </rule>
        <rule>
            <name>hide root password</name>
            <regexp>qwerty123</regexp>
        </rule>
        <rule>
            <regexp>(?i)Ivan</regexp>
            <replace>John</replace>
        </rule>
        <rule>
            <regexp>(?i)Petrov</regexp>
            <replace>Doe</replace>
        </rule>
        <rule>
            <name>hide email</name>
            <regexp>(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}</regexp>
            <replace>hidden@hidden.test</replace>
        </rule>
        <rule>
            <name>remove selects to bad_words table</name>
            <regexp>^.*bad_words.*$</regexp>
            <replace>[QUERY IS CENSORED]</replace>
        </rule>
    </query_masking_rules>
</clickhouse>)END");

        Poco::AutoPtr<Poco::Util::XMLConfiguration> xml_config = new Poco::Util::XMLConfiguration(xml_isteam);
        DB::SensitiveDataMasker masker_xml_based(*xml_config, "query_masking_rules");
        std::string top_secret = "The e-mail of IVAN PETROV is kotik1902@sdsdf.test, and the password is qwerty123";
        EXPECT_EQ(masker_xml_based.wipeSensitiveData(top_secret), 4);
        EXPECT_EQ(top_secret, "The e-mail of John Doe is hidden@hidden.test, and the password is ******");

        top_secret = "SELECT * FROM bad_words";
        EXPECT_EQ(masker_xml_based.wipeSensitiveData(top_secret), 1);
        EXPECT_EQ(top_secret, "[QUERY IS CENSORED]");

#ifndef NDEBUG
        masker_xml_based.printStats();
#endif
    }
}
