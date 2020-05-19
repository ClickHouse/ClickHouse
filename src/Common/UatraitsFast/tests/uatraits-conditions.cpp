#include <boost/algorithm/string/case_conv.hpp>
#include <iostream>
#include <library/unittest/registar.h>
#include <library/unittest/tests_data.h>
#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>
#include <Poco/Util/MapConfiguration.h>
#include <util/system/compiler.h>

Y_UNIT_TEST_SUITE(UATraitsCondition)
{

Y_UNIT_TEST(YaITPExpVersion)
{
    auto ua_traits = std::make_unique<UATraits>("data/browser.xml", "data/profiles.xml", "data/extra.xml");
    UATraits::Result engine_result;
    UATraits::MatchedSubstrings matched_substrings;

    auto detect = [&](const std::string & user_agent) {
        ua_traits->detect(
                boost::to_lower_copy(user_agent),
                {},
                {},
                engine_result,
                matched_substrings);
    };

    detect("Mozilla/5.0 (Linux; arm; Android 8.1.0; Moto G (5S)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.92 YaBrowser/19.10.1.81.00 Mobile Safari/537.36");
    UNIT_ASSERT_EQUAL(engine_result.version_fields[UATraits::Result::YaITPExpVersion].v1, 0);
    UNIT_ASSERT(engine_result.version_fields[UATraits::Result::YaITPExpVersion].empty());
    UNIT_ASSERT(!engine_result.bool_fields[UATraits::Result::ITP]);

    detect("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36");
    UNIT_ASSERT_EQUAL(engine_result.version_fields[UATraits::Result::YaITPExpVersion].v1, 0);
    UNIT_ASSERT(engine_result.version_fields[UATraits::Result::YaITPExpVersion].empty());
    UNIT_ASSERT(!engine_result.bool_fields[UATraits::Result::ITP]);

    detect("Mozilla/5.0 (Linux; arm; Android 8.1.0; Moto G (5S)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.92 YaBrowser/19.10.1.81.00 Mobile Safari/537.36 Yptp/1.49");
    UNIT_ASSERT_EQUAL(engine_result.version_fields[UATraits::Result::YaITPExpVersion].v1, 49);
    UNIT_ASSERT(!engine_result.version_fields[UATraits::Result::YaITPExpVersion].empty());
    UNIT_ASSERT(!engine_result.bool_fields[UATraits::Result::ITP]);

    detect("Mozilla/5.0 (Linux; arm; Android 8.1.0; Moto G (5S)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.92 YaBrowser/19.10.1.81.00 Mobile Safari/537.36 Yptp/1.50");
    UNIT_ASSERT_EQUAL(engine_result.version_fields[UATraits::Result::YaITPExpVersion].v1, 50);
    UNIT_ASSERT(!engine_result.version_fields[UATraits::Result::YaITPExpVersion].empty());
    UNIT_ASSERT(engine_result.bool_fields[UATraits::Result::ITP]);

    detect("Mozilla/5.0 (Linux; arm; Android 8.1.0; Moto G (5S)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.92 YaBrowser/19.10.1.81.00 Mobile Safari/537.36 Yptp/1.99");
    UNIT_ASSERT_EQUAL(engine_result.version_fields[UATraits::Result::YaITPExpVersion].v1, 99);
    UNIT_ASSERT(!engine_result.version_fields[UATraits::Result::YaITPExpVersion].empty());
    UNIT_ASSERT(engine_result.bool_fields[UATraits::Result::ITP]);
}

}
