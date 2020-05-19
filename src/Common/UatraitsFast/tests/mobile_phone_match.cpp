#include <iostream>
#include <fstream>
#include <Poco/String.h>
#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>
#include <library/unittest/registar.h>

#include <library/unittest/tests_data.h>

namespace
{
    std::string getOutputDirectoryForFile(const std::string & file_name)
    {
        std::string output_directory = static_cast<std::string>(TString(GetOutputPath().RealPath()));
        // Не хочется тянуть дополнительные зависимости а-ля boost/filesystem, чтобы "честно" обрабатывать путь
        if (output_directory.back() != '/')
            output_directory.push_back('/');
        return output_directory + file_name;
    }

    const std::string TEST_PROFILES_FILENAME = "test_profiles.xml";
    const std::string TEST_BROWSER_FILENAME = "test_browser.xml";
    const std::string TEST_EXTRA_FILENAME = "test_extra.xml";
}


struct UATraitsFixture : public NUnitTest::TBaseFixture
{
    std::unique_ptr<UATraits> uatraits;

    UATraitsFixture()
    {
        {
            std::fstream browser(getOutputDirectoryForFile(TEST_BROWSER_FILENAME), std::ios_base::out);
            browser << R"HICSUNTDRACONES(<?xml version="1.0"?>
<!DOCTYPE rules [
<!ENTITY ver1d "[0-9]+">
<!ENTITY ver2d "[0-9]+(?:[\.][0-9]+)?">
<!ENTITY ver3d "[0-9]+(?:[\.][0-9]+){0,2}">
<!ENTITY ver "[0-9][0-9\.]*">
<!ENTITY word "a-z0-9_">
<!ENTITY num "0-9">
<!ENTITY s " ">
]>
<rules xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:noNamespaceSchemaLocation="browser.xsd" minver="1.1" date="2013.08.01.10.00">
    <branch name="Mobile">
        <match type="any">
            <pattern type="string">Android</pattern>
            <pattern type="string">iPhone</pattern>
            <pattern type="string">Bada/</pattern>
            <pattern type="string">iPad</pattern>
            <pattern type="string">iPod;</pattern>
            <pattern type="string">Mobile Safari</pattern>
            <pattern type="string">MobileSafari</pattern>
            <pattern type="string">Opera Mob</pattern>
            <pattern type="string">Opera Tab</pattern>
            <pattern type="string">Opera Mini</pattern>
            <pattern type="string">OPiOS</pattern>
            <pattern type="string">CLDC-</pattern>
            <pattern type="string">MIDP-</pattern>
            <pattern type="string">Series 60</pattern>
            <pattern type="string">Symbian</pattern>
            <pattern type="string">Windows Phone</pattern>
            <pattern type="string">ZuneWP7</pattern>
            <pattern type="string">Skyfire</pattern>
            <pattern type="string">Windows CE</pattern>
            <pattern type="string">Maemo</pattern>
            <pattern type="string"> Tizen</pattern>
            <pattern type="string">Moblin</pattern>
            <pattern type="string">Fennec</pattern>
            <pattern type="string">UCWEB</pattern>
            <pattern type="string">UC Browser</pattern>
            <pattern type="string">UP.Browser</pattern>
            <pattern type="string">NetFront</pattern>
            <pattern type="string">Obigo</pattern>
            <pattern type="string">hpwOS</pattern>
            <pattern type="string">webOS</pattern>
            <pattern type="string">BREW</pattern>
            <pattern type="string">OpenWave</pattern>
            <pattern type="string">WAP</pattern>
            <pattern type="string">Nokia</pattern>
            <pattern type="string">DoCoMo</pattern>
            <pattern type="string">Kindle</pattern>
            <pattern type="string">Minimo</pattern>
            <pattern type="string">PlayStation Portable</pattern>
            <pattern type="string">Tablet browser</pattern>
            <pattern type="string">BlackBerry</pattern>
            <pattern type="string">PlayBook</pattern>
            <pattern type="string">PalmSource</pattern>
            <pattern type="string">MQQBrowser</pattern>
            <pattern type="string">iTunes-i</pattern>
            <pattern type="string">MAUI</pattern>
            <pattern type="string">Novarra-Vision</pattern>
            <pattern type="string">Puffin/</pattern>
            <pattern type="string">Ubuntu; Mobile</pattern>
            <pattern type="string">Ubuntu; Touch</pattern>
            <pattern type="regex">Windows NT.*ARM;</pattern>
            <pattern type="regex">\(Mobile;.*Gecko/</pattern>
            <pattern type="regex">MSIE.*PPC[;)]</pattern>
            <pattern type="regex">HTC[_/]</pattern>
            <pattern type="regex">^SAMSUNG</pattern>
            <pattern type="regex">^HUAWEI</pattern>
            <pattern type="regex">^Fly</pattern>
            <pattern type="regex">^SonyEricsson</pattern>
            <pattern type="regex">^Alcatel</pattern>
            <pattern type="regex">^[&word;]+ Opera/</pattern>
        </match>
        <define name="isMobile" value="true" />
        <define name="isBrowser" value="true" />
        <branch type="common" name="DeviceProps">
            <define name="DeviceName">
                <pattern type="string" value="Xperia M">C1905</pattern>
                <pattern type="regex" value="Login 3">(MFLogin3[^ ]*)</pattern>
                <pattern type="regex" value="Smarto">(3GD52i|3GDi10)</pattern>
                <pattern type="regex" value="Galaxy S5">(SM-G9006V|SM-G9008V|SM-G9009D|SM-G900A|SM-G900D|SM-G900F|SM-G900H|SM-G900I|SM-G900J|SM-G900K|SM-G900L|SM-G900M|SM-G900P|SM-G900R4|SM-G900S|SM-G900T|SM-G900V|SM-G900W8) Build</pattern>
                <pattern type="regex" value="$1">(Lumia [&num;]+)\)</pattern>
                <pattern type="regex" value="$1">;&s;?([a-zA-Z0-9/\_\-\.\+'"\(\)\\,&s;]+) Build</pattern>
            </define>
        </branch>
    </branch>
</rules>
)HICSUNTDRACONES";
        }
        {
            std::fstream profiles(getOutputDirectoryForFile(TEST_PROFILES_FILENAME), std::ios_base::out);
            profiles << R"HICSUNTDRACONES(<?xml version="1.0"?>
<profiles>
 <profile url="http://122.200.68.229/docs/mini3ix.xml" id="7d80b53134b8d7b1610cb3a5b3314183">
  <define name="DeviceKeyboard" value="SoftKeyPad"/>
  <define name="DeviceModel" value="Dell_Mini3iX"/>
  <define name="DeviceVendor" value="dell"/>
  <define name="ScreenSize" value="360x640"/>
  <define name="BitsPerPixel" value="24"/>
  <define name="ScreenWidth" value="360"/>
  <define name="ScreenHeight" value="640"/>
 </profile>
 <profile url="http://218.249.47.94/Xianghe/MTK_Athens15_UAProfile.xml" id="297202bf0b4e70a850ee14843b8d1df0">
  <define name="DeviceKeyboard" value="No"/>
  <define name="DeviceModel" value="eagle75"/>
  <define name="DeviceVendor" value="MediaTek"/>
  <define name="ScreenSize" value="480x800"/>
  <define name="BitsPerPixel" value="16"/>
  <define name="ScreenWidth" value="480"/>
  <define name="ScreenHeight" value="800"/>
 </profile>
 <profile url="http://50.18.182.85/profile/AZ210_UA_Profile.xml" id="9fb462aff8946ee01954251cff8ffd5d">
  <define name="DeviceKeyboard" value="No"/>
  <define name="DeviceModel" value="AZ210"/>
  <define name="DeviceVendor" value="Orange"/>
  <define name="ScreenSize" value="600x1024"/>
  <define name="BitsPerPixel" value="16"/>
  <define name="ScreenWidth" value="600"/>
  <define name="ScreenHeight" value="1024"/>
 </profile>
</profiles>
)HICSUNTDRACONES";
        }
        {
            std::fstream extra(getOutputDirectoryForFile(TEST_EXTRA_FILENAME), std::ios_base::out);
            extra << R"HICSUNTDRACONES(<?xml version="1.0"?>
<rules minver="1.1.6">
    <rule name="ITPFakeCookie" value="true">
        <group type="or">
            <group type="and">
                <eq field="OSFamily">iOS</eq>
                <eq field="BrowserName">MobileSafari</eq>
                <gte field="OSVersion">11</gte>
                <lte field="OSVersion">12.1</lte>
            </group>
            <group type="and">
                <eq field="OSFamily">MacOS</eq>
                <eq field="BrowserName">Safari</eq>
                <gte field="OSVersion">10.13</gte>
            </group>
        </group>
    </rule>
    <rule name="ITP" value="true">
        <group type="or">
            <group type="and">
                <eq field="OSFamily">iOS</eq>
                <eq field="BrowserName">MobileSafari</eq>
                <gte field="OSVersion">11</gte>
            </group>
            <group type="and">
                <eq field="OSFamily">MacOS</eq>
                <eq field="BrowserName">Safari</eq>
                <gte field="OSVersion">10.13</gte>
            </group>
        </group>
    </rule>
    <rule name="ITPMaybe" value="true">
        <group type="or">
            <group type="and">
                <eq field="BrowserName">Firefox</eq>
                <gte field="BrowserVersion">67</gte>
            </group>
            <group type="and">
                <eq field="BrowserName">MobileFirefox</eq>
                <gte field="BrowserVersion">67</gte>
            </group>
        </group>
    </rule>
</rules>
)HICSUNTDRACONES";
        }
        uatraits = std::make_unique<UATraits>(
                getOutputDirectoryForFile(TEST_BROWSER_FILENAME),
                getOutputDirectoryForFile(TEST_PROFILES_FILENAME),
                getOutputDirectoryForFile(TEST_EXTRA_FILENAME));
    }

    void checkNamesSimple(std::string ua, std::string d, std::string v)
    {
        UATraits::MatchedSubstrings matched_substrings;
        UATraits::Result result;
        std::string user_agent_str;
        user_agent_str = Poco::toLower(ua);
        std::string empty;
        uatraits->detect(user_agent_str, empty, empty, result, matched_substrings);

        UNIT_ASSERT_EQUAL_C(result.string_ref_fields[UATraits::Result::DeviceName], d, "Passing '" << ua << "'");
        UNIT_ASSERT_EQUAL_C(result.string_ref_fields[UATraits::Result::DeviceVendor], v, "Passing '" << ua << "'");
    }

    void checkNamesCaseSafeSimple(std::string ua, std::string d, std::string v)
    {
        UATraits::MatchedSubstrings matched_substrings;
        UATraits::Result result;
        std::string user_agent_str;
        user_agent_str = Poco::toLower(ua);
        std::string empty;
        uatraits->detectCaseSafe(ua, user_agent_str,
                empty, empty, empty,
                result, matched_substrings);

        UNIT_ASSERT_EQUAL_C(result.string_ref_fields[UATraits::Result::DeviceName], d, "Passing '" << ua << "'");
        UNIT_ASSERT_EQUAL_C(result.string_ref_fields[UATraits::Result::DeviceVendor], v, "Passing '" << ua << "'");
    }

    ~UATraitsFixture()
    {
    }
};

Y_UNIT_TEST_SUITE_F(MobilePhoneMatch, UATraitsFixture)
{

Y_UNIT_TEST(Simple)
{
    checkNamesSimple("",
            "", "");
    checkNamesSimple("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; BTRS122332; MRSPUTNIK 2, 4, 0, 516; GTB7.5; MRA 5.10 (build 5339); .NET4.0C; InfoPath.2)",
            "", ""); /// no match
    checkNamesSimple("Android; Samsung SM-G900W8; 22 (5.1.1)",
            "", ""); /// SM-G900W8 without Build afterwards
    checkNamesSimple("CrpKik/8.4.18.095 (Android 5.1.1) Mozilla/5.0 (Linux; Android 5.1.1; SM-G900W8 Build/LMY47X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/49.0.2623.105 Mobile Safari/537.36",
            "Galaxy S5", "");
    checkNamesSimple("MQQBrowser/6.6 (MIDP-2.0; U; zh-cn; SM-G900W8)",
            "", ""); /// SM-G900W8 without Build afterwards
    checkNamesSimple("Mozilla/5.0 (Linux; 5.1.1; SM-G900W8 Build/LMY47X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/49.0.2623.105 Mobile Safari/537.36",
            "Galaxy S5", "");
    checkNamesSimple("Mozilla/5.0 (Mobile; Windows Phone 8.1; Android 4.0; ARM; Trident/7.0; Touch; MSAppHost/2.0; rv:11.0; IEMobile/11.0; NOKIA; Lumia 830) like iPhone OS 7_0_3 Mac OS X AppleWebKit/537 (KHTML, like Gecko) Mobile Safari/537",
            "lumia 830", "");
    checkNamesSimple("MWP/1.0/Mozilla/5.0 (Windows Phone 8.1; ARM; Trident/8.0; Touch; rv:11.0; WebBrowser/8.1; IEMobile/11.0; Microsoft; Lumia 640 LTE Dual SIM) like Gecko",
            "", ""); /// Lumia 640 without parenthesis after
    checkNamesSimple("UCWEB/2.0 (Windows; U; wds 7.10; ru-RU; NOKIA; Lumia 900) U2/1.0.0 UCBrowser/3.2.0.340 U2/1.0.0 Mobile",
            "lumia 900", "");
    checkNamesSimple("Jimdo-Android/2016.03.17-a0fe41b (C1905; 4.3; ru_RU)",
            "Xperia M", "");
    checkNamesSimple("Mozilla/5.0 (Linux; Android 4.1.2; C1905 Build/15.1.B.0.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.135 Mobile Safari/537.36",
            "Xperia M", "");
    checkNamesSimple("Snapchat/9.27.0.0 (C1905; Andr_oid 4.3#eng.user.20140509.125022#18; gzip)",
            "", ""); /// broken Android prerequisite
    checkNamesSimple("WomanRU/713 (Sony C1905; Android 4.3; Scale/1,50)",
            "Xperia M", "");
}

Y_UNIT_TEST(CaseSafeSimple)
{
    checkNamesCaseSafeSimple("",
            "", "");
    checkNamesCaseSafeSimple("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; BTRS122332; MRSPUTNIK 2, 4, 0, 516; GTB7.5; MRA 5.10 (build 5339); .NET4.0C; InfoPath.2)",
            "", ""); /// no match
    checkNamesCaseSafeSimple("Android; Samsung SM-G900W8; 22 (5.1.1)",
            "", ""); /// SM-G900W8 without Build afterwards
    checkNamesCaseSafeSimple("CrpKik/8.4.18.095 (Android 5.1.1) Mozilla/5.0 (Linux; Android 5.1.1; SM-G900W8 Build/LMY47X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/49.0.2623.105 Mobile Safari/537.36",
            "Galaxy S5", "");
    checkNamesCaseSafeSimple("MQQBrowser/6.6 (MIDP-2.0; U; zh-cn; SM-G900W8)",
            "", ""); /// SM-G900W8 without Build afterwards
    checkNamesCaseSafeSimple("Mozilla/5.0 (Linux; 5.1.1; SM-G900W8 Build/LMY47X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/49.0.2623.105 Mobile Safari/537.36",
            "Galaxy S5", "");
    checkNamesCaseSafeSimple("Mozilla/5.0 (Mobile; Windows Phone 8.1; Android 4.0; ARM; Trident/7.0; Touch; MSAppHost/2.0; rv:11.0; IEMobile/11.0; NOKIA; Lumia 830) like iPhone OS 7_0_3 Mac OS X AppleWebKit/537 (KHTML, like Gecko) Mobile Safari/537",
            "Lumia 830", "");
    checkNamesCaseSafeSimple("MWP/1.0/Mozilla/5.0 (Windows Phone 8.1; ARM; Trident/8.0; Touch; rv:11.0; WebBrowser/8.1; IEMobile/11.0; Microsoft; Lumia 640 LTE Dual SIM) like Gecko",
            "", ""); /// Lumia 640 without parenthesis after
    checkNamesCaseSafeSimple("UCWEB/2.0 (Windows; U; wds 7.10; ru-RU; NOKIA; Lumia 900) U2/1.0.0 UCBrowser/3.2.0.340 U2/1.0.0 Mobile",
            "Lumia 900", "");
    checkNamesCaseSafeSimple("Jimdo-Android/2016.03.17-a0fe41b (C1905; 4.3; ru_RU)",
            "Xperia M", "");
    checkNamesCaseSafeSimple("Mozilla/5.0 (Linux; Android 4.1.2; C1905 Build/15.1.B.0.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.135 Mobile Safari/537.36",
            "Xperia M", "");
    checkNamesCaseSafeSimple("Snapchat/9.27.0.0 (C1905; Andr_oid 4.3#eng.user.20140509.125022#18; gzip)",
            "", ""); /// broken Android prerequisite
    checkNamesCaseSafeSimple("WomanRU/713 (Sony C1905; Android 4.3; Scale/1,50)",
            "Xperia M", "");
}

}
