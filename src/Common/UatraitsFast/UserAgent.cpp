#include <Common/UatraitsFast/UserAgent.h>

#include "types/OperatingSystem.h"
#include <boost/algorithm/string/case_conv.hpp>

namespace components
{
void UserAgent::create(const Poco::Util::AbstractConfiguration & config)
{
    reload_frequency = std::chrono::seconds(config.getInt("reload_frequency_sec"));
    browsers_path = config.getString("browsers_path");
    profiles_path = config.getString("profiles_path");
    extra_path = config.getString("extra_path");
}

bool UserAgent::isReady() const
{
    const std::shared_lock<std::shared_mutex> lock(ua_traits_mutex);
    return ua_traits != nullptr;
}

void UserAgent::reload()
{
    auto ua_traits_ = std::make_unique<UATraits>(browsers_path, profiles_path, extra_path);
    const std::unique_lock<std::shared_mutex> lock(ua_traits_mutex);
    ua_traits.swap(ua_traits_);
}

const std::chrono::seconds & UserAgent::getReloadFrequency() const
{
    return reload_frequency;
}

UserAgent::Agent UserAgent::detect(
    const std::string & user_agent,
    const std::string & profile,
    const std::string & opera_mini_user_agent) const
{
    UATraits::Result engine_result;
    UATraits::MatchedSubstrings matched_substrings;

    const std::shared_lock<std::shared_mutex> lock(ua_traits_mutex);
    if (!ua_traits)
        throw std::runtime_error("UserAgentTraits is not ready");

    ua_traits->detect(
        boost::to_lower_copy(user_agent),
        profile,
        boost::to_lower_copy(opera_mini_user_agent),
        engine_result,
        matched_substrings);

    return Agent{
        engine_result.bool_fields[UATraits::Result::SameSiteSupport],
        engine_result.bool_fields[UATraits::Result::isMobile],
        engine_result.bool_fields[UATraits::Result::isBrowser],
        engine_result.bool_fields[UATraits::Result::ITP],
        {
            engine_result.string_ref_fields[UATraits::Result::OSFamily].toString(),
            engine_result.version_fields[UATraits::Result::OSVersion],
        },
        {
            engine_result.string_ref_fields[UATraits::Result::BrowserName].toString(),
            engine_result.version_fields[UATraits::Result::BrowserVersion],
            engine_result.string_ref_fields[UATraits::Result::BrowserBase].toString(),
            engine_result.version_fields[UATraits::Result::BrowserBaseVersion],
        }
    };
}

UserAgent::Agent UserAgent::detect(const std::string & user_agent) const
{
    return detect(user_agent, {}, {});
}

UserAgent::OperatingSystem::OperatingSystem(
    const std::string & os,
    const UATraits::Version & version_)
    : operating_system(tools::operatingSystemFromString(os))
    , version(version_)
{
}

uatraits::types::OperatingSystem UserAgent::OperatingSystem::getName() const
{
    return operating_system;
}

const UATraits::Version & UserAgent::OperatingSystem::getVersion() const
{
    return version;
}

UserAgent::Browser::Browser(const std::string & name_, const UATraits::Version & version_, const std::string & browser_base_, const UATraits::Version & version_base_)
    : browser(appmetrica::types::browserFromString(name_))
    , browser_base(appmetrica::types::browserBaseFromString(browser_base_))
    , version(version_)
    , version_base(version_base_)
{
}

UserAgent::Browser::Browser(const uatraits::types::Browser browser_, const UATraits::Version & version_)
    : browser(browser_)
    , browser_base{}
    , version(version_)
    , version_base{}
{
}

const std::optional<uatraits::types::Browser> & UserAgent::Browser::getName() const
{
    return browser;
}

const std::optional<uatraits::types::BrowserBase> & UserAgent::Browser::getBase() const
{
    return browser_base;
}

const UATraits::Version & UserAgent::Browser::getVersion() const
{
    return version;
}

const UATraits::Version & UserAgent::Browser::getBaseVersion() const
{
    return version_base;
}


UserAgent::Agent::Agent(
    const bool has_same_site_support_,
    const bool is_mobile_,
    const bool is_browser_,
    const bool is_itp_enabled_,
    const OperatingSystem & operating_system_,
    const Browser & browser_)
    : has_same_site_support(has_same_site_support_)
    , is_browser(is_browser_)
    , is_itp_enabled(is_itp_enabled_)
    , is_mobile(is_mobile_)
    , operating_system(operating_system_)
    , browser(browser_)
{
}

bool UserAgent::Agent::hasSameSiteSupport() const
{
    return has_same_site_support;
}

bool UserAgent::Agent::isMobile() const
{
    return is_mobile;
}

bool UserAgent::Agent::isBrowser() const
{
    return is_browser;
}

bool UserAgent::Agent::isItpEnabled() const
{
    return is_itp_enabled;
}

const UserAgent::OperatingSystem & UserAgent::Agent::getOperatingSystem() const
{
    return operating_system;
}

const UserAgent::Browser & UserAgent::Agent::getBrowser() const
{
    return browser;
}

} // namespace components
