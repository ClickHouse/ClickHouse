#include <Storages/getOrCreateDiskFromSettings.h>
#include <Common/logger_useful.h>
#include <Common/FieldVisitorToString.h>
#include <Common/filesystemHelpers.h>
#include <Disks/DiskSelector.h>
#include <Interpreters/Context.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <ranges>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using DiskSettings = std::vector<std::pair<std::string, std::string>>;

    Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromSettingsImpl(const std::string & root_name, const DiskSettings & settings)
    {
        Poco::AutoPtr<Poco::XML::Document> xml_document(new Poco::XML::Document());
        Poco::AutoPtr<Poco::XML::Element> root(xml_document->createElement("disk"));
        xml_document->appendChild(root);
        Poco::AutoPtr<Poco::XML::Element> disk_configuration(xml_document->createElement(root_name));
        root->appendChild(disk_configuration);

        for (const auto & [name, value] : settings)
        {
            Poco::AutoPtr<Poco::XML::Element> key_element(xml_document->createElement(name));
            disk_configuration->appendChild(key_element);

            Poco::AutoPtr<Poco::XML::Text> value_element(xml_document->createTextNode(value));
            key_element->appendChild(value_element);
        }

        return xml_document;
    }

    DiskConfigurationPtr getDiskConfigurationFromSettings(const std::string & root_name, const DiskSettings & settings)
    {
        auto xml_document = getDiskConfigurationFromSettingsImpl(root_name, settings);
        Poco::AutoPtr<Poco::Util::XMLConfiguration> conf(new Poco::Util::XMLConfiguration());
        conf->load(xml_document);
        return conf;
    }
}

std::string getOrCreateDiskFromSettings(const SettingsChanges & configuration, ContextPtr context)
{
    /// We need a unique name for a created custom disk, but it needs to be the same
    /// after table is reattached or server is restarted, so take a hash of the disk
    /// configuration serialized ast as a disk name suffix.
    auto settings_range = configuration
        | std::views::transform(
            [](const auto & setting) { return std::pair(setting.name, convertFieldToString(setting.value)); });

    DiskSettings sorted_settings(settings_range.begin(), settings_range.end());
    std::sort(sorted_settings.begin(), sorted_settings.end());

    std::string disk_setting_string;
    for (auto it = sorted_settings.begin(); it != sorted_settings.end(); ++it)
    {
        if (it != sorted_settings.begin())
            disk_setting_string += ",";
        disk_setting_string += std::get<0>(*it) + '=' + std::get<1>(*it);
    }

    auto disk_name = DiskSelector::TMP_DISK_PREFIX
        + toString(sipHash128(disk_setting_string.data(), disk_setting_string.size()));

    LOG_TRACE(
        &Poco::Logger::get("getOrCreateDiskFromSettings"),
        "Using disk name `{}` for custom disk {}",
        disk_name, disk_setting_string);

    auto result_disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        auto config = getDiskConfigurationFromSettings(disk_name, sorted_settings);
        auto disk = DiskFactory::instance().create(disk_name, *config, disk_name, context, disks_map);
        /// Mark that disk can be used without storage policy.
        disk->markDiskAsCustom();
        return disk;
    });

    if (!result_disk->isRemote())
    {
        static constexpr auto custom_disks_base_dir_in_config = "custom_local_disks_base_directory";
        auto disk_path_expected_prefix = context->getConfigRef().getString(custom_disks_base_dir_in_config, "");

        if (disk_path_expected_prefix.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Base path for custom local disks must be defined in config file by `{}`",
                custom_disks_base_dir_in_config);

        if (!pathStartsWith(result_disk->getPath(), disk_path_expected_prefix))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path of the custom local disk must be inside `{}` directory",
                disk_path_expected_prefix);
    }

    return disk_name;
}

}
