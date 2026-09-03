#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int BAD_ARGUMENTS;
}


/** Allows you to select the compression settings for the conditions specified in the configuration file.
  * The config looks like this

    <compression>

        <!-- Set of options. Options are checked in a row. The last worked option wins. If none has worked, then lz4 is used. -->
        <case>

            <!-- Conditions. All must be satisfied simultaneously. Some conditions may not be specified. -->
            <min_part_size>10000000000</min_part_size>         <!-- The minimum size of a part in bytes. -->
            <min_part_size_ratio>0.01</min_part_size_ratio>    <!-- The minimum size of the part relative to all the data in the table. -->

            <!-- Which compression method to choose. -->
            <method>zstd</method>
            <level>2</level>
        </case>

        <case>
                ...
        </case>
    </compression>
  */
class CompressionCodecSelector
{
private:
    struct Element
    {
        size_t min_part_size = 0;
        double min_part_size_ratio = 0;
        std::string family_name;
        std::optional<int> level;


        Element(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
        {
            min_part_size = config.getUInt64(config_prefix + ".min_part_size", 0);
            min_part_size_ratio = config.getDouble(config_prefix + ".min_part_size_ratio", 0);

            family_name = config.getString(config_prefix + ".method", "lz4");
            if (config.has(config_prefix + ".level"))
                level = config.getInt64(config_prefix + ".level");
        }

        bool check(size_t part_size, double part_size_ratio) const
        {
            return part_size >= min_part_size
                && part_size_ratio >= min_part_size_ratio;
        }
    };

    std::vector<Element> elements;

public:
    CompressionCodecSelector() = default;    /// Always returns the default method.

    /// `validation_settings` carries the server-level policy (the default profile), not the settings of
    /// whichever query happens to construct the selector first: the selector is built once and shared.
    CompressionCodecSelector(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const CodecValidationSettings & validation_settings)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);

        const auto & factory = CompressionCodecFactory::instance();

        for (const auto & name : keys)
        {
            if (!startsWith(name, "case"))
                throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}.{}, must be 'case'", config_prefix, name);

            const auto & element = elements.emplace_back(config, config_prefix + "." + name);

            /// The chosen codec becomes the part's default codec, which some writers (statistics and
            /// text-index streams) feed raw, untyped data into. A codec that requires a column type
            /// (e.g. `PCO`) would only fail later, at the first such write, with a confusing error.
            /// Reject it here — mirroring how the `default_compression_codec`, `marks_compression_codec`
            /// and `primary_key_compression_codec` settings are validated — so a misconfiguration is
            /// reported when the server configuration is loaded. A lossy codec (e.g. `SZ3`) is rejected
            /// by `get` itself while resolving without a column type.
            /// A gated codec (experimental or beta) is rejected as well, unless the server-level policy
            /// (the default profile) enables it: the selected codec becomes the default codec of every new
            /// part, so putting a gated codec there must be as explicit an opt-in as using one in a query.
            auto codec = factory.get(element.family_name, element.level);
            if (codec->requiresColumnTypeToCompress())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The '{}' configuration cannot use the codec {} because it requires a column type and is applied"
                    " to untyped data",
                    config_prefix,
                    element.family_name);
            if (CompressionCodecFactory::isCodecFamilyGated(element.family_name))
            {
                try
                {
                    factory.validateCodec(element.family_name, element.level, validation_settings);
                }
                catch (Exception & e)
                {
                    e.addMessage(
                        "while checking the '{}' configuration: a gated codec can only be used there when it is"
                        " enabled in the default profile",
                        config_prefix);
                    throw;
                }
            }
        }
    }

    CompressionCodecPtr choose(size_t part_size, double part_size_ratio) const
    {
        const auto & factory = CompressionCodecFactory::instance();
        CompressionCodecPtr res = factory.getDefaultCodec();

        for (const auto & element : elements)
            if (element.check(part_size, part_size_ratio))
                res = factory.get(element.family_name, element.level);

        return res;
    }
};

}
