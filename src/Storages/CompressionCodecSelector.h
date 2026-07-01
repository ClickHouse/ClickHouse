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

    CompressionCodecSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);

        const auto & factory = CompressionCodecFactory::instance();

        for (const auto & name : keys)
        {
            if (!startsWith(name, "case"))
                throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}.{}, must be 'case'", config_prefix, name);

            const Element & element = elements.emplace_back(config, config_prefix + "." + name);

            /// The server-wide `<compression>` selector builds a codec from just its family name and
            /// level, with no column type, and `choose` returns it as the part's default codec. The
            /// part writer then re-resolves that stored `CODEC(...)` description with each column type,
            /// bypassing the `allow_experimental_codecs` gate and the "requires column type" rejection
            /// that the SQL/untyped codec settings (`default_compression_codec`, `marks_compression_codec`,
            /// `primary_key_compression_codec`) enforce at validation time. Apply the same rejection here,
            /// at config load, so an experimental or type-dependent codec (such as `PCO`) cannot be used
            /// from server config and silently bypass that gate. Fail closed at load rather than at the
            /// first part write.
            CompressionCodecPtr codec = factory.get(element.family_name, element.level);
            if (codec->requiresColumnTypeToCompress())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codec {} requires the column type to compress and cannot be used in the <compression> configuration; "
                    "specify it per column instead", element.family_name);
            if (codec->isExperimental())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codec {} is experimental and cannot be used in the <compression> configuration. "
                    "You can enable it per column with the 'allow_experimental_codecs' setting", element.family_name);
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
