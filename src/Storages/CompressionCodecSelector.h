#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Compression/CompressionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}


/** Allows you to select the compression settings for the conditions specified in the configuration file.
  * The config looks like this

    <compression>

        <!-- Set of options. Options are checked in a row. The last worked option wins.
             If none has worked, the size-aware built-in default is used (see below). -->
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

  * The built-in default (used when no `<case>` matched, including when there is no `<compression>`
  * configuration at all) is size-aware: parts smaller than `min_part_size_for_default_codec` use the
  * faster `LZ4`, while larger parts use the default codec (`ZSTD(3)`). Freshly inserted parts, whose
  * final size is not yet known, are passed a size of `0` and therefore start as `LZ4`; the bigger parts
  * produced by background merges cross the threshold and switch to `ZSTD(3)`. This keeps compression
  * cheap for small, frequently rewritten data while getting the better ratio for the bulk of the data.
  */
class CompressionCodecSelector
{
private:
    /// Parts smaller than this use `LZ4` by the built-in default; larger parts use the default codec (`ZSTD(3)`).
    static constexpr size_t min_part_size_for_default_codec = 100 * 1024 * 1024;

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

        for (const auto & name : keys)
        {
            if (!startsWith(name, "case"))
                throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}.{}, must be 'case'", config_prefix, name);

            elements.emplace_back(config, config_prefix + "." + name);
        }
    }

    CompressionCodecPtr choose(size_t part_size, double part_size_ratio) const
    {
        const auto & factory = CompressionCodecFactory::instance();

        /// Size-aware built-in default: use the faster `LZ4` for small parts (where the ratio matters
        /// less and the data is frequently merged and read), and the default codec (`ZSTD(3)`) for larger
        /// parts (where the better ratio pays off). Explicit `<case>` rules below still take precedence.
        CompressionCodecPtr res = part_size < min_part_size_for_default_codec
            ? factory.get("LZ4", {})
            : factory.getDefaultCodec();

        for (const auto & element : elements)
            if (element.check(part_size, part_size_ratio))
                res = factory.get(element.family_name, element.level);

        return res;
    }
};

}
