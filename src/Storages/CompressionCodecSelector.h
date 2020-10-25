#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
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
    CompressionCodecSelector() {}    /// Always returns the default method.

    CompressionCodecSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);

        for (const auto & name : keys)
        {
            if (!startsWith(name.data(), "case"))
                throw Exception("Unknown element in config: " + config_prefix + "." + name + ", must be 'case'", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

            elements.emplace_back(config, config_prefix + "." + name);
        }
    }

    CompressionCodecPtr choose(size_t part_size, double part_size_ratio) const
    {
        const auto & factory = CompressionCodecFactory::instance();
        CompressionCodecPtr res = factory.getDefaultCodec();

        for (const auto & element : elements)
            if (element.check(part_size, part_size_ratio))
                res = factory.get(element.family_name, element.level, false);

        return res;
    }
};

}
