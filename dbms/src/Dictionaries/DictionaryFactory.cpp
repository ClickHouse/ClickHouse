#include "DictionaryFactory.h"

#include <memory>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

void DictionaryFactory::registerLayout(const std::string & layout_type, Creator create_layout)
{
    //LOG_DEBUG(log, "Register dictionary layout type `" + layout_type + "`");
    if (!registered_layouts.emplace(layout_type, std::move(create_layout)).second)
        throw Exception("DictionaryFactory: the layout name '" + layout_type + "' is not unique", ErrorCodes::LOGICAL_ERROR);
}


DictionaryPtr DictionaryFactory::create(
    const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, Context & context) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    const auto & layout_prefix = config_prefix + ".layout";
    config.keys(layout_prefix, keys);
    if (keys.size() != 1)
        throw Exception {name + ": element dictionary.layout should have exactly one child element",
                         ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

    const DictionaryStructure dict_struct {config, config_prefix + ".structure"};

    auto source_ptr = DictionarySourceFactory::instance().create(name, config, config_prefix + ".source", dict_struct, context);

    const auto & layout_type = keys.front();

    {
        const auto found = registered_layouts.find(layout_type);
        if (found != registered_layouts.end())
        {
            const auto & create_layout = found->second;
            return create_layout(name, dict_struct, config, config_prefix, std::move(source_ptr));
        }
    }

    throw Exception {name + ": unknown dictionary layout type: " + layout_type, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
}

}
