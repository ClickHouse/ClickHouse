#include <Dictionaries/DictionaryFactory.h>

#include <memory>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

void DictionaryFactory::registerLayout(const std::string & layout_type, LayoutCreateFunction create_layout, bool is_layout_complex, bool has_layout_complex)
{
    auto it = registered_layouts.find(layout_type);
    if (it != registered_layouts.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DictionaryFactory: the layout name '{}' is not unique", layout_type);

    RegisteredLayout layout { .layout_create_function = create_layout, .is_layout_complex = is_layout_complex, .has_layout_complex = has_layout_complex };
    registered_layouts.emplace(layout_type, std::move(layout));
}

DictionaryPtr DictionaryFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr global_context,
    bool created_from_ddl) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    const auto & layout_prefix = config_prefix + ".layout";
    config.keys(layout_prefix, keys);
    if (keys.size() != 1)
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
            "{}: element dictionary.layout should have exactly one child element",
            name);

    const DictionaryStructure dict_struct{config, config_prefix};

    DictionarySourcePtr source_ptr = DictionarySourceFactory::instance().create(
        name, config, config_prefix + ".source", dict_struct, global_context, config.getString(config_prefix + ".database", ""), created_from_ddl);
    LOG_TRACE(getLogger("DictionaryFactory"), "Created dictionary source '{}' for dictionary '{}'", source_ptr->toString(), name);

    const auto & layout_type = keys.front();

    {
        const auto found = registered_layouts.find(layout_type);
        if (found != registered_layouts.end())
        {
            const auto & layout_creator = found->second.layout_create_function;
            return layout_creator(name, dict_struct, config, config_prefix, std::move(source_ptr), global_context, created_from_ddl);
        }
    }

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
        "{}: unknown dictionary layout type: {}",
        name,
        layout_type);
}

bool DictionaryFactory::isComplex(const std::string & layout_type) const
{
    auto it = registered_layouts.find(layout_type);

    if (it == registered_layouts.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
           "Unknown dictionary layout type: {}",
           layout_type);
    }

    return it->second.is_layout_complex;
}

bool DictionaryFactory::convertToComplex(std::string & layout_type) const
{
    auto it = registered_layouts.find(layout_type);

    if (it == registered_layouts.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "Unknown dictionary layout type: {}",
                        layout_type);
    }

    if (!it->second.is_layout_complex && it->second.has_layout_complex)
    {
        layout_type = "complex_key_" + layout_type;
        return true;
    }
    return false;
}


DictionaryFactory & DictionaryFactory::instance()
{
    static DictionaryFactory ret;
    return ret;
}

}
