#include "DictionaryFactory.h"

#include <memory>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "getDictionaryConfigurationFromAST.h"
#include <Interpreters/Context.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

void DictionaryFactory::registerLayout(const std::string & layout_type, Creator create_layout, bool is_complex)
{
    if (!registered_layouts.emplace(layout_type, std::move(create_layout)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DictionaryFactory: the layout name '{}' is not unique", layout_type);

    layout_complexity[layout_type] = is_complex;

}


DictionaryPtr DictionaryFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context,
    bool check_source_config) const
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
        name, config, config_prefix + ".source", dict_struct, context, config.getString(config_prefix + ".database", ""), check_source_config);
    LOG_TRACE(&Poco::Logger::get("DictionaryFactory"), "Created dictionary source '{}' for dictionary '{}'", source_ptr->toString(), name);

    if (context->hasQueryContext() && context->getSettingsRef().log_queries)
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Dictionary, name);

    const auto & layout_type = keys.front();

    {
        const auto found = registered_layouts.find(layout_type);
        if (found != registered_layouts.end())
        {
            const auto & layout_creator = found->second;
            return layout_creator(name, dict_struct, config, config_prefix, std::move(source_ptr));
        }
    }

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
        "{}: unknown dictionary layout type: {}",
        name,
        layout_type);
}

DictionaryPtr DictionaryFactory::create(const std::string & name, const ASTCreateQuery & ast, ContextPtr context) const
{
    auto configuration = getDictionaryConfigurationFromAST(ast, context);
    return DictionaryFactory::create(name, *configuration, "dictionary", context, true);
}

bool DictionaryFactory::isComplex(const std::string & layout_type) const
{
    auto found = layout_complexity.find(layout_type);

    if (found != layout_complexity.end())
        return found->second;

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
        "Unknown dictionary layout type: {}",
        layout_type);
}


DictionaryFactory & DictionaryFactory::instance()
{
    static DictionaryFactory ret;
    return ret;
}

}
