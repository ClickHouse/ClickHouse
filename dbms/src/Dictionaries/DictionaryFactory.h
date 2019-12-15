#pragma once

#include "IDictionary.h"
#include "registerDictionaries.h"
#include <Parsers/ASTCreateQuery.h>


namespace Poco
{

namespace Util
{
    class AbstractConfiguration;
}

class Logger;

}


namespace DB
{

class Context;

class DictionaryFactory : private boost::noncopyable
{
public:

    static DictionaryFactory & instance();

    DictionaryPtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Context & context) const;

    DictionaryPtr create(const std::string & name,
        const ASTCreateQuery & ast,
        const Context & context) const;

    using Creator = std::function<DictionaryPtr(
        const std::string & name,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        DictionarySourcePtr source_ptr)>;

    bool isComplex(const std::string & layout_type) const { return layout_complexity.at(layout_type); }

    void registerLayout(const std::string & layout_type, Creator create_layout, bool is_complex);

private:
    using LayoutRegistry = std::unordered_map<std::string, Creator>;
    LayoutRegistry registered_layouts;
    using LayoutComplexity = std::unordered_map<std::string, bool>;
    LayoutComplexity layout_complexity;
};

}
