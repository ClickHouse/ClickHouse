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

/** Create dictionary according to its layout.
  */
class DictionaryFactory : private boost::noncopyable
{
public:

    static DictionaryFactory & instance();

    /// Create dictionary from AbstractConfiguration parsed
    /// from xml-file on filesystem.
    DictionaryPtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Context & context,
        bool check_source_config = false) const;

    /// Create dictionary from DDL-query
    DictionaryPtr create(const std::string & name,
        const ASTCreateQuery & ast,
        const Context & context) const;

    using Creator = std::function<DictionaryPtr(
        const std::string & name,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        DictionarySourcePtr source_ptr)>;

    bool isComplex(const std::string & layout_type) const;

    void registerLayout(const std::string & layout_type, Creator create_layout, bool is_complex);

private:
    using LayoutRegistry = std::unordered_map<std::string, Creator>;
    LayoutRegistry registered_layouts;
    using LayoutComplexity = std::unordered_map<std::string, bool>;
    LayoutComplexity layout_complexity;
};

}
