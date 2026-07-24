#pragma once

#include <Interpreters/Context_fwd.h>
#include <Dictionaries/IDictionary.h>


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
        ContextPtr global_context,
        bool created_from_ddl) const;

    using LayoutCreateFunction = std::function<DictionaryPtr(
        const std::string & name,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        DictionarySourcePtr source_ptr,
        ContextPtr global_context,
        bool created_from_ddl)>;

    bool isComplex(const std::string & layout_type) const;

    /// If the argument `layout_type` is not complex layout and has corresponding complex layout,
    /// change `layout_type` to corresponding complex and return true; otherwise do nothing and return false.
    bool convertToComplex(std::string & layout_type) const;

    void registerLayout(const std::string & layout_type, LayoutCreateFunction create_layout, bool is_layout_complex, bool has_layout_complex = true);

private:
    struct RegisteredLayout
    {
        LayoutCreateFunction layout_create_function;
        bool is_layout_complex;
        bool has_layout_complex;
    };

    using LayoutRegistry = std::unordered_map<std::string, RegisteredLayout>;
    LayoutRegistry registered_layouts;

};

}
