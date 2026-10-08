#pragma once


#include <string_view>
#include <unordered_map>
#include <vector>
#include <boost/program_options.hpp>
#include <Common/StringHashForHeterogeneousLookup.h>
namespace DB
{

/*
 * This functor is used to parse command line arguments and replace dashes with underscores,
 * allowing options to be specified using either dashes or underscores.
 */
class OptionsAliasParser
{
public:
    using Option = boost::shared_ptr<boost::program_options::option_description>;

    explicit OptionsAliasParser(const boost::program_options::options_description & options);
    const Option * findOption(std::string_view name, bool allow_prefix = true) const;
    /*
     * Parses arguments by replacing dashes with underscores, and matches the resulting name with known options
     * Implements boost::program_options::ext_parser logic
     */
    std::pair<std::string, std::string> operator()(const std::string & token) const;

private:
    struct IndexedOption
    {
        const Option * option;
        bool is_primary;
    };

    const std::vector<Option> & registered_options;
    std::unordered_map<std::string, IndexedOption, StringHashForHeterogeneousLookup, std::equal_to<>> options_names;
};

}
