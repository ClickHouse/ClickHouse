#pragma once


#include <unordered_set>
#include <boost/program_options.hpp>
namespace DB
{

/*
 * This functor is used to parse command line arguments and replace dashes with underscores,
 * allowing options to be specified using either dashes or underscores.
 */
class OptionsAliasParser
{
public:
    explicit OptionsAliasParser(const boost::program_options::options_description & options);
    /*
     * Parses arguments by replacing dashes with underscores, and matches the resulting name with known options
     * Implements boost::program_options::ext_parser logic
     */
    std::pair<std::string, std::string> operator()(const std::string & token) const;

private:
    std::unordered_set<std::string> options_names;
};

}
