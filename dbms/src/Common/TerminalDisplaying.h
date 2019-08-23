#pragma once

#include <iostream>
#include <string>
#include <boost/program_options.hpp>


namespace po = boost::program_options;


unsigned short int getTerminalWidth();

/** Sets a name and an appropriate size for option displaying
 *  when program is called with option --help
 * */
po::options_description setOptionsDescription(const std::string & caption, unsigned short terminal_width);

