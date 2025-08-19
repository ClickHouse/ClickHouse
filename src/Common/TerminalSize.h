#pragma once

#include <string>
#include <unistd.h>
#include <boost/program_options.hpp>


namespace po = boost::program_options;


uint16_t getTerminalWidth(int in_fd = STDIN_FILENO, int err_fd = STDERR_FILENO);

/** Creates po::options_description with name and an appropriate size for option displaying
 *  when program is called with option --help
 * */
po::options_description createOptionsDescription(const std::string &caption, unsigned short terminal_width); /// NOLINT
