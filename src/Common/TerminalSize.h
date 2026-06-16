#pragma once

#include <string>
#include <unistd.h>
#include <boost/program_options.hpp>


namespace po = boost::program_options;


std::pair<uint16_t, uint16_t> getTerminalSize(int in_fd = STDIN_FILENO, int err_fd = STDERR_FILENO);
uint16_t getTerminalWidth(int in_fd = STDIN_FILENO, int err_fd = STDERR_FILENO);

/** Returns true if the terminal character encoding is UTF-8, as determined by the
  * locale environment variables (`LC_ALL`, `LC_CTYPE`, `LANG`, in order of precedence).
  * When it returns false (e.g. with `LANG=C`), printing Unicode characters would corrupt the terminal.
  */
bool terminalSupportsUTF8();

/** Creates po::options_description with name and an appropriate size for option displaying
 *  when program is called with option --help
 * */
po::options_description createOptionsDescription(const std::string &caption, unsigned short terminal_width); /// NOLINT
