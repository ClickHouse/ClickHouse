#pragma once

/** If there are --password=... or --password ... arguments in command line, replace their values with zero bytes.
  * This is needed to prevent password exposure in 'ps' and similar tools.
  */
void clearPasswordFromCommandLine(int argc, char ** argv);
