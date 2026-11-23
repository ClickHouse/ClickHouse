#pragma once

/** Replace with zero bytes the values of the options carrying credentials (`--password`, `--jwt`, ...), given as
  * `--option value`, `--option=value`, `--option = value` or `--option:value`, to prevent their exposure in `ps` and similar tools.
  */
void clearPasswordFromCommandLine(int argc, char ** argv);
