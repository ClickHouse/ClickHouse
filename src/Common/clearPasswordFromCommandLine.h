#pragma once

/** Replace with zero bytes the values of the options carrying credentials (`--password`, `--jwt`, ...), given as
  * `--option value`, `--option=value`, `--option = value` or `--option:value`, to prevent their exposure in `ps` and similar tools.
  */
bool isIdentChar(char c);
char * mask(char * begin, char * end);
void shrederSecretInQuery(char* query);
void clearPasswordFromCommandLine(int argc, char ** argv);
