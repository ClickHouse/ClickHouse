#pragma once

/** If there are --password=... or --password ... arguments in command line, replace their values with zero bytes.
  * This is needed to prevent password exposure in 'ps' and similar tools.
  */
bool isIdentChar(char c);
char * mask(char * begin, char * end);
void shrederSecretInQuery(char* query);
void clearPasswordFromCommandLine(int argc, char ** argv);
