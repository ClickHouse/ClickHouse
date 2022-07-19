#include <string.h>
#include "clearPasswordFromCommandLine.h"

void clearPasswordFromCommandLine(int argc, char ** argv)
{
    for (int arg = 1; arg < argc; ++arg)
    {
        if (arg + 1 < argc && 0 == strcmp(argv[arg], "--password"))
        {
            ++arg;
            memset(argv[arg], 0, strlen(argv[arg]));
        }
        else if (0 == strncmp(argv[arg], "--password=", strlen("--password=")))
        {
            memset(argv[arg] + strlen("--password="), 0, strlen(argv[arg]) - strlen("--password="));
        }
    }
}
