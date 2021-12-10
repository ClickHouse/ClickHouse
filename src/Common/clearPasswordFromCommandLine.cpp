#include <string.h>
#include <string_view>
#include <Common/clearPasswordFromCommandLine.h>

using namespace std::literals;

void clearPasswordFromCommandLine(int argc, char ** argv)
{
    for (int arg = 1; arg < argc; ++arg)
    {
        if (arg + 1 < argc && argv[arg] == "--password"sv)
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
