#pragma once

#if __has_include(<common/config_common.h>)
#include <common/config_common.h>
#endif

/// Different line editing libraries can be used depending on the environment.
#if USE_READLINE
    #include <readline/readline.h>
    #include <readline/history.h>
#elif USE_LIBEDIT
    #include <editline/readline.h>
    #include <editline/history.h> // Y_IGNORE
#else
    #include <string>
    #include <cstring>
    #include <iostream>
    inline char * readline(const char * prompt)
    {
        std::string s;
        std::cout << prompt;
        std::getline(std::cin, s);

        if (!std::cin.good())
            return nullptr;
        return strdup(s.data());
    }
    #define add_history(...) do {} while (0);
    #define rl_bind_key(...) do {} while (0);
#endif
