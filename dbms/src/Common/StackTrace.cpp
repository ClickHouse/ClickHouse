#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <execinfo.h>
#include <cxxabi.h>
#include <string.h>

#include <sstream>

#include <Common/StackTrace.h>


StackTrace::StackTrace()
{
    frames_size = backtrace(frames, STACK_TRACE_MAX_DEPTH);
}

std::string StackTrace::toString() const
{
    char ** symbols = backtrace_symbols(frames, frames_size);
    std::stringstream res;

    if (!symbols)
        return "Cannot get symbols for stack trace.\n";

    try
    {
        for (size_t i = 0, size = frames_size; i < size; ++i)
        {
            /// We do "demangling" of names. The name is in parenthesis, before the '+' character.

            char * name_start = nullptr;
            char * name_end = nullptr;
            char * demangled_name = nullptr;
            int status = 0;

            if (nullptr != (name_start = strchr(symbols[i], '('))
                && nullptr != (name_end = strchr(name_start, '+')))
            {
                ++name_start;
                *name_end = '\0';
                demangled_name = abi::__cxa_demangle(name_start, 0, 0, &status);
                *name_end = '+';
            }

            try
            {
                res << i << ". ";

                if (nullptr != demangled_name && 0 == status)
                {
                    res.write(symbols[i], name_start - symbols[i]);
                    res << demangled_name << name_end;
                }
                else
                    res << symbols[i];

                res << std::endl;
            }
            catch (...)
            {
                free(demangled_name);
                throw;
            }
            free(demangled_name);
        }
    }
    catch (...)
    {
        free(symbols);
        throw;
    }

    free(symbols);
    return res.str();
}
