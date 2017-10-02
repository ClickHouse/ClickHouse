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

thread_local std::stringstream stack_trace_stream;

std::string StackTrace::toString() const
{
    char ** symbols = backtrace_symbols(frames, frames_size);

    if (!symbols)
        return "Cannot get symbols for stack trace.\n";

    stack_trace_stream.str(std::string());

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
                stack_trace_stream << i << ". ";

                if (nullptr != demangled_name && 0 == status)
                {
                    stack_trace_stream.write(symbols[i], name_start - symbols[i]);
                    stack_trace_stream << demangled_name << name_end;
                }
                else
                    stack_trace_stream << symbols[i];

                stack_trace_stream << std::endl;
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
    return stack_trace_stream.str();
}
