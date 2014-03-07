#include <malloc.h>
#include <execinfo.h>
#include <cxxabi.h>
#include <string.h>

#include <sstream>

#include <DB/Core/StackTrace.h>


#define DBMS_STACK_TRACE_MAX_DEPTH 32


namespace DB
{

StackTrace::StackTrace()
{
	frames_size = backtrace(frames, DBMS_STACK_TRACE_MAX_DEPTH);
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
			/// Делаем demangling имён. Имя находится в скобках, до символа '+'.
			
			char * name_start = NULL;
			char * name_end = NULL;
			char * demangled_name = NULL;
			int status = 0;
			
			if (NULL != (name_start = strchr(symbols[i], '('))
				&& NULL != (name_end = strchr(name_start, '+')))
			{
				++name_start;
				*name_end = '\0';
				demangled_name = abi::__cxa_demangle(name_start, 0, 0, &status);
				*name_end = '+';
			}

			try
			{
				res << i << ". ";
				
				if (NULL != demangled_name && 0 == status)
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

}
