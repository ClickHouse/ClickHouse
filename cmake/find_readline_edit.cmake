set (READLINE_PATHS "/usr/local/opt/readline/lib")
# First try find custom lib for macos users (default lib without history support)
if (USE_STATIC_LIBRARIES)
	find_library (READLINE_LIB NAMES libreadline.a PATHS ${READLINE_PATHS} NO_DEFAULT_PATH)
else ()
	find_library (READLINE_LIB NAMES readline PATHS ${READLINE_PATHS} NO_DEFAULT_PATH)
endif ()
if (NOT READLINE_LIB)
	if (USE_STATIC_LIBRARIES)
		find_library (READLINE_LIB NAMES libreadline.a PATHS ${READLINE_PATHS})
	else ()
		find_library (READLINE_LIB NAMES readline PATHS ${READLINE_PATHS})
	endif ()
endif ()

if (USE_STATIC_LIBRARIES)
	find_library (TERMCAP_LIB NAMES libtermcap.a termcap)
else ()
	find_library (TERMCAP_LIB NAMES termcap)
endif ()

if (USE_STATIC_LIBRARIES)
	find_library (EDIT_LIB NAMES libedit.a)
else ()
	find_library (EDIT_LIB NAMES edit)
endif ()

set(READLINE_INCLUDE_PATHS "/usr/local/opt/readline/include")
if (READLINE_LIB)
	find_path (READLINE_INCLUDE_DIR NAMES readline/readline.h PATHS ${READLINE_INCLUDE_PATHS} NO_DEFAULT_PATH)
	if (NOT READLINE_INCLUDE_DIR)
		find_path (READLINE_INCLUDE_DIR NAMES readline/readline.h PATHS ${READLINE_INCLUDE_PATHS})
	endif ()
	set (USE_READLINE 1)
	set (LINE_EDITING_LIBS ${READLINE_LIB} ${TERMCAP_LIB})
	message (STATUS "Using line editing libraries (readline): ${READLINE_INCLUDE_DIR} : ${LINE_EDITING_LIBS}")
elseif (EDIT_LIB)
	if (USE_STATIC_LIBRARIES)
		find_library (CURSES_LIB NAMES libcurses.a)
	else ()
		find_library (CURSES_LIB NAMES curses)
	endif ()
	set(USE_LIBEDIT 1)
	find_path (READLINE_INCLUDE_DIR NAMES editline/readline.h PATHS ${READLINE_INCLUDE_PATHS})
	set (LINE_EDITING_LIBS ${EDIT_LIB} ${CURSES_LIB} ${TERMCAP_LIB})
	message (STATUS "Using line editing libraries (edit): ${READLINE_INCLUDE_DIR} : ${LINE_EDITING_LIBS}")
else ()
	message (STATUS "Not using any library for line editing.")
endif ()
if (READLINE_INCLUDE_DIR)
	include_directories (${READLINE_INCLUDE_DIR})
endif ()

include (CheckCXXSourceRuns)

set (CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} ${LINE_EDITING_LIBS})
check_cxx_source_runs("
	#include <readline/readline.h>
	#include <readline/history.h>
	int main() {
		add_history(nullptr);
		append_history(1,nullptr);
	}
" HAVE_READLINE_HISTORY)

#if (HAVE_READLINE_HISTORY)
#	add_definitions (-D HAVE_READLINE_HISTORY)
#endif ()
