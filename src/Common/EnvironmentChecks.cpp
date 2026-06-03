#include <Common/EnvironmentChecks.h>
#include <Common/IO.h>

#include <fmt/format.h>

#include <csignal>
#include <csetjmp>
#include <cstdint>

#include <tuple>

#include <unistd.h>

#if !defined(USE_MUSL)
/// NOTE: We will migrate to full static linking or our own dynamic loader to make this code obsolete.
void checkHarmfulEnvironmentVariables(char ** argv)
{
    std::initializer_list<const char *> harmful_env_variables = {
        /// The list is a selection from "man ld-linux".
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "LD_ORIGIN_PATH",
        "LD_AUDIT",
        "LD_DYNAMIC_WEAK",
        /// The list is a selection from "man dyld" (osx).
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
        "DYLD_VERSIONED_LIBRARY_PATH",
        "DYLD_INSERT_LIBRARIES",
    };

    bool require_reexec = false;
    for (const auto * var : harmful_env_variables)
    {
        if (const char * value = getenv(var); value && value[0]) // NOLINT(concurrency-mt-unsafe)
        {
            /// NOTE: setenv() is used over unsetenv() since unsetenv() marked as harmful
            if (setenv(var, "", true)) // NOLINT(concurrency-mt-unsafe) // this is safe if not called concurrently
            {
                fmt::print(stderr, "Cannot override {} environment variable", var);
                _exit(1);
            }
            require_reexec = true;
        }
    }

    if (require_reexec)
    {
        /// Use execvp() over execv() to search in PATH.
        ///
        /// This should be safe, since:
        /// - if argv[0] is relative path - it is OK
        /// - if argv[0] has only basename, the it will search in PATH, like shell will do.
        ///
        /// Also note, that this (search in PATH) because there is no easy and
        /// portable way to get absolute path of argv[0].
        /// - on linux there is /proc/self/exec and AT_EXECFN
        /// - but on other OSes there is no such thing (especially on OSX).
        ///
        /// And since static linking will be done someday anyway,
        /// let's not pollute the code base with special cases.
        int error = execvp(argv[0], argv);
        _exit(error);
    }
}
#endif
