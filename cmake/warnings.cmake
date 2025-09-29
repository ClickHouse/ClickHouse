# Our principle is to enable as many warnings as possible and always do it with "warnings as errors" flag.
#
# But it comes with some cost:
# - we have to disable some warnings in 3rd party libraries (they are located in "contrib" directory)
# - we have to include headers of these libraries as -isystem to avoid warnings from headers
#   (this is the same behaviour as if these libraries were located in /usr/include)
# - sometimes warnings from 3rd party libraries may come from macro substitutions in our code
#   and we have to wrap them with #pragma clang diagnostic ignored

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# Control maximum size of stack frames. It can be important if the code is run in fibers with small stack size.
# Only in release build because debug has too large stack frames.
if ((NOT CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG") AND (NOT SANITIZE))
    add_warning(frame-larger-than=65536)
endif ()

# Add some warnings that are not available even with -Wall -Wextra -Wpedantic.
# We want to get everything out of the compiler for code quality.
add_warning(everything)
add_warning(pedantic)
no_warning(return-type-c-linkage) # Used in some 3rd party libraries like delta-kernel-rs ffi
no_warning(zero-length-array) # Clang extension
no_warning(c++98-compat-pedantic) # We don't care about C++98 compatibility (We use aliases, variadic macros...)
no_warning(c++20-compat) # Use C++20 features incompatible with older standards (consteval, constinit, implicit typename...)
no_warning(sign-conversion) # TODO: Fix the code and enable it
no_warning(implicit-int-conversion) # TODO: Fix the code and enable it
no_warning(implicit-int-float-conversion) # TODO: Fix the code and enable it
no_warning(disabled-macro-expansion)
no_warning(documentation-unknown-command)
no_warning(double-promotion)
no_warning(exit-time-destructors)
no_warning(float-equal)
no_warning(global-constructors)
no_warning(missing-prototypes)
no_warning(missing-variable-declarations)
no_warning(padded)
no_warning(switch-enum)
no_warning(undefined-func-template)
no_warning(unused-template)
no_warning(weak-template-vtables)
no_warning(weak-vtables)
no_warning(thread-safety-negative) # experimental flag, too many false positives
no_warning(unsafe-buffer-usage) # too aggressive
no_warning(switch-default) # conflicts with "defaults in a switch covering all enum values"
no_warning(nrvo) # not eliding copy on return - too aggressive
no_warning(missing-noreturn) # too aggressive with no clear benefit, see https://github.com/ClickHouse/ClickHouse/pull/86416
