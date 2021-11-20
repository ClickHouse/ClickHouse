# Our principle is to enable as many warnings as possible and always do it with "warnings as errors" flag.
#
# But it comes with some cost:
# - we have to disable some warnings in 3rd party libraries (they are located in "contrib" directory)
# - we have to include headers of these libraries as -isystem to avoid warnings from headers
#   (this is the same behaviour as if these libraries were located in /usr/include)
# - sometimes warnings from 3rd party libraries may come from macro substitutions in our code
#   and we have to wrap them with #pragma GCC/clang diagnostic ignored

if (NOT MSVC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif ()

# Add some warnings that are not available even with -Wall -Wextra -Wpedantic.
# Intended for exploration of new compiler warnings that may be found useful.
# Applies to clang only
option (WEVERYTHING "Enable -Weverything option with some exceptions." ON)

# Control maximum size of stack frames. It can be important if the code is run in fibers with small stack size.
# Only in release build because debug has too large stack frames.
if ((NOT CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG") AND (NOT SANITIZE) AND (NOT CMAKE_CXX_COMPILER_ID MATCHES "AppleClang"))
    add_warning(frame-larger-than=65536)
endif ()

if (COMPILER_CLANG)
    add_warning(pedantic)
    no_warning(vla-extension)
    no_warning(zero-length-array)
    no_warning(c11-extensions)

    add_warning(comma)
    add_warning(conditional-uninitialized)
    add_warning(covered-switch-default)
    add_warning(deprecated)
    add_warning(embedded-directive)
    add_warning(empty-init-stmt) # linux-only
    add_warning(extra-semi-stmt) # linux-only
    add_warning(extra-semi)
    add_warning(gnu-case-range)
    add_warning(inconsistent-missing-destructor-override)
    add_warning(newline-eof)
    add_warning(old-style-cast)
    add_warning(range-loop-analysis)
    add_warning(redundant-parens)
    add_warning(reserved-id-macro)
    add_warning(shadow-field) # clang 8+
    add_warning(shadow-uncaptured-local)
    add_warning(shadow)
    add_warning(string-plus-int) # clang 8+
    add_warning(undef)
    add_warning(unreachable-code-return)
    add_warning(unreachable-code)
    add_warning(unused-exception-parameter)
    add_warning(unused-macros)
    add_warning(unused-member-function)
    # XXX: libstdc++ has some of these for 3way compare
    if (USE_LIBCXX)
        add_warning(zero-as-null-pointer-constant)
    endif()

    if (WEVERYTHING)
        add_warning(everything)
        no_warning(c++98-compat-pedantic)
        no_warning(c++98-compat)
        no_warning(c99-extensions)
        no_warning(conversion)
        no_warning(ctad-maybe-unsupported) # clang 9+, linux-only
        no_warning(deprecated-dynamic-exception-spec)
        no_warning(disabled-macro-expansion)
        no_warning(documentation-unknown-command)
        no_warning(double-promotion)
        no_warning(exit-time-destructors)
        no_warning(float-equal)
        no_warning(global-constructors)
        no_warning(missing-prototypes)
        no_warning(missing-variable-declarations)
        no_warning(nested-anon-types)
        no_warning(packed)
        no_warning(padded)
        no_warning(return-std-move-in-c++11) # clang 7+
        no_warning(shift-sign-overflow)
        no_warning(sign-conversion)
        no_warning(switch-enum)
        no_warning(undefined-func-template)
        no_warning(unused-template)
        no_warning(vla)
        no_warning(weak-template-vtables)
        no_warning(weak-vtables)

        # XXX: libstdc++ has some of these for 3way compare
        if (NOT USE_LIBCXX)
            no_warning(zero-as-null-pointer-constant)
        endif()

        # TODO Enable conversion, sign-conversion, double-promotion warnings.
    endif ()
elseif (COMPILER_GCC)
    # Add compiler options only to c++ compiler
    function(add_cxx_compile_options option)
        add_compile_options("$<$<STREQUAL:$<TARGET_PROPERTY:LINKER_LANGUAGE>,CXX>:${option}>")
    endfunction()
    # Warn about boolean expression compared with an integer value different from true/false
    add_cxx_compile_options(-Wbool-compare)
    # Warn whenever a pointer is cast such that the required alignment of the target is increased.
    add_cxx_compile_options(-Wcast-align)
    # Warn whenever a pointer is cast so as to remove a type qualifier from the target type.
    add_cxx_compile_options(-Wcast-qual)
    # Warn when deleting a pointer to incomplete type, which may cause undefined behavior at runtime
    add_cxx_compile_options(-Wdelete-incomplete)
    # Warn if a requested optimization pass is disabled. Code is too big or too complex
    add_cxx_compile_options(-Wdisabled-optimization)
    # Warn about duplicated conditions in an if-else-if chain
    add_cxx_compile_options(-Wduplicated-cond)
    # Warn about a comparison between values of different enumerated types
    add_cxx_compile_options(-Wenum-compare)
    # Warn about uninitialized variables that are initialized with themselves
    add_cxx_compile_options(-Winit-self)
    # Warn about logical not used on the left hand side operand of a comparison
    add_cxx_compile_options(-Wlogical-not-parentheses)
    # Warn about suspicious uses of logical operators in expressions
    add_cxx_compile_options(-Wlogical-op)
    # Warn if there exists a path from the function entry to a use of the variable that is uninitialized.
    add_cxx_compile_options(-Wmaybe-uninitialized)
    # Warn when the indentation of the code does not reflect the block structure
    add_cxx_compile_options(-Wmisleading-indentation)
    # Warn if a global function is defined without a previous declaration - disabled because of build times
    # add_cxx_compile_options(-Wmissing-declarations)
    # Warn if a user-supplied include directory does not exist
    add_cxx_compile_options(-Wmissing-include-dirs)
    # Obvious
    add_cxx_compile_options(-Wnon-virtual-dtor)
    # Obvious
    add_cxx_compile_options(-Wno-return-local-addr)
    # This warning is disabled due to false positives if compiled with libc++: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=90037
    #add_cxx_compile_options(-Wnull-dereference)
    # Obvious
    add_cxx_compile_options(-Wodr)
    # Obvious
    add_cxx_compile_options(-Wold-style-cast)
    # Warn when a function declaration hides virtual functions from a base class
    # add_cxx_compile_options(-Woverloaded-virtual)
    # Warn about placement new expressions with undefined behavior
    add_cxx_compile_options(-Wplacement-new=2)
    # Warn about anything that depends on the “size of” a function type or of void
    add_cxx_compile_options(-Wpointer-arith)
    # Warn if anything is declared more than once in the same scope
    add_cxx_compile_options(-Wredundant-decls)
    # Member initialization reordering
    add_cxx_compile_options(-Wreorder)
    # Obvious
    add_cxx_compile_options(-Wshadow)
    # Warn if left shifting a negative value
    add_cxx_compile_options(-Wshift-negative-value)
    # Warn about a definition of an unsized deallocation function
    add_cxx_compile_options(-Wsized-deallocation)
    # Warn when the sizeof operator is applied to a parameter that is declared as an array in a function definition
    add_cxx_compile_options(-Wsizeof-array-argument)
    # Warn for suspicious length parameters to certain string and memory built-in functions if the argument uses sizeof
    add_cxx_compile_options(-Wsizeof-pointer-memaccess)

    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 9)
        # Warn about overriding virtual functions that are not marked with the override keyword
        add_cxx_compile_options(-Wsuggest-override)
    endif ()

    # Warn whenever a switch statement has an index of boolean type and the case values are outside the range of a boolean type
    add_cxx_compile_options(-Wswitch-bool)
    # Warn if a self-comparison always evaluates to true or false
    add_cxx_compile_options(-Wtautological-compare)
    # Warn about trampolines generated for pointers to nested functions
    add_cxx_compile_options(-Wtrampolines)
    # Obvious
    add_cxx_compile_options(-Wunused)
    add_cxx_compile_options(-Wundef)
    # Warn if vector operation is not implemented via SIMD capabilities of the architecture
    add_cxx_compile_options(-Wvector-operation-performance)
    # XXX: libstdc++ has some of these for 3way compare
    if (USE_LIBCXX)
        # Warn when a literal 0 is used as null pointer constant.
        add_cxx_compile_options(-Wzero-as-null-pointer-constant)
    endif()

    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 10)
        # XXX: gcc10 stuck with this option while compiling GatherUtils code
        # (anyway there are builds with clang, that will warn)
        add_cxx_compile_options(-Wno-sequence-point)
        # XXX: gcc10 false positive with this warning in MergeTreePartition.cpp
        #     inlined from 'void writeHexByteLowercase(UInt8, void*)' at ../src/Common/hex.h:39:11,
        #     inlined from 'DB::String DB::MergeTreePartition::getID(const DB::Block&) const' at ../src/Storages/MergeTree/MergeTreePartition.cpp:85:30:
        #     ../contrib/libc-headers/x86_64-linux-gnu/bits/string_fortified.h:34:33: error: writing 2 bytes into a region of size 0 [-Werror=stringop-overflow=]
        #     34 |   return __builtin___memcpy_chk (__dest, __src, __len, __bos0 (__dest));
        # For some reason (bug in gcc?) macro 'GCC diagnostic ignored "-Wstringop-overflow"' doesn't help.
        add_cxx_compile_options(-Wno-stringop-overflow)
    endif()
endif ()
