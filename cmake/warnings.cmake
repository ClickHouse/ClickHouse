# Our principle is to enable as many warnings as possible and always do it with "warnings as errors" flag.
#
# But it comes with some cost:
# - we have to disable some warnings in 3rd party libraries (they are located in "contrib" directory)
# - we have to include headers of these libraries as -isystem to avoid warnings from headers
#   (this is the same behaviour as if these libraries were located in /usr/include)
# - sometimes warnings from 3rd party libraries may come from macro substitutions in our code
#   and we have to wrap them with #pragma GCC/clang diagnostic ignored

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# Control maximum size of stack frames. It can be important if the code is run in fibers with small stack size.
# Only in release build because debug has too large stack frames.
if ((NOT CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG") AND (NOT SANITIZE) AND (NOT CMAKE_CXX_COMPILER_ID MATCHES "AppleClang"))
    add_warning(frame-larger-than=65536)
endif ()

if (COMPILER_CLANG)
    # Add some warnings that are not available even with -Wall -Wextra -Wpedantic.
    # We want to get everything out of the compiler for code quality.
    add_warning(everything)
    add_warning(pedantic)
    no_warning(zero-length-array)
    no_warning(c++98-compat-pedantic)
    no_warning(c++98-compat)
    no_warning(c++20-compat) # Use constinit in C++20 without warnings
    no_warning(conversion)
    no_warning(ctad-maybe-unsupported) # clang 9+, linux-only
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
    no_warning(vla)
    no_warning(weak-template-vtables)
    no_warning(weak-vtables)
    no_warning(thread-safety-negative) # experimental flag, too many false positives
    no_warning(enum-constexpr-conversion) # breaks magic-enum library in clang-16
    # TODO Enable conversion, sign-conversion, double-promotion warnings.
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
    # Warn about overriding virtual functions that are not marked with the override keyword
    add_cxx_compile_options(-Wsuggest-override)
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
    # Warn when a literal 0 is used as null pointer constant.
    add_cxx_compile_options(-Wzero-as-null-pointer-constant)

    # The following warnings are generally useful but had to be disabled because of compiler bugs with older GCCs.
    # XXX: We should try again on more recent GCCs (--> see CMake variable GCC_MINIMUM_VERSION).

    # gcc10 stuck with this option while compiling GatherUtils code, anyway there are builds with clang that will warn
    add_cxx_compile_options(-Wno-sequence-point)
    # gcc10 false positive with this warning in MergeTreePartition.cpp
    #     inlined from 'void writeHexByteLowercase(UInt8, void*)' at ../src/Common/hex.h:39:11,
    #     inlined from 'DB::String DB::MergeTreePartition::getID(const DB::Block&) const' at ../src/Storages/MergeTree/MergeTreePartition.cpp:85:30:
    #     ../contrib/libc-headers/x86_64-linux-gnu/bits/string_fortified.h:34:33: error: writing 2 bytes into a region of size 0 [-Werror=stringop-overflow=]
    #     34 |   return __builtin___memcpy_chk (__dest, __src, __len, __bos0 (__dest));
    # For some reason (bug in gcc?) macro 'GCC diagnostic ignored "-Wstringop-overflow"' doesn't help.
    add_cxx_compile_options(-Wno-stringop-overflow)
    # reinterpretAs.cpp:182:31: error: ‘void* memcpy(void*, const void*, size_t)’ copying an object of non-trivial type
    # ‘using ToFieldType = using FieldType = using UUID = struct StrongTypedef<wide::integer<128, unsigned int>, DB::UUIDTag>’
    # {aka ‘struct StrongTypedef<wide::integer<128, unsigned int>, DB::UUIDTag>’} from an array of ‘const char8_t’
    add_cxx_compile_options(-Wno-error=class-memaccess)
    # Maybe false positive...
    # In file included from /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/memory:673,
    # In function ‘void std::__1::__libcpp_operator_delete(_Args ...) [with _Args = {void*, long unsigned int}]’,
    # inlined from ‘void std::__1::__do_deallocate_handle_size(void*, size_t, _Args ...) [with _Args = {}]’ at /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/new:271:34,
    # inlined from ‘void std::__1::__libcpp_deallocate(void*, size_t, size_t)’ at /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/new:285:41,
    # inlined from ‘constexpr void std::__1::allocator<_Tp>::deallocate(_Tp*, size_t) [with _Tp = char]’ at /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/memory:849:39,
    # inlined from ‘static constexpr void std::__1::allocator_traits<_Alloc>::deallocate(std::__1::allocator_traits<_Alloc>::allocator_type&, std::__1::allocator_traits<_Alloc>::pointer, std::__1::allocator_traits<_Alloc>::size_type) [with _Alloc = std::__1::allocator<char>]’ at /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/__memory/allocator_traits.h:476:24,
    # inlined from ‘std::__1::basic_string<_CharT, _Traits, _Allocator>::~basic_string() [with _CharT = char; _Traits = std::__1::char_traits<char>; _Allocator = std::__1::allocator<char>]’ at /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/string:2219:35,
    # inlined from ‘std::__1::basic_string<_CharT, _Traits, _Allocator>::~basic_string() [with _CharT = char; _Traits = std::__1::char_traits<char>; _Allocator = std::__1::allocator<char>]’ at /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/string:2213:1,
    # inlined from ‘DB::JSONBuilder::JSONMap::Pair::~Pair()’ at /home/jakalletti/ClickHouse/ClickHouse/src/Common/JSONBuilder.h:90:12,
    # inlined from ‘void DB::JSONBuilder::JSONMap::add(std::__1::string, DB::JSONBuilder::ItemPtr)’ at /home/jakalletti/ClickHouse/ClickHouse/src/Common/JSONBuilder.h:97:68,
    # inlined from ‘virtual void DB::ExpressionStep::describeActions(DB::JSONBuilder::JSONMap&) const’ at /home/jakalletti/ClickHouse/ClickHouse/src/Processors/QueryPlan/ExpressionStep.cpp:102:12:
    # /home/jakalletti/ClickHouse/ClickHouse/contrib/libcxx/include/new:247:20: error: ‘void operator delete(void*, size_t)’ called on a pointer to an unallocated object ‘7598543875853023301’ [-Werror=free-nonheap-object]
    add_cxx_compile_options(-Wno-error=free-nonheap-object)
    # AggregateFunctionAvg.h:203:100: error: ‘this’ pointer is null [-Werror=nonnull]
    add_cxx_compile_options(-Wno-error=nonnull)
endif ()
