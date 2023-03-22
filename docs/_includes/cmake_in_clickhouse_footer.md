
## Developer's guide for adding new CMake options

### Don't be obvious. Be informative.

Bad:
```cmake
option (ENABLE_TESTS "Enables testing" OFF)
```

This description is quite useless as is neither gives the viewer any additional information nor explains the option purpose.

Better:

```cmake
option(ENABLE_TESTS "Provide unit_test_dbms target with Google.test unit tests" OFF)
```

If the option's purpose can't be guessed by its name, or the purpose guess may be misleading, or option has some
pre-conditions, leave a comment above the `option()` line and explain what it does.
The best way would be linking the docs page (if it exists).
The comment is parsed into a separate column (see below).

Even better:

```cmake
# implies ${TESTS_ARE_ENABLED}
# see tests/CMakeLists.txt for implementation detail.
option(ENABLE_TESTS "Provide unit_test_dbms target with Google.test unit tests" OFF)
```

### If the option's state could produce unwanted (or unusual) result, explicitly warn the user.

Suppose you have an option that may strip debug symbols from the ClickHouse's part.
This can speed up the linking process, but produces a binary that cannot be debugged.
In that case, prefer explicitly raising a warning telling the developer that he may be doing something wrong.
Also, such options should be disabled if applies.

Bad:
```cmake
option(STRIP_DEBUG_SYMBOLS_FUNCTIONS
    "Do not generate debugger info for ClickHouse functions.
    ${STRIP_DSF_DEFAULT})

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif()

```
Better:

```cmake
# Provides faster linking and lower binary size.
# Tradeoff is the inability to debug some source files with e.g. gdb
# (empty stack frames and no local variables)."
option(STRIP_DEBUG_SYMBOLS_FUNCTIONS
    "Do not generate debugger info for ClickHouse functions."
    ${STRIP_DSF_DEFAULT})

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    message(WARNING "Not generating debugger info for ClickHouse functions")
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif()
```

### In the option's description, explain WHAT the option does rather than WHY it does something.

The WHY explanation should be placed in the comment.
You may find that the option's name is self-descriptive.

Bad:

```cmake
option(ENABLE_THINLTO "Enable Thin LTO. Only applicable for clang. It's also suppressed when building with tests or sanitizers." ON)
```

Better:

```cmake
# Only applicable for clang.
# Turned off when building with tests or sanitizers.
option(ENABLE_THINLTO "Clang-specific link time optimisation" ON).
```

### Don't assume other developers know as much as you do.

In ClickHouse, there are many tools used that an ordinary developer may not know. If you are in doubt, give a link to
the tool's docs. It won't take much of your time.

Bad:

```cmake
option(ENABLE_THINLTO "Enable Thin LTO. Only applicable for clang. It's also suppressed when building with tests or sanitizers." ON)
```

Better (combined with the above hint):

```cmake
# https://clang.llvm.org/docs/ThinLTO.html
# Only applicable for clang.
# Turned off when building with tests or sanitizers.
option(ENABLE_THINLTO "Clang-specific link time optimisation" ON).
```

Other example, bad:

```cmake
option (USE_INCLUDE_WHAT_YOU_USE "Use 'include-what-you-use' tool" OFF)
```

Better:

```cmake
# https://github.com/include-what-you-use/include-what-you-use
option (USE_INCLUDE_WHAT_YOU_USE "Reduce unneeded #include s (external tool)" OFF)
```

### Prefer consistent default values.

CMake allows you to pass a plethora of values representing boolean `true/false`, e.g. `1, ON, YES, ...`.
Prefer the `ON/OFF` values, if possible.
