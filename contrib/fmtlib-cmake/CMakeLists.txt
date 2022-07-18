set (SRCS
    # NOTE: do not build module for now:
    # ../fmtlib/src/fmt.cc
    ../fmtlib/src/format.cc
    ../fmtlib/src/os.cc

    ../fmtlib/include/fmt/args.h
    ../fmtlib/include/fmt/chrono.h
    ../fmtlib/include/fmt/color.h
    ../fmtlib/include/fmt/compile.h
    ../fmtlib/include/fmt/core.h
    ../fmtlib/include/fmt/format.h
    ../fmtlib/include/fmt/format-inl.h
    ../fmtlib/include/fmt/locale.h
    ../fmtlib/include/fmt/os.h
    ../fmtlib/include/fmt/ostream.h
    ../fmtlib/include/fmt/printf.h
    ../fmtlib/include/fmt/ranges.h
    ../fmtlib/include/fmt/xchar.h
)

add_library(_fmt ${SRCS})
target_include_directories(_fmt SYSTEM PUBLIC ../fmtlib/include)
add_library(ch_contrib::fmt ALIAS _fmt)
