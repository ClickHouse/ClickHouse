OWNER(g:clickhouse)

LIBRARY()

ADDINCL(GLOBAL clickhouse/base/widechar_width)

CFLAGS(-g0)

SRCS(
    widechar_width.cpp
)

END()
