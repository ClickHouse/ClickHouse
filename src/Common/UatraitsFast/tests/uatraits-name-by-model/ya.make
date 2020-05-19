PROGRAM()

OWNER(g:metrika-core)

PEERDIR(
    metrika/core/libs/statdaemons
    metrika/core/libs/uatraits-fast
)

INCLUDE(${ARCADIA_ROOT}/metrika/core/include_dirs.inc)

SRCDIR(metrika/core/libs/uatraits-fast/tests)

SRCS(
    uatraits-name-by-model.cpp
)

END()
