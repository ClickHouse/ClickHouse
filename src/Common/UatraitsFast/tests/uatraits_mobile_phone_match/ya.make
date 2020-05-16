UNITTEST_FOR(metrika/core/libs/uatraits-fast)

OWNER(g:metrika-core)

PEERDIR(
    metrika/core/libs/statdaemons
    metrika/core/libs/uatraits-fast
    library/unittest
)

INCLUDE(${ARCADIA_ROOT}/metrika/core/include_dirs.inc)

ADDINCL(${ARCADIA_BUILD_ROOT}/metrika/core/libs/uatraits-fast/tests)

SRCDIR(metrika/core/libs/uatraits-fast/tests)

SRCS(
    mobile_phone_match.cpp
)

END()
