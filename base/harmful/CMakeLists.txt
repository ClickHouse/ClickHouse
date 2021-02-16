# Some strage segfaults if linked
if(NOT WITH_COVERAGE)
    add_library(harmful harmful.c)
    install(TARGETS harmful EXPORT global ARCHIVE DESTINATION lib)
endif()
