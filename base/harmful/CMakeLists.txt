# Segfaults if linked with coverage runtime
if(NOT WITH_COVERAGE)
    add_library(harmful harmful.c)
    install(TARGETS harmful EXPORT global ARCHIVE DESTINATION lib)
endif()
