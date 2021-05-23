SET (USE_INTERNAL_LZSSE_LIBRARY 1)
SET (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/lzsse")

SET (Sources
    "${LIBRARY_DIR}/lzsse2/lzsse2.cpp"
    "${LIBRARY_DIR}/lzsse4/lzsse4.cpp"
    "${LIBRARY_DIR}/lzsse8/lzsse8.cpp"
)

SET (Headers
    "${LIBRARY_DIR}/lzsse2/lzsse2.h"
    "${LIBRARY_DIR}/lzsse4/lzsse4.h"
    "${LIBRARY_DIR}/lzsse8/lzsse8.h"
    "${LIBRARY_DIR}/lzsse2/lzsse2_platform.h"
    "${LIBRARY_DIR}/lzsse4/lzsse4_platform.h"
    "${LIBRARY_DIR}/lzsse8/lzsse8_platform.h"
)

ADD_LIBRARY(lzsse ${Sources} ${Headers})

target_include_directories (lzsse SYSTEM PUBLIC ${LIBRARY_DIR})
