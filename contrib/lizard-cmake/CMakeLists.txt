SET (USE_INTERNAL_LIZARD_LIBRARY 1)
SET (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/lizard")

SET (Sources
    "${LIBRARY_DIR}/lib/lizard_decompress.c"
    "${LIBRARY_DIR}/lib/lizard_compress.c"
)

SET (Headers
    "${LIBRARY_DIR}/lib/lizard_compress.h"
    "${LIBRARY_DIR}/lib/lizard_common.h"
)

ADD_LIBRARY(lizard ${Sources} ${Headers})

target_include_directories (lizard PUBLIC ${LIBRARY_DIR})
