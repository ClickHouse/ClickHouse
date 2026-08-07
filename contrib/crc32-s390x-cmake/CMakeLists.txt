if(ARCH_S390X)
    option (ENABLE_CRC32_S390X "Enable crc32 on s390x platform" ON)
endif()

if (NOT ENABLE_CRC32_S390X)
    return()
endif()

set(CRC32_S390X_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc32-s390x)
set(CRC32_S390X_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc32-s390x)

set(CRC32_SRCS 
    "${CRC32_S390X_SOURCE_DIR}/crc32-s390x.c"
    "${CRC32_S390X_SOURCE_DIR}/crc32be-vx.S"
    "${CRC32_S390X_SOURCE_DIR}/crc32le-vx.S"
)

set(CRC32_HDRS
    "${CRC32_S390X_INCLUDE_DIR}/crc32-s390x.h"
)

add_library(_crc32_s390x ${CRC32_SRCS} ${CRC32_HDRS})

target_include_directories(_crc32_s390x SYSTEM PUBLIC "${CRC32_S390X_INCLUDE_DIR}")
target_compile_definitions(_crc32_s390x PUBLIC)

add_library(ch_contrib::crc32_s390x ALIAS _crc32_s390x)