
include_directories (${ClickHouse_SOURCE_DIR}/dbms/src)
# for generated config_version.h and config.h:
include_directories (${ClickHouse_BINARY_DIR}/dbms/src)

# TODO:
# move code with incldes from .h to .cpp and clean this list:
include_directories (${ClickHouse_SOURCE_DIR}/libs/libcommon/include)
# for generated config_common.h:
include_directories (${ClickHouse_BINARY_DIR}/libs/libcommon/include)
