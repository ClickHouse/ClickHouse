
include_directories (${ClickHouse_SOURCE_DIR}/dbms/include)
# for generated config_version.h and config.h:
include_directories (${ClickHouse_BINARY_DIR}/dbms/include)

# TODO:
# move code with incldes from .h to .cpp and clean this list:
include_directories (${ClickHouse_SOURCE_DIR}/libs/libcommon/include)
# for generated config_common.h:
include_directories (${ClickHouse_BINARY_DIR}/libs/libcommon/include)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libpocoext/include)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libzkutil/include)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libmysqlxx/include)
include_directories (BEFORE ${ClickHouse_SOURCE_DIR}/contrib/libcityhash/include)
