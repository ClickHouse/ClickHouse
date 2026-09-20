set (CLICKHOUSE_DOCKER_INIT_SOURCES docker-init.cpp)

set (CLICKHOUSE_DOCKER_INIT_LINK
    PRIVATE
        clickhouse_common_io
)

clickhouse_program_add(docker-init)
