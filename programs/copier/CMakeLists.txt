set(CLICKHOUSE_COPIER_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/ClusterCopierApp.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/ClusterCopier.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/Internals.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/ShardPartition.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/ShardPartitionPiece.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/StatusAccumulator.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/TaskCluster.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/TaskShard.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/TaskTable.cpp")

set (CLICKHOUSE_COPIER_LINK
        PRIVATE
                clickhouse_common_zookeeper
                clickhouse_common_config
                clickhouse_parsers
                clickhouse_functions
                clickhouse_table_functions
                clickhouse_aggregate_functions
                string_utils

        PUBLIC
                daemon
)

set(CLICKHOUSE_COPIER_INCLUDE SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

clickhouse_program_add(copier)
