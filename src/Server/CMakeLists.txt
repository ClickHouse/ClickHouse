if (TARGET ch_contrib::grpc)
    add_subdirectory (grpc_protos)
endif()

if (TARGET ch_contrib::acme-lw)
    target_link_libraries(clickhouse_server PRIVATE ch_contrib::acme-lw)
endif()
