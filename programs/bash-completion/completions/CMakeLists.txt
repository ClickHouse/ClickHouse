macro(configure_bash_completion)
    set(out "/usr/share/bash-completion/completions")
    find_program(pkg-config PKG_CONFIG_BIN)
    if (PKG_CONFIG_BIN)
        execute_process(
            COMMAND ${PKG_CONFIG_BIN} --variable=completionsdir bash-completion
            OUTPUT_VARIABLE ${out}
            OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY
        )
    endif()
    string(REPLACE /usr "${CMAKE_INSTALL_PREFIX}" out "${out}")
    message(STATUS "bash_completion will be written to ${out}")
endmacro()

configure_bash_completion()
foreach (name
    # set of functions
    clickhouse-bootstrap

    # binaries that accept settings as command line argument
    clickhouse-client
    clickhouse-local
    clickhouse-benchmark

    clickhouse
)
    install(FILES ${name} DESTINATION ${out})
endforeach()
