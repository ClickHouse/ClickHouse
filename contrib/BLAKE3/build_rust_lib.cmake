function(build_cargo target_name project_dir)
    file(GLOB sources ${project_dir}/src/**/*.rs)

    set(compile_message "Compiling ${target_name}")

    if(CARGO_RELEASE_FLAG STREQUAL "--release")
        set(compile_message "${compile_message} in release mode")
    endif()

    set(TARGET_SPEC "")

    set(OSX_RUST_ROOT "")

    message(STATUS "Toolchain file for ${target_name}: ${CMAKE_TOOLCHAIN_FILE}")
    if(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-aarch64")
        set(TARGET_SPEC "aarch64-unknown-linux-gnu")
        message("Checking Rust toolchain for special target")
        execute_process(COMMAND rustup target add aarch64-unknown-linux-gnu)
        message(STATUS "Switch Rust target to ${TARGET_SPEC}")
        set(compile_message "${compile_message} for special target ${TARGET_SPEC}")
    endif()

    if((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "toolchain-aarch64"))
        set(TARGET_SPEC "aarch64-apple-darwin")
        message(STATUS "Checking Rust toolchain for special target")
        execute_process(COMMAND rustup target add aarch64-apple-darwin)
        message(STATUS "Switch Rust target to ${TARGET_SPEC}")
        set(compile_message "${compile_message} for special target ${TARGET_SPEC}")
        if (DEFINED CMAKE_OSX_SYSROOT)
            set(OSX_RUST_ROOT "${CMAKE_OSX_SYSROOT}")
        endif()
    endif()

    if((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
        set(TARGET_SPEC "x86_64-apple-darwin")
        message("Checking Rust toolchain for special target")
        execute_process(COMMAND rustup target add x86_64-apple-darwin)
        message(STATUS "Switch Rust target to ${TARGET_SPEC}")
        set(compile_message "${compile_message} for special target ${TARGET_SPEC}")
    endif()

    if((CMAKE_TOOLCHAIN_FILE MATCHES "freebsd") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
        set(TARGET_SPEC "x86_64-unknown-freebsd")
        message("Checking Rust toolchain for special target")
        execute_process(COMMAND rustup target add x86_64-unknown-freebsd)
        message(STATUS "Switch Rust target to ${TARGET_SPEC}")
        set(compile_message "${compile_message} for special target ${TARGET_SPEC}")
    endif()

    if(CMAKE_TOOLCHAIN_FILE MATCHES "ppc64le")
        set(TARGET_SPEC "powerpc64le-unknown-linux-gnu")
        message("Checking Rust toolchain for special target")
        execute_process(COMMAND rustup target add powerpc64le-unknown-linux-gnu)
        message(STATUS "Switch Rust target to ${TARGET_SPEC}")
        set(compile_message "${compile_message} for special target ${TARGET_SPEC}")
    endif()

    set(output_library ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_SPEC}/${TARGET_DIR}/lib${target_name}.a)

    set(TARGET_SPEC "--target=${TARGET_SPEC}")

    if(TARGET_SPEC STREQUAL "--target=")
        set(TARGET_SPEC "")
    endif()

    add_custom_command(
        COMMENT ${compile_message}
        COMMAND export BUILD_FOR_OSX=${OSX_RUST_ROOT}
        COMMAND env CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} cargo build ${CARGO_RELEASE_FLAG} ${TARGET_SPEC}
        COMMAND cp ${output_library} ${CMAKE_CURRENT_BINARY_DIR}
        OUTPUT ${output_library}
        WORKING_DIRECTORY ${project_dir})

    if(NOT TARGET ${target_name}-target)
        add_custom_target(${target_name}-target ALL DEPENDS ${output_library})
    endif()

    set_property(
        TARGET ${target_name}-target
        APPEND PROPERTY
            INTERFACE_DEPENDENCIES ${output_library}
    )

    set_target_properties(${target_name}-target PROPERTIES LOCATION ${output_library})

    add_library(ch_contrib::${target_name} STATIC IMPORTED GLOBAL)

    add_dependencies(ch_contrib::${target_name} ${target_name}-target)

    set_target_properties(ch_contrib::${target_name}
    	PROPERTIES
    	IMPORTED_LOCATION ${output_library}
    	INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/include/)

    add_library(blake3 ALIAS ch_contrib::blake3)

endfunction()


