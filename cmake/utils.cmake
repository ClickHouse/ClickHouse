# Useful stuff

# Function get_all_targets collects all targets recursively
function(get_all_targets outvar)
    macro(get_all_targets_recursive targets dir)
        get_property(subdirectories DIRECTORY ${dir} PROPERTY SUBDIRECTORIES)
        foreach(subdir ${subdirectories})
            get_all_targets_recursive(${targets} ${subdir})
        endforeach()
        get_property(current_targets DIRECTORY ${dir} PROPERTY BUILDSYSTEM_TARGETS)
        list(APPEND ${targets} ${current_targets})
    endmacro()

    set(targets)
    get_all_targets_recursive(targets ${CMAKE_CURRENT_SOURCE_DIR})
    set(${outvar} ${targets} PARENT_SCOPE)
endfunction()


# Function get_target_filename calculates target's output file name
function(get_target_filename target outvar)
    get_target_property(prop_type "${target}" TYPE)
    get_target_property(prop_is_framework "${target}" FRAMEWORK)
    get_target_property(prop_outname "${target}" OUTPUT_NAME)
    get_target_property(prop_archive_outname "${target}" ARCHIVE_OUTPUT_NAME)
    get_target_property(prop_library_outname "${target}" LIBRARY_OUTPUT_NAME)
    get_target_property(prop_runtime_outname "${target}" RUNTIME_OUTPUT_NAME)
    # message("prop_archive_outname: ${prop_archive_outname}")
    # message("prop_library_outname: ${prop_library_outname}")
    # message("prop_runtime_outname: ${prop_runtime_outname}")
    if(DEFINED CMAKE_BUILD_TYPE)
        get_target_property(prop_cfg_outname "${target}" "${OUTPUT_NAME}_${CMAKE_BUILD_TYPE}")
        get_target_property(prop_archive_cfg_outname "${target}" "${ARCHIVE_OUTPUT_NAME}_${CMAKE_BUILD_TYPE}")
        get_target_property(prop_library_cfg_outname "${target}" "${LIBRARY_OUTPUT_NAME}_${CMAKE_BUILD_TYPE}")
        get_target_property(prop_runtime_cfg_outname "${target}" "${RUNTIME_OUTPUT_NAME}_${CMAKE_BUILD_TYPE}")
        # message("prop_archive_cfg_outname: ${prop_archive_cfg_outname}")
        # message("prop_library_cfg_outname: ${prop_library_cfg_outname}")
        # message("prop_runtime_cfg_outname: ${prop_runtime_cfg_outname}")
        if(NOT ("${prop_cfg_outname}" STREQUAL "prop_cfg_outname-NOTFOUND"))
            set(prop_outname "${prop_cfg_outname}")
        endif()
        if(NOT ("${prop_archive_cfg_outname}" STREQUAL "prop_archive_cfg_outname-NOTFOUND"))
            set(prop_archive_outname "${prop_archive_cfg_outname}")
        endif()
        if(NOT ("${prop_library_cfg_outname}" STREQUAL "prop_library_cfg_outname-NOTFOUND"))
            set(prop_library_outname "${prop_library_cfg_outname}")
        endif()
        if(NOT ("${prop_runtime_cfg_outname}" STREQUAL "prop_runtime_cfg_outname-NOTFOUND"))
            set(prop_runtime_outname "${prop_runtime_cfg_outname}")
        endif()
    endif()
    set(outname "${target}")
    if(NOT ("${prop_outname}" STREQUAL "prop_outname-NOTFOUND"))
        set(outname "${prop_outname}")
    endif()
    if("${prop_is_framework}")
        set(filename "${outname}")
    elseif(prop_type STREQUAL "STATIC_LIBRARY")
        if(NOT ("${prop_archive_outname}" STREQUAL "prop_archive_outname-NOTFOUND"))
            set(outname "${prop_archive_outname}")
        endif()
        set(filename "${CMAKE_STATIC_LIBRARY_PREFIX}${outname}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    elseif(prop_type STREQUAL "MODULE_LIBRARY")
        if(NOT ("${prop_library_outname}" STREQUAL "prop_library_outname-NOTFOUND"))
            set(outname "${prop_library_outname}")
        endif()
        set(filename "${CMAKE_SHARED_MODULE_LIBRARY_PREFIX}${outname}${CMAKE_SHARED_MODULE_LIBRARY_SUFFIX}")
    elseif(prop_type STREQUAL "SHARED_LIBRARY")
        if(WIN32)
            if(NOT ("${prop_runtime_outname}" STREQUAL "prop_runtime_outname-NOTFOUND"))
                set(outname "${prop_runtime_outname}")
            endif()
        else()
            if(NOT ("${prop_library_outname}" STREQUAL "prop_library_outname-NOTFOUND"))
                set(outname "${prop_library_outname}")
            endif()
        endif()
        set(filename "${CMAKE_SHARED_LIBRARY_PREFIX}${outname}${CMAKE_SHARED_LIBRARY_SUFFIX}")
    elseif(prop_type STREQUAL "EXECUTABLE")
        if(NOT ("${prop_runtime_outname}" STREQUAL "prop_runtime_outname-NOTFOUND"))
            set(outname "${prop_runtime_outname}")
        endif()
        set(filename "${CMAKE_EXECUTABLE_PREFIX}${outname}${CMAKE_EXECUTABLE_SUFFIX}")
    else()
        message(FATAL_ERROR "target \"${target}\" is not of type STATIC_LIBRARY, MODULE_LIBRARY, SHARED_LIBRARY, or EXECUTABLE.")
    endif()
    set("${outvar}" "${filename}" PARENT_SCOPE)
endfunction()


# Function get_cmake_properties returns list of all propreties that cmake supports
function(get_cmake_properties outvar)
    execute_process(COMMAND cmake --help-property-list
        OUTPUT_VARIABLE cmake_properties
        COMMAND_ERROR_IS_FATAL ANY
    )
    # Convert command output into a CMake list
    string(REGEX REPLACE ";" "\\\\;" cmake_properties "${cmake_properties}")
    string(REGEX REPLACE "\n" ";" cmake_properties "${cmake_properties}")
    list(REMOVE_DUPLICATES cmake_properties)
    set("${outvar}" "${cmake_properties}" PARENT_SCOPE)
endfunction()

# Function get_target_property_list returns list of all propreties set for target
function(get_target_property_list target outvar)
    get_cmake_properties(cmake_property_list)
    foreach(property ${cmake_property_list})
        string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" property ${property})

        # https://stackoverflow.com/questions/32197663/how-can-i-remove-the-the-location-property-may-not-be-read-from-target-error-i
        if(property STREQUAL "LOCATION" OR property MATCHES "^LOCATION_" OR property MATCHES "_LOCATION$")
            continue()
        endif()

        get_property(was_set TARGET ${target} PROPERTY ${property} SET)
        if(was_set)
            get_target_property(value ${target} ${property})
            string(REGEX REPLACE ";" "\\\\\\\\;" value "${value}")
            list(APPEND outvar "${property} = ${value}")
        endif()
    endforeach()
    set(${outvar} ${${outvar}} PARENT_SCOPE)
endfunction()

# Clang-tidy only requires compilation, linking is superfluous. CMake unfortunately has no way to
# suppress linking. As a workaround, we set custom launchers clang-tidy builds which create empty
# files during linking to trick CMake. The only situation where this doesn't work are intermediate
# code-generating binaries like protoc, llvm-tlbgen and their dependencies. These can be built/linked
# as usual using disable_dummy_launchers_if_needed and enable_dummy_launchers_if_needed.
macro(disable_dummy_launchers_if_needed)
    if(ENABLE_DUMMY_LAUNCHERS AND USING_DUMMY_LAUNCHERS)
        set(CMAKE_CXX_COMPILER_LAUNCHER ${DUMMY_LAUNCHER_ORIGINAL_CMAKE_CXX_COMPILER_LAUNCHER})
        set(CMAKE_C_COMPILER_LAUNCHER ${DUMMY_LAUNCHER_ORIGINAL_CMAKE_C_COMPILER_LAUNCHER})
        set(CMAKE_CXX_LINKER_LAUNCHER ${DUMMY_LAUNCHER_ORIGINAL_CMAKE_CXX_LINKER_LAUNCHER})
        set(CMAKE_C_LINKER_LAUNCHER ${DUMMY_LAUNCHER_ORIGINAL_CMAKE_C_LINKER_LAUNCHER})
        set(LINKER_NAME ${ORIGINAL_LINKER_NAME})

        set(USING_DUMMY_LAUNCHERS 0)

        include(${CMAKE_SOURCE_DIR}/cmake/tools.cmake) # include to set the real launchers for all tools
    endif()
endmacro()

macro(enable_dummy_launchers_if_needed)
    if(ENABLE_DUMMY_LAUNCHERS AND NOT USING_DUMMY_LAUNCHERS)
        set(DUMMY_LAUNCHER_ORIGINAL_CMAKE_CXX_COMPILER_LAUNCHER ${CMAKE_CXX_COMPILER_LAUNCHER})
        set(DUMMY_LAUNCHER_ORIGINAL_CMAKE_C_COMPILER_LAUNCHER ${CMAKE_C_COMPILER_LAUNCHER})
        set(DUMMY_LAUNCHER_ORIGINAL_CMAKE_CXX_LINKER_LAUNCHER ${CMAKE_CXX_LINKER_LAUNCHER})
        set(DUMMY_LAUNCHER_ORIGINAL_CMAKE_C_LINKER_LAUNCHER ${CMAKE_C_LINKER_LAUNCHER})
        set(DUMMY_LAUNCHER_ORIGINAL_LINKER_NAME ${LINKER_NAME})

        set(CMAKE_CXX_COMPILER_LAUNCHER "${CMAKE_SOURCE_DIR}/cmake/dummy_compiler_linker.sh")
        set(CMAKE_C_COMPILER_LAUNCHER "${CMAKE_SOURCE_DIR}/cmake/dummy_compiler_linker.sh")
        set(CMAKE_CXX_LINKER_LAUNCHER "${CMAKE_SOURCE_DIR}/cmake/dummy_compiler_linker.sh")
        set(CMAKE_C_LINKER_LAUNCHER "${CMAKE_SOURCE_DIR}/cmake/dummy_compiler_linker.sh")
        set(LINKER_NAME "${CMAKE_SOURCE_DIR}/cmake/dummy_compiler_linker.sh")

        set(USING_DUMMY_LAUNCHERS 1)

        include(${CMAKE_SOURCE_DIR}/cmake/tools.cmake) # include to set the dummy launchers for all tools
    endif()
endmacro()

# Macros to enable wrapping compilation in `prlimit` check.
macro(disable_heavy_build_check_if_needed)
    if(ENABLE_CHECK_HEAVY_BUILDS AND USING_HEAVY_BUILD_CHECK_LAUNCHER)
        set(CMAKE_CXX_COMPILER_LAUNCHER ${HEAVY_BUILD_CHECK_ORIGINAL_CMAKE_CXX_COMPILER_LAUNCHER})

        set(USING_HEAVY_BUILD_CHECK_LAUNCHER 0)
    endif()
endmacro()

macro(enable_heavy_build_check_if_needed)
    if(ENABLE_CHECK_HEAVY_BUILDS AND NOT USING_HEAVY_BUILD_CHECK_LAUNCHER)
        set(HEAVY_BUILD_CHECK_LAUNCHER "${CMAKE_SOURCE_DIR}/cmake/heavy_build_check_scripts/prlimit_generic.sh")

        # Sanitizers are too heavy. Some architectures too.
        if (SANITIZE OR SANITIZE_COVERAGE OR WITH_COVERAGE OR ARCH_RISCV64 OR ARCH_LOONGARCH64)
            set(HEAVY_BUILD_CHECK_LAUNCHER "${CMAKE_SOURCE_DIR}/cmake/heavy_build_check_scripts/prlimit_sanitizers.sh")
        endif()

        set(HEAVY_BUILD_CHECK_ORIGINAL_CMAKE_CXX_COMPILER_LAUNCHER ${CMAKE_CXX_COMPILER_LAUNCHER})
        set(CMAKE_CXX_COMPILER_LAUNCHER ${HEAVY_BUILD_CHECK_LAUNCHER} ${CMAKE_CXX_COMPILER_LAUNCHER})

        set(USING_HEAVY_BUILD_CHECK_LAUNCHER 1)
    endif()
endmacro()
