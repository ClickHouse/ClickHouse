# https://stackoverflow.com/a/62311397/328260
macro (get_all_targets_recursive targets dir)
    get_property (subdirectories DIRECTORY ${dir} PROPERTY SUBDIRECTORIES)
    foreach (subdir ${subdirectories})
        get_all_targets_recursive (${targets} ${subdir})
    endforeach ()
    get_property (current_targets DIRECTORY ${dir} PROPERTY BUILDSYSTEM_TARGETS)
    list (APPEND ${targets} ${current_targets})
endmacro ()

# When you will try to link target with the directory (that exists), cmake will
# skip this without an error, only the following warning will be reported:
#
#     target_link_libraries(main /tmp)
#
#     WARNING: Target "main" requests linking to directory "/tmp".  Targets may link only to libraries.  CMake is dropping the item.
#
# And there is no cmake policy that controls this.
# (I guess the reason that it is allowed is because of FRAMEWORK for OSX).
#
# So to avoid error-prone cmake rules, this can be sanitized.
# There are the following ways:
# - overwrite target_link_libraries()/link_libraries() and check *before*
#   calling real macro, but this requires duplicate all supported syntax
#   -- too complex
# - overwrite target_link_libraries() and check LINK_LIBRARIES property, this
#   works great
#   -- but cannot be used with link_libraries()
# - use BUILDSYSTEM_TARGETS property to get list of all targets and sanitize
#   -- this will work.
function (get_all_targets var)
    set (targets)
    get_all_targets_recursive (targets ${CMAKE_CURRENT_SOURCE_DIR})
    set (${var} ${targets} PARENT_SCOPE)
endfunction()
function (sanitize_link_libraries target)
    get_target_property(target_type ${target} TYPE)
    if (${target_type} STREQUAL "INTERFACE_LIBRARY")
        get_property(linked_libraries TARGET ${target} PROPERTY INTERFACE_LINK_LIBRARIES)
    else()
        get_property(linked_libraries TARGET ${target} PROPERTY LINK_LIBRARIES)
    endif()
    foreach (linked_library ${linked_libraries})
        if (TARGET ${linked_library})
            # just in case, skip if TARGET
        elseif (IS_DIRECTORY ${linked_library})
            message(FATAL_ERROR "${target} requested to link with directory: ${linked_library}")
        endif()
    endforeach()
endfunction()
get_all_targets (all_targets)
foreach (target ${all_targets})
    sanitize_link_libraries(${target})
endforeach()

#
# Do not allow to define -W* from contrib publically (INTERFACE/PUBLIC).
#
function (get_contrib_targets var)
    set (targets)
    get_all_targets_recursive (targets ${CMAKE_CURRENT_SOURCE_DIR}/contrib)
    set (${var} ${targets} PARENT_SCOPE)
endfunction()
function (sanitize_interface_flags target)
    get_target_property(target_type ${target} TYPE)
    get_property(compile_definitions TARGET ${target} PROPERTY INTERFACE_COMPILE_DEFINITIONS)
    get_property(compile_options TARGET ${target} PROPERTY INTERFACE_COMPILE_OPTIONS)
    if (NOT "${compile_options}" STREQUAL "")
        message(FATAL_ERROR "${target} set INTERFACE_COMPILE_OPTIONS to ${compile_options}. This is forbidden.")
    endif()
    if ("${compile_definitions}" MATCHES "-Wl,")
        # linker option - OK
    elseif ("${compile_definitions}" MATCHES "-W")
        message(FATAL_ERROR "${target} contains ${compile_definitions} flags in INTERFACE_COMPILE_DEFINITIONS. This is forbidden.")
    endif()
endfunction()
get_contrib_targets (contrib_targets)
foreach (contrib_target ${contrib_targets})
    sanitize_interface_flags(${contrib_target})
endforeach()

