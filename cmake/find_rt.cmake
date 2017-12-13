if (APPLE)
    # lib from libs/libcommon
    set (RT_LIBRARY "apple_rt")
else ()
    find_library (RT_LIBRARY rt)
endif ()

message(STATUS "Using rt: ${RT_LIBRARY}")

function (target_link_rt_by_force TARGET)
    if (NOT APPLE)
        set (FLAGS "-Wl,-no-as-needed -lrt -Wl,-as-needed")
        set_property (TARGET ${TARGET} APPEND PROPERTY LINK_FLAGS "${FLAGS}")
    endif ()
endfunction ()
