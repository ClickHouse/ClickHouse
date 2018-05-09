if (APPLE)
    # lib from libs/libcommon
    set (RT_LIBRARY "apple_rt")
else ()
    find_library (RT_LIBRARY rt)
endif ()

message(STATUS "Using rt: ${RT_LIBRARY}")
