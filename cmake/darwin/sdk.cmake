option (SDK_PATH "Path to the SDK to build with" "")

if (NOT EXISTS "${SDK_PATH}/SDKSettings.plist")
    message (FATAL_ERROR "Wrong SDK path provided: ${SDK_PATH}")
endif ()

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -isysroot ${SDK_PATH} -mmacosx-version-min=10.14")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -isysroot ${SDK_PATH} -mmacosx-version-min=10.14")

set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -isysroot ${SDK_PATH} -mmacosx-version-min=10.14")
set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -isysroot ${SDK_PATH} -mmacosx-version-min=10.14")
