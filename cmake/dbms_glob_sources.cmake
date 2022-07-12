macro(add_glob cur_list)
    file(GLOB __tmp CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${ARGN})
    list(APPEND ${cur_list} ${__tmp})
endmacro()

macro(add_headers_and_sources prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.h)
endmacro()

macro(add_headers_only prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
endmacro()

if (ENABLE_CUDA)
    macro(add_cuda_headers_and_sources prefix common_path)
        add_glob(${prefix}_headers RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h ${common_path}/*.cuh)
        add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.cu ${common_path}/*.h ${common_path}/*.cuh)
    endmacro()

    macro(add_cuda_headers_only prefix common_path)
        add_glob(${prefix}_headers RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h ${common_path}/*.cuh)
    endmacro()
endif ()