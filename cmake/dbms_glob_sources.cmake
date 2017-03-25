macro(add_glob cur_list)
	file(GLOB __tmp RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${ARGN})
	list(APPEND ${cur_list} ${__tmp})
endmacro()

macro(add_headers_and_sources prefix common_path)
	add_glob(${prefix}_headers RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} include/DB/${common_path}/*.h include/DB/${common_path}/*.inl)
	add_glob(${prefix}_sources src/${common_path}/*.cpp src/${common_path}/*.h)
endmacro()

macro(add_headers_only prefix common_path)
	add_glob(${prefix}_headers RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} include/DB/${common_path}/*.h src/${common_path}/*.h)
endmacro()

macro(add_path_sources prefix common_path)
	add_glob(${prefix}_sources ${common_path}/*.cpp)
endmacro()
