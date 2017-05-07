include_directories (${CMAKE_CURRENT_BINARY_DIR})
set(SRCS )

add_executable (read_buffer read_buffer.cpp ${SRCS})
target_link_libraries (read_buffer dbms)

add_executable (read_buffer_perf read_buffer_perf.cpp ${SRCS})
target_link_libraries (read_buffer_perf dbms)

add_executable (read_float_perf read_float_perf.cpp ${SRCS})
target_link_libraries (read_float_perf dbms)

add_executable (write_buffer write_buffer.cpp ${SRCS})
target_link_libraries (write_buffer dbms)

add_executable (write_buffer_perf write_buffer_perf.cpp ${SRCS})
target_link_libraries (write_buffer_perf dbms)

add_executable (valid_utf8_perf valid_utf8_perf.cpp ${SRCS})
target_link_libraries (valid_utf8_perf dbms)

add_executable (valid_utf8 valid_utf8.cpp ${SRCS})
target_link_libraries (valid_utf8 dbms)

add_executable (compressed_buffer compressed_buffer.cpp ${SRCS})
target_link_libraries (compressed_buffer dbms)

add_executable (var_uint var_uint.cpp ${SRCS})
target_link_libraries (var_uint dbms)

add_executable (read_escaped_string read_escaped_string.cpp ${SRCS})
target_link_libraries (read_escaped_string dbms)

add_executable (async_write async_write.cpp ${SRCS})
target_link_libraries (async_write dbms)

add_executable (parse_int_perf parse_int_perf.cpp ${SRCS})
target_link_libraries (parse_int_perf dbms)

add_executable (parse_int_perf2 parse_int_perf2.cpp ${SRCS})
target_link_libraries (parse_int_perf2 dbms)

add_executable (read_write_int read_write_int.cpp ${SRCS})
target_link_libraries (read_write_int dbms)

add_executable (mempbrk mempbrk.cpp ${SRCS})
target_link_libraries (mempbrk dbms)

add_executable (cached_compressed_read_buffer cached_compressed_read_buffer.cpp ${SRCS})
target_link_libraries (cached_compressed_read_buffer dbms)

add_executable (o_direct_and_dirty_pages o_direct_and_dirty_pages.cpp ${SRCS})
target_link_libraries (o_direct_and_dirty_pages dbms)

add_executable (hashing_write_buffer hashing_write_buffer.cpp ${SRCS})
target_link_libraries (hashing_write_buffer dbms)
add_check(hashing_write_buffer)

add_executable (hashing_read_buffer hashing_read_buffer.cpp)
target_link_libraries (hashing_read_buffer dbms)
add_check (hashing_read_buffer)

add_executable (io_operators operators.cpp ${SRCS})
target_link_libraries (io_operators dbms)

if (NOT APPLE AND NOT CMAKE_SYSTEM MATCHES "FreeBSD")
    add_executable(write_buffer_aio write_buffer_aio.cpp)
    target_link_libraries (write_buffer_aio dbms ${Boost_FILESYSTEM_LIBRARY})

    add_executable(read_buffer_aio read_buffer_aio.cpp)
    target_link_libraries (read_buffer_aio dbms ${Boost_FILESYSTEM_LIBRARY})
endif ()

add_executable (zlib_buffers zlib_buffers.cpp ${SRCS})
target_link_libraries (zlib_buffers dbms)

add_executable (remote_read_write_buffer remote_read_write_buffer.cpp ${SRCS})
target_link_libraries (remote_read_write_buffer dbms)
