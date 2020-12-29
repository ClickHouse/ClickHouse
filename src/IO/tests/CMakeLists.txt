add_executable (read_buffer read_buffer.cpp)
target_link_libraries (read_buffer PRIVATE clickhouse_common_io)

add_executable (read_buffer_perf read_buffer_perf.cpp)
target_link_libraries (read_buffer_perf PRIVATE clickhouse_common_io)

add_executable (read_float_perf read_float_perf.cpp)
target_link_libraries (read_float_perf PRIVATE clickhouse_common_io)

add_executable (write_buffer write_buffer.cpp)
target_link_libraries (write_buffer PRIVATE clickhouse_common_io)

add_executable (write_buffer_perf write_buffer_perf.cpp)
target_link_libraries (write_buffer_perf PRIVATE clickhouse_common_io)

add_executable (valid_utf8_perf valid_utf8_perf.cpp)
target_link_libraries (valid_utf8_perf PRIVATE clickhouse_common_io)

add_executable (valid_utf8 valid_utf8.cpp)
target_link_libraries (valid_utf8 PRIVATE clickhouse_common_io)

add_executable (var_uint var_uint.cpp)
target_link_libraries (var_uint PRIVATE clickhouse_common_io)

add_executable (read_escaped_string read_escaped_string.cpp)
target_link_libraries (read_escaped_string PRIVATE clickhouse_common_io)

add_executable (async_write async_write.cpp)
target_link_libraries (async_write PRIVATE dbms)

add_executable (parse_int_perf parse_int_perf.cpp)
target_link_libraries (parse_int_perf PRIVATE clickhouse_common_io)

add_executable (parse_int_perf2 parse_int_perf2.cpp)
target_link_libraries (parse_int_perf2 PRIVATE clickhouse_common_io)

add_executable (read_write_int read_write_int.cpp)
target_link_libraries (read_write_int PRIVATE clickhouse_common_io)

add_executable (o_direct_and_dirty_pages o_direct_and_dirty_pages.cpp)
target_link_libraries (o_direct_and_dirty_pages PRIVATE clickhouse_common_io)

add_executable (hashing_write_buffer hashing_write_buffer.cpp)
target_link_libraries (hashing_write_buffer PRIVATE clickhouse_common_io)
add_check(hashing_write_buffer)

add_executable (hashing_read_buffer hashing_read_buffer.cpp)
target_link_libraries (hashing_read_buffer PRIVATE clickhouse_common_io)
add_check (hashing_read_buffer)

add_executable (io_operators io_operators.cpp)
target_link_libraries (io_operators PRIVATE clickhouse_common_io)

add_executable (write_int write_int.cpp)
target_link_libraries (write_int PRIVATE clickhouse_common_io)

if (OS_LINUX OR OS_FREEBSD)
    add_executable(write_buffer_aio write_buffer_aio.cpp)
    target_link_libraries (write_buffer_aio PRIVATE clickhouse_common_io)

    add_executable(read_buffer_aio read_buffer_aio.cpp)
    target_link_libraries (read_buffer_aio PRIVATE clickhouse_common_io)
endif ()

add_executable (zlib_buffers zlib_buffers.cpp)
target_link_libraries (zlib_buffers PRIVATE clickhouse_common_io)

add_executable (limit_read_buffer limit_read_buffer.cpp)
target_link_libraries (limit_read_buffer PRIVATE clickhouse_common_io)

add_executable (limit_read_buffer2 limit_read_buffer2.cpp)
target_link_libraries (limit_read_buffer2 PRIVATE clickhouse_common_io)

add_executable (parse_date_time_best_effort parse_date_time_best_effort.cpp)
target_link_libraries (parse_date_time_best_effort PRIVATE clickhouse_common_io)

add_executable (zlib_ng_bug zlib_ng_bug.cpp)
target_link_libraries (zlib_ng_bug PRIVATE ${ZLIB_LIBRARIES})

add_executable (ryu_test ryu_test.cpp)
target_link_libraries (ryu_test PRIVATE ryu)
