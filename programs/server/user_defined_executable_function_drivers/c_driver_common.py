#!/usr/bin/env python3
"""
Shared implementation for C-body executable user-defined function drivers.

Invocation:
    c_driver_common.py --runtime docker [create|drop] --name NAME --return TYPE --args "x UInt8, y DateTime"

When called with `create`, reads the C function body from stdin and:

  1. Generates a wrapper.c that defines the user function and reads/writes
     chunked `Buffers` blocks on stdin/stdout, calling the user function for each row.
  2. Compiles the wrapper.
  3. Prints to stdout an XML configuration for an `executable_pool` UDF whose
     command runs the compiled binary using the selected runtime.

When called with `drop`, the driver removes runtime state it created outside
ClickHouse's dynamic configuration and work directory.
"""

import argparse
import hashlib
import os
import shlex
import subprocess
import sys
from html import escape as xml_escape

# Mapping from ClickHouse data types to a C type.
# Only a small set of types is supported here - this is a proof of concept.
TYPE_MAP = {
    "UInt8":  "uint8_t",
    "UInt16": "uint16_t",
    "UInt32": "uint32_t",
    "UInt64": "uint64_t",
    "Int8":   "int8_t",
    "Int16":  "int16_t",
    "Int32":  "int32_t",
    "Int64":  "int64_t",
    "Float32": "float",
    "Float64": "double",
    "String": "struct buf",
}


def parse_args_signature(args_signature):
    """Parse "name1 Type1, name2 Type2" -> [(name, type), ...]."""
    if not args_signature:
        return []
    out = []
    for part in args_signature.split(","):
        part = part.strip()
        if not part:
            continue
        tokens = part.split()
        if len(tokens) != 2:
            raise SystemExit(f"Bad argument signature element: {part!r} (expected 'name Type')")
        name, ty = tokens
        if ty not in TYPE_MAP:
            raise SystemExit(f"Unsupported argument type: {ty}")
        out.append((name, ty))
    return out


def generate_wrapper_c(function_name, return_type, args, user_body):
    if return_type not in TYPE_MAP:
        raise SystemExit(f"Unsupported return type: {return_type}")

    ret_c_type = TYPE_MAP[return_type]
    ret_is_string = return_type == "String"

    arg_column_decls = []
    arg_column_reads = []
    arg_value_decls = []
    process_params = []
    process_call_args = []
    process_user_args = []
    user_func_params = []
    for index, (name, ty) in enumerate(args):
        c_type = TYPE_MAP[ty]
        column_name = f"ch_arg_{index}"
        values_name = f"{column_name}_values"
        arg_column_decls.append(f"    struct ch_column {column_name} = {{0}};")
        if ty == "String":
            arg_column_reads.append(
                f'        if (ch_read_column(&{column_name}) != 1 || ch_parse_string_column(&{column_name}, rows) != 1) {{ ch_error("input column read error\\n"); return 2; }}')
            arg_value_decls.append(f"        const struct buf * __restrict {values_name} = {column_name}.strings;")
        else:
            arg_column_reads.append(
                f'        if (ch_read_column(&{column_name}) != 1 || ch_validate_fixed_column(&{column_name}, rows, sizeof({c_type})) != 1) {{ ch_error("input column read error\\n"); return 2; }}')
            arg_value_decls.append(f"        const {c_type} * __restrict {values_name} = (const {c_type} *){column_name}.data.data;")
        process_params.append(f"const {c_type} * __restrict {values_name}")
        process_call_args.append(values_name)
        process_user_args.append(f"{values_name}[row]")
        user_func_params.append(f"{c_type} {name}")

    arg_column_decls_str = "\n".join(arg_column_decls) if arg_column_decls else ""
    arg_column_reads_str = "\n".join(arg_column_reads) if arg_column_reads else ""
    arg_value_decls_str = "\n".join(arg_value_decls) if arg_value_decls else ""
    user_params_str = ", ".join(user_func_params) if user_func_params else "void"
    process_user_args_str = ", ".join(process_user_args)

    if ret_is_string:
        result_setup_str = "        ch_buffer_reset(&ch_result);"
        process_params.append("struct ch_buffer * ch_result")
        process_call_args.append("&ch_result")
        process_result_str = f"""\
        {ret_c_type} result = user_function({process_user_args_str});
        if (ch_append_string(ch_result, &result) != 1)
            return -1;"""
    else:
        result_setup_str = f"""\
        if (rows > (uint64_t)(SIZE_MAX / sizeof({ret_c_type})) || ch_buffer_resize(&ch_result, (size_t)rows * sizeof({ret_c_type})) != 1)
        {{
            ch_error("result buffer allocation error\\n");
            return 3;
        }}
        {ret_c_type} * __restrict ch_result_values = ({ret_c_type} *)ch_result.data;"""
        process_params.append(f"{ret_c_type} * __restrict ch_result_values")
        process_call_args.append("ch_result_values")
        process_result_str = f"        ch_result_values[row] = user_function({process_user_args_str});"

    process_params_str = ", ".join(["uint64_t rows"] + process_params)
    process_call_args_str = ", ".join(["rows"] + process_call_args)
    process_function_str = f"""\
static CH_ALWAYS_INLINE int ch_process_chunk({process_params_str})
{{
    for (uint64_t row = 0; row != rows; ++row)
    {{
{process_result_str}
    }}
    return 1;
}}
"""

    process_call_str = f"""\
        if (ch_process_chunk({process_call_args_str}) != 1)
        {{
            ch_error("write error\\n");
            return 3;
        }}"""

    safe_function_name = function_name.replace('"', '\\"')

    return f"""\
/* Auto-generated wrapper for executable UDF '{safe_function_name}'. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

struct buf
{{
    const char * data;
    size_t size;
}};

struct buf alloc(size_t size);

#if defined(__GNUC__) || defined(__clang__)
#define CH_ALWAYS_INLINE __attribute__((always_inline)) inline
#else
#define CH_ALWAYS_INLINE inline
#endif

static CH_ALWAYS_INLINE {ret_c_type} user_function({user_params_str})
{{
{user_body}
}}

#define CH_BUFFER_SIZE (1U << 16)
#define CH_PIPE_CAPACITY (1U << 20)

static unsigned char ch_output_buffer[CH_BUFFER_SIZE];
static size_t ch_output_pos = 0;

struct ch_buffer
{{
    unsigned char * data;
    size_t size;
    size_t capacity;
}};

struct ch_column
{{
    struct ch_buffer data;
    struct buf * strings;
    size_t string_capacity;
}};

struct ch_arena_block
{{
    struct ch_arena_block * next;
    size_t size;
    size_t pos;
    char data[];
}};

static struct ch_arena_block * ch_arena_first = NULL;
static struct ch_arena_block * ch_arena_current = NULL;

static struct ch_arena_block * ch_new_arena_block(size_t size)
{{
    size_t block_size = size < CH_BUFFER_SIZE ? CH_BUFFER_SIZE : size;
    if (block_size > SIZE_MAX - sizeof(struct ch_arena_block))
        return NULL;

    struct ch_arena_block * block = (struct ch_arena_block *)malloc(sizeof(struct ch_arena_block) + block_size);
    if (block == NULL)
        return NULL;

    block->next = NULL;
    block->size = block_size;
    block->pos = 0;
    return block;
}}

struct buf alloc(size_t size)
{{
    struct buf result = {{NULL, 0}};
    if (size == 0)
        return result;

    if (ch_arena_current == NULL)
    {{
        ch_arena_current = ch_new_arena_block(size);
        if (ch_arena_current == NULL)
            return result;
        ch_arena_first = ch_arena_current;
    }}

    if (ch_arena_current->size - ch_arena_current->pos < size)
    {{
        struct ch_arena_block * block = ch_arena_current->next;
        while (block != NULL && block->size - block->pos < size)
            block = block->next;

        if (block == NULL)
        {{
            block = ch_new_arena_block(size);
            if (block == NULL)
                return result;

            block->next = ch_arena_current->next;
            ch_arena_current->next = block;
        }}

        ch_arena_current = block;
    }}

    if (ch_arena_current->size - ch_arena_current->pos < size)
        return result;

    result.data = ch_arena_current->data + ch_arena_current->pos;
    result.size = size;
    ch_arena_current->pos += size;
    return result;
}}

static inline void ch_reset_alloc(void)
{{
    for (struct ch_arena_block * block = ch_arena_first; block != NULL; block = block->next)
        block->pos = 0;
    ch_arena_current = ch_arena_first;
}}

static ssize_t ch_read_retry(int fd, void * data, size_t size)
{{
    for (;;)
    {{
        ssize_t res = read(fd, data, size);
        if (res < 0 && errno == EINTR)
            continue;
        return res;
    }}
}}

static ssize_t ch_write_retry(int fd, const void * data, size_t size)
{{
    for (;;)
    {{
        ssize_t res = write(fd, data, size);
        if (res < 0 && errno == EINTR)
            continue;
        return res;
    }}
}}

static void ch_error(const char * message)
{{
    for (const char * end = message; *end; ++end)
    {{
        if (end[1] == '\\0')
        {{
            (void)ch_write_retry(STDERR_FILENO, message, (size_t)(end + 1 - message));
            return;
        }}
    }}
}}

static inline void ch_try_set_pipe_capacity(int fd)
{{
#if defined(__linux__) && defined(F_SETPIPE_SZ)
    (void)fcntl(fd, F_SETPIPE_SZ, CH_PIPE_CAPACITY);
#else
    (void)fd;
#endif
}}

static inline int ch_read_byte(unsigned char * value)
{{
    ssize_t bytes = ch_read_retry(STDIN_FILENO, value, 1);
    if (bytes < 0)
        return -1;
    return bytes == 0 ? 0 : 1;
}}

static inline int ch_read_exact(void * ptr, size_t size)
{{
    unsigned char * out = (unsigned char *)ptr;
    while (size != 0)
    {{
        ssize_t bytes = ch_read_retry(STDIN_FILENO, out, size);
        if (bytes <= 0)
            return bytes == 0 ? 0 : -1;
        out += (size_t)bytes;
        size -= (size_t)bytes;
    }}
    return 1;
}}

static int ch_flush_output(void)
{{
    size_t written = 0;
    while (written != ch_output_pos)
    {{
        ssize_t bytes = ch_write_retry(STDOUT_FILENO, ch_output_buffer + written, ch_output_pos - written);
        if (bytes <= 0)
            return -1;
        written += (size_t)bytes;
    }}

    ch_output_pos = 0;
    return 1;
}}

static int ch_write_direct(const void * ptr, size_t size)
{{
    if (ch_flush_output() != 1)
        return -1;

    const unsigned char * in = (const unsigned char *)ptr;
    while (size != 0)
    {{
        ssize_t bytes = ch_write_retry(STDOUT_FILENO, in, size);
        if (bytes <= 0)
            return -1;
        in += (size_t)bytes;
        size -= (size_t)bytes;
    }}
    return 1;
}}

static inline int ch_write_exact(const void * ptr, size_t size)
{{
    const unsigned char * in = (const unsigned char *)ptr;
    while (size != 0)
    {{
        if (ch_output_pos == sizeof(ch_output_buffer) && ch_flush_output() != 1)
            return -1;

        size_t available = sizeof(ch_output_buffer) - ch_output_pos;
        size_t bytes = available < size ? available : size;
        __builtin_memcpy(ch_output_buffer + ch_output_pos, in, bytes);
        ch_output_pos += bytes;
        in += bytes;
        size -= bytes;
    }}
    return 1;
}}

static int ch_read_uint64_le(uint64_t * value)
{{
    return ch_read_exact(value, sizeof(*value));
}}

static int ch_write_uint64_le(uint64_t value)
{{
    return ch_write_exact(&value, sizeof(value));
}}

static int ch_buffer_reserve(struct ch_buffer * buffer, size_t capacity)
{{
    if (capacity <= buffer->capacity)
        return 1;

    size_t new_capacity = buffer->capacity == 0 ? CH_BUFFER_SIZE : buffer->capacity;
    while (new_capacity < capacity)
    {{
        if (new_capacity > SIZE_MAX / 2)
        {{
            new_capacity = capacity;
            break;
        }}
        new_capacity *= 2;
    }}

    unsigned char * new_data = (unsigned char *)realloc(buffer->data, new_capacity);
    if (new_data == NULL)
        return -1;

    buffer->data = new_data;
    buffer->capacity = new_capacity;
    return 1;
}}

static int ch_buffer_resize(struct ch_buffer * buffer, size_t size)
{{
    if (ch_buffer_reserve(buffer, size) != 1)
        return -1;

    buffer->size = size;
    return 1;
}}

static int ch_buffer_append(struct ch_buffer * buffer, const void * data, size_t size)
{{
    if (size == 0)
        return 1;

    if (size > SIZE_MAX - buffer->size)
        return -1;

    size_t new_size = buffer->size + size;
    if (ch_buffer_reserve(buffer, new_size) != 1)
        return -1;

    __builtin_memcpy(buffer->data + buffer->size, data, size);
    buffer->size = new_size;
    return 1;
}}

static inline void ch_buffer_reset(struct ch_buffer * buffer)
{{
    buffer->size = 0;
}}

static int ch_read_column(struct ch_column * column)
{{
    uint64_t buffer_size = 0;
    if (ch_read_uint64_le(&buffer_size) != 1)
        return -1;

    if (buffer_size > (uint64_t)SIZE_MAX)
        return -1;

    if (ch_buffer_resize(&column->data, (size_t)buffer_size) != 1)
        return -1;

    return ch_read_exact(column->data.data, column->data.size);
}}

static int ch_validate_fixed_column(const struct ch_column * column, uint64_t rows, size_t element_size)
{{
    if (element_size == 0)
        return -1;

    if (rows > (uint64_t)(SIZE_MAX / element_size))
        return -1;

    return column->data.size == (size_t)rows * element_size ? 1 : -1;
}}

static int ch_column_reserve_strings(struct ch_column * column, uint64_t rows)
{{
    if (rows > (uint64_t)(SIZE_MAX / sizeof(struct buf)))
        return -1;

    size_t capacity = (size_t)rows;
    if (capacity <= column->string_capacity)
        return 1;

    struct buf * strings = (struct buf *)realloc(column->strings, capacity * sizeof(struct buf));
    if (strings == NULL)
        return -1;

    column->strings = strings;
    column->string_capacity = capacity;
    return 1;
}}

static int ch_read_var_uint_from_memory(const unsigned char ** pos, const unsigned char * end, uint64_t * value)
{{
    uint64_t result = 0;

    for (unsigned int shift = 0; shift < 64; shift += 7)
    {{
        if (*pos == end)
            return -1;

        unsigned char byte = **pos;
        ++*pos;

        if (shift == 63 && (byte & 0xFE) != 0)
            return -1;

        result |= ((uint64_t)(byte & 0x7F)) << shift;
        if ((byte & 0x80) == 0)
        {{
            *value = result;
            return 1;
        }}
    }}

    return -1;
}}

static int ch_parse_string_column(struct ch_column * column, uint64_t rows)
{{
    if (ch_column_reserve_strings(column, rows) != 1)
        return -1;

    if (rows == 0)
        return column->data.size == 0 ? 1 : -1;

    const unsigned char * pos = column->data.data;
    const unsigned char * end = pos + column->data.size;

    for (uint64_t row = 0; row != rows; ++row)
    {{
        uint64_t size = 0;
        if (ch_read_var_uint_from_memory(&pos, end, &size) != 1)
            return -1;

        if (size > (uint64_t)SIZE_MAX || size > (uint64_t)(end - pos))
            return -1;

        column->strings[row].data = (const char *)pos;
        column->strings[row].size = (size_t)size;
        pos += size;
    }}

    return pos == end ? 1 : -1;
}}

static int ch_buffer_append_var_uint(struct ch_buffer * buffer, uint64_t value)
{{
    unsigned char bytes[10];
    size_t pos = 0;

    do
    {{
        unsigned char byte = (unsigned char)(value & 0x7F);
        value >>= 7;
        if (value != 0)
            byte |= 0x80;
        bytes[pos++] = byte;
    }} while (value != 0);

    return ch_buffer_append(buffer, bytes, pos);
}}

static int ch_append_string(struct ch_buffer * buffer, const struct buf * value)
{{
    uint64_t size = (uint64_t)value->size;
    if ((size_t)size != value->size)
        return -1;

    if (value->size != 0 && value->data == NULL)
        return -1;

    if (ch_buffer_append_var_uint(buffer, size) != 1)
        return -1;

    return ch_buffer_append(buffer, value->data, value->size);
}}

static int ch_read_buffers_header(uint64_t * rows, uint64_t expected_columns)
{{
    uint64_t columns = 0;
    if (ch_read_uint64_le(&columns) != 1)
        return -1;

    if (columns != expected_columns)
        return -1;

    return ch_read_uint64_le(rows);
}}

static int ch_write_buffers_result(const struct ch_buffer * column, uint64_t rows)
{{
    uint64_t column_size = (uint64_t)column->size;
    if ((size_t)column_size != column->size)
        return -1;

    if (ch_write_uint64_le(1) != 1)
        return -1;

    if (ch_write_uint64_le(rows) != 1)
        return -1;

    if (ch_write_uint64_le(column_size) != 1)
        return -1;

    return ch_write_direct(column->data, column->size);
}}

static int ch_read_chunk_header(uint64_t * rows)
{{
    uint64_t value = 0;
    unsigned char c = 0;
    int status = ch_read_byte(&c);
    if (status <= 0)
        return status;

    if (c < '0' || c > '9')
        return -1;

    while (c != '\\n')
    {{
        if (c < '0' || c > '9')
            return -1;

        uint64_t digit = (uint64_t)(c - '0');
        if (value > (UINT64_MAX - digit) / 10)
            return -1;
        value = value * 10 + digit;

        status = ch_read_byte(&c);
        if (status <= 0)
            return -1;
    }}

    *rows = (uint64_t)value;
    return 1;
}}

{process_function_str}
int main(void)
{{
    ch_try_set_pipe_capacity(STDIN_FILENO);
    ch_try_set_pipe_capacity(STDOUT_FILENO);

{arg_column_decls_str}
    struct ch_buffer ch_result = {{0}};

    for (;;)
    {{
        uint64_t rows = 0;
        int header_status = ch_read_chunk_header(&rows);
        if (header_status == 0)
            break;
        if (header_status < 0)
        {{
            ch_error("chunk header read error\\n");
            return 2;
        }}

        uint64_t block_rows = 0;
        if (ch_read_buffers_header(&block_rows, {len(args)}) != 1 || block_rows != rows)
        {{
            ch_error("buffers header read error\\n");
            return 2;
        }}

{arg_column_reads_str}
{arg_value_decls_str}
{result_setup_str}
{process_call_str}

        if (ch_write_buffers_result(&ch_result, rows) != 1)
        {{
            ch_error("write error\\n");
            return 3;
        }}

        if (ch_flush_output() != 1)
        {{
            ch_error("flush error\\n");
            return 3;
        }}
        ch_reset_alloc();
    }}
    return 0;
}}
"""


def run(cmd, **kwargs):
    """Run a subprocess, raising with stderr on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        sys.stderr.write(f"Command failed: {' '.join(cmd)}\n")
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        sys.exit(result.returncode)
    return result


def docker_image_for_build():
    return os.environ.get("CLICKHOUSE_C_DRIVER_BUILD_IMAGE", "gcc:14")


def docker_image_for_run():
    return os.environ.get("CLICKHOUSE_C_DRIVER_RUN_IMAGE", "alpine:3.20")


def docker_resource_limits():
    return {
        "memory": os.environ.get("CLICKHOUSE_C_DRIVER_MEMORY", "256m"),
        "cpus": os.environ.get("CLICKHOUSE_C_DRIVER_CPUS", "1.0"),
        "pids": os.environ.get("CLICKHOUSE_C_DRIVER_PIDS", "64"),
    }


def pool_size():
    return int(os.environ.get("CLICKHOUSE_C_DRIVER_POOL_SIZE", "64"))


def pipe_capacity(runtime):
    override = os.environ.get("CLICKHOUSE_C_DRIVER_PIPE_CAPACITY")
    if override is not None:
        return int(override)
    if runtime == "gvisor":
        return 1 << 20
    return 0


def docker_user():
    return os.environ.get("CLICKHOUSE_C_DRIVER_DOCKER_USER", f"{os.getuid()}:{os.getgid()}")


def gvisor_binary():
    return os.environ.get(
        "CLICKHOUSE_C_DRIVER_GVISOR_BINARY",
        os.environ.get("CLICKHOUSE_C_DRIVER_GVISOR_RUNTIME", "runsc"))


def shell_join(args):
    return " ".join(shlex.quote(str(arg)) for arg in args)


def compile_with_docker(work_dir):
    """Compile wrapper.c -> user_func inside an isolated Docker build container."""
    image = docker_image_for_build()
    cmd = [
        "docker", "run", "--rm",
        "--log-driver=none",
        "--network=none",
        "--read-only",
        "--tmpfs=/tmp:rw,size=64m",
        "--cap-drop=ALL",
        "--memory=512m",
        "--cpus=1.0",
        "--pids-limit=128",
        "--user", docker_user(),
        "-v", f"{work_dir}:/work",
        "-w", "/work",
        image,
        "sh", "-c", "cc -O3 -march=native -static -o user_func wrapper.c && chmod 0755 user_func",
    ]
    run(cmd)


def compile_with_cc(work_dir):
    """Compile wrapper.c -> user_func with the host C compiler."""
    cmd = ["cc", "-O3", "-march=native", "-o", os.path.join(work_dir, "user_func"), os.path.join(work_dir, "wrapper.c")]
    run(cmd)


def docker_container_name(function_name, work_dir):
    key = os.path.abspath(os.path.dirname(work_dir)) + "\0" + function_name
    digest = hashlib.sha256(key.encode()).hexdigest()[:32]
    return f"clickhouse_udf_{digest}"


def docker_container_name_path(work_dir):
    return os.path.join(work_dir, "docker_container_name")


def docker_fifo_runner_path(work_dir):
    return os.path.join(work_dir, "docker_fifo_runner.sh")


def read_docker_container_name(function_name, work_dir):
    try:
        with open(docker_container_name_path(work_dir)) as f:
            name = f.read().strip()
            if name:
                return name
    except OSError:
        pass
    return docker_container_name(function_name, work_dir)


def docker_container_pids_limit():
    limit = docker_resource_limits()["pids"]
    try:
        return str(max(int(limit), pool_size() * 16))
    except ValueError:
        return limit


def write_docker_fifo_runner(container_name, work_dir):
    runner_path = docker_fifo_runner_path(work_dir)
    script = f"""#!/bin/sh
set -u

container={shell_join([container_name])}
work_dir={shell_join([work_dir])}
mount_dir=/work

pipe_dir=$(mktemp -d "${{work_dir}}/fifo.XXXXXX") || exit 1
cleanup()
{{
    if [ "${{writer_pid:-}}" ]; then
        kill "$writer_pid" 2>/dev/null || true
    fi
    if [ "${{reader_pid:-}}" ]; then
        kill "$reader_pid" 2>/dev/null || true
    fi
    if [ "${{docker_pid:-}}" ]; then
        kill "$docker_pid" 2>/dev/null || true
    fi
    rm -rf "$pipe_dir"
}}
trap cleanup EXIT INT TERM

mkfifo "$pipe_dir/in" "$pipe_dir/out" || exit 1
pipe_name=$(basename "$pipe_dir")
docker_log="$pipe_dir/docker.log"

docker exec -w "$mount_dir" "$container" sh -c 'exec "$1" < "$2" > "$3"' sh \
    "$mount_dir/user_func" "$mount_dir/$pipe_name/in" "$mount_dir/$pipe_name/out" < /dev/null > "$docker_log" 2>&1 &
docker_pid=$!

exec 5<&0
cat <&5 > "$pipe_dir/in" &
writer_pid=$!
exec 5<&-

cat "$pipe_dir/out" &
reader_pid=$!

writer_status=0
reader_status=0
docker_status=0
wait "$docker_pid" || docker_status=$?
if [ "$docker_status" -ne 0 ]; then
    if [ -s "$docker_log" ]; then
        cat "$docker_log" >&2
    fi
    kill "$writer_pid" "$reader_pid" 2>/dev/null || true
    wait "$writer_pid" 2>/dev/null || true
    wait "$reader_pid" 2>/dev/null || true
    exit "$docker_status"
fi
wait "$writer_pid" || writer_status=$?
wait "$reader_pid" || reader_status=$?

if [ "$writer_status" -ne 0 ]; then
    exit "$writer_status"
fi
if [ -s "$docker_log" ]; then
    cat "$docker_log" >&2
fi
exit "$reader_status"
"""
    with open(runner_path, "w") as f:
        f.write(script)
    os.chmod(runner_path, 0o755)


def start_docker_container(function_name, work_dir):
    name = docker_container_name(function_name, work_dir)
    with open(docker_container_name_path(work_dir), "w") as f:
        f.write(name + "\n")

    write_docker_fifo_runner(name, work_dir)

    if os.environ.get("CLICKHOUSE_C_DRIVER_SKIP_DOCKER_CONTAINER") == "1":
        return

    image = docker_image_for_run()
    limits = docker_resource_limits()
    user = docker_user()

    subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)
    cmd = [
        "docker", "run", "-d",
        "--name", name,
        "--log-driver=none",
        "--network=none",
        "--read-only",
        "--tmpfs=/tmp:rw,size=16m",
        "--cap-drop=ALL",
        "--user", user,
        f"--memory={limits['memory']}",
        f"--cpus={limits['cpus']}",
        f"--pids-limit={docker_container_pids_limit()}",
        "-v", f"{work_dir}:/work:rw",
        image,
        "sleep", "2147483647",
    ]
    run(cmd)


def stop_docker_container(function_name, work_dir):
    name = read_docker_container_name(function_name, work_dir)
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)


def should_compile_locally(runtime):
    return (
        runtime == "unsafe"
        or os.environ.get("CLICKHOUSE_C_DRIVER_FORCE_LOCAL") == "1"
        or os.environ.get("CLICKHOUSE_C_DRIVER_COMPILE_LOCAL") == "1"
    )


def runtime_command(function_name, runtime, work_dir):
    if runtime == "unsafe" or os.environ.get("CLICKHOUSE_C_DRIVER_FORCE_LOCAL") == "1":
        return "user_func", 1

    if runtime == "gvisor":
        return shell_join([
            gvisor_binary(),
            "--rootless",
            "--network=none",
            "do",
            os.path.join(work_dir, "user_func"),
        ]), 0

    return "docker_fifo_runner.sh", 1


def generate_xml_config(function_name, return_type, args, work_dir, runtime):
    """Produce an executable_pool UDF configuration for the selected runtime."""
    arguments_xml = "".join(
        "        <argument><name>{0}</name><type>{1}</type></argument>\n".format(
            xml_escape(name), xml_escape(ty))
        for name, ty in args
    )
    pipe_capacity_value = pipe_capacity(runtime)
    pipe_capacity_xml = ""
    if pipe_capacity_value:
        pipe_capacity_xml = f"        <command_pipe_capacity>{pipe_capacity_value}</command_pipe_capacity>\n"

    command, execute_direct = runtime_command(function_name, runtime, work_dir)

    return f"""<functions>
    <function>
        <type>executable_pool</type>
        <name>{xml_escape(function_name)}</name>
        <return_type>{xml_escape(return_type)}</return_type>
{arguments_xml}        <format>Buffers</format>
        <command>{xml_escape(command)}</command>
        <execute_direct>{execute_direct}</execute_direct>
        <pool_size>{pool_size()}</pool_size>
        <send_chunk_header>1</send_chunk_header>
        <command_read_timeout>10000</command_read_timeout>
        <command_write_timeout>10000</command_write_timeout>
{pipe_capacity_xml}        <command_termination_timeout>10</command_termination_timeout>
    </function>
</functions>
"""


def cmd_create(args):
    body = sys.stdin.read()
    parsed_args = parse_args_signature(args.args)

    work_dir = os.getcwd()
    wrapper_path = os.path.join(work_dir, "wrapper.c")
    with open(wrapper_path, "w") as f:
        f.write(generate_wrapper_c(args.name, getattr(args, "return"), parsed_args, body))

    if should_compile_locally(args.runtime):
        compile_with_cc(work_dir)
    else:
        compile_with_docker(work_dir)

    if not os.path.exists(os.path.join(work_dir, "user_func")):
        sys.exit("Compilation produced no binary")

    if args.runtime == "docker" and os.environ.get("CLICKHOUSE_C_DRIVER_FORCE_LOCAL") != "1":
        start_docker_container(args.name, work_dir)

    sys.stdout.write(generate_xml_config(args.name, getattr(args, "return"), parsed_args, work_dir, args.runtime))


def cmd_drop(args):
    """Remove driver-owned runtime state before ClickHouse removes the working directory."""
    if (
        args.runtime == "docker"
        and os.environ.get("CLICKHOUSE_C_DRIVER_FORCE_LOCAL") != "1"
        and os.environ.get("CLICKHOUSE_C_DRIVER_SKIP_DOCKER_CONTAINER") != "1"
    ):
        stop_docker_container(args.name, os.getcwd())


def main():
    parser = argparse.ArgumentParser(description="Driver for C-body executable user-defined functions.")
    parser.add_argument("--runtime", choices=("docker", "gvisor", "unsafe"), default="docker")
    subparsers = parser.add_subparsers(dest="action", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--name", required=True)
    common.add_argument("--return", required=True, dest="return")
    common.add_argument("--args", default="")

    create_p = subparsers.add_parser("create", parents=[common])
    create_p.set_defaults(handler=cmd_create)

    drop_p = subparsers.add_parser("drop", parents=[common])
    drop_p.set_defaults(handler=cmd_drop)

    args, _ = parser.parse_known_args()
    args.handler(args)


if __name__ == "__main__":
    main()
