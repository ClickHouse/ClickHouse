#!/usr/bin/env python3
"""
Driver for executable user-defined functions whose body is a C expression.

Invocation:
    c_function_body_driver.py [create|drop] --name NAME --return TYPE --args "x UInt8, y DateTime"

When called with `create`, reads the C function body from stdin and:

  1. Generates a wrapper.c that defines the user function and reads/writes
     chunked RowBinary rows on stdin/stdout, calling the user function for each row.
  2. Compiles the wrapper inside a sandboxed Docker container.
  3. Prints to stdout an XML configuration for an `executable_pool` UDF whose
     command runs the compiled binary inside another sandboxed Docker
     container with no network access, read-only root filesystem, dropped
     capabilities, and as the server OS user.

When called with `drop`, this driver has no extra work to do besides what
ClickHouse does itself (delete the dynamic config file and remove the working
directory).
"""

import argparse
import os
import shutil
import subprocess
import sys
from html import escape as xml_escape

# Mapping from ClickHouse data types to a (C type, reader, writer) tuple.
# Only a small set of types is supported here - this is a proof of concept.
TYPE_MAP = {
    "UInt8":  ("uint8_t",  "ch_read_uint8",  "ch_write_uint8"),
    "UInt16": ("uint16_t", "ch_read_uint16", "ch_write_uint16"),
    "UInt32": ("uint32_t", "ch_read_uint32", "ch_write_uint32"),
    "UInt64": ("uint64_t", "ch_read_uint64", "ch_write_uint64"),
    "Int8":   ("int8_t",   "ch_read_int8",   "ch_write_int8"),
    "Int16":  ("int16_t",  "ch_read_int16",  "ch_write_int16"),
    "Int32":  ("int32_t",  "ch_read_int32",  "ch_write_int32"),
    "Int64":  ("int64_t",  "ch_read_int64",  "ch_write_int64"),
    "Float32": ("float",   "ch_read_float",  "ch_write_float"),
    "Float64": ("double",  "ch_read_double", "ch_write_double"),
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

    ret_c_type, _, ret_writer = TYPE_MAP[return_type]

    arg_c_decls = []
    arg_reads = []
    user_func_params = []
    user_call_args = []
    for name, ty in args:
        c_type, reader, _ = TYPE_MAP[ty]
        arg_c_decls.append(f"            {c_type} {name};")
        arg_reads.append(f'            if ({reader}(&{name}) != 1) {{ ch_error("read error\\n"); return 2; }}')
        user_func_params.append(f"{c_type} {name}")
        user_call_args.append(name)

    arg_c_decls_str = "\n".join(arg_c_decls) if arg_c_decls else ""
    arg_reads_str = "\n".join(arg_reads) if arg_reads else ""
    user_params_str = ", ".join(user_func_params) if user_func_params else "void"
    user_call_args_str = ", ".join(user_call_args)

    safe_function_name = function_name.replace('"', '\\"')

    return f"""\
/* Auto-generated wrapper for executable UDF '{safe_function_name}'. */
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

static {ret_c_type} user_function({user_params_str})
{{
{user_body}
}}

#define CH_BUFFER_SIZE (1U << 20)

static unsigned char ch_input_buffer[CH_BUFFER_SIZE];
static size_t ch_input_pos = 0;
static size_t ch_input_size = 0;

static unsigned char ch_output_buffer[CH_BUFFER_SIZE];
static size_t ch_output_pos = 0;

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

static int ch_refill_input(void)
{{
    ssize_t bytes = ch_read_retry(STDIN_FILENO, ch_input_buffer, sizeof(ch_input_buffer));
    if (bytes < 0)
        return -1;

    ch_input_pos = 0;
    ch_input_size = (size_t)bytes;
    return bytes == 0 ? 0 : 1;
}}

static inline int ch_read_byte(unsigned char * value)
{{
    if (ch_input_pos == ch_input_size)
    {{
        int status = ch_refill_input();
        if (status <= 0)
            return status;
    }}

    *value = ch_input_buffer[ch_input_pos++];
    return 1;
}}

static inline int ch_read_exact(void * ptr, size_t size)
{{
    unsigned char * out = (unsigned char *)ptr;
    while (size != 0)
    {{
        if (ch_input_pos == ch_input_size)
        {{
            int status = ch_refill_input();
            if (status <= 0)
                return status;
        }}

        size_t available = ch_input_size - ch_input_pos;
        size_t bytes = available < size ? available : size;
        __builtin_memcpy(out, ch_input_buffer + ch_input_pos, bytes);
        ch_input_pos += bytes;
        out += bytes;
        size -= bytes;
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

#define CH_DEFINE_RW(name, type) \\
    static inline int ch_read_##name(type * value) \\
    {{ \\
        size_t size = sizeof(*value); \\
        if (ch_input_size - ch_input_pos < size) \\
            return ch_read_exact(value, size); \\
        __builtin_memcpy(value, ch_input_buffer + ch_input_pos, size); \\
        ch_input_pos += size; \\
        return 1; \\
    }} \\
    static inline int ch_write_##name(const type * value) \\
    {{ \\
        size_t size = sizeof(*value); \\
        if (sizeof(ch_output_buffer) - ch_output_pos < size) \\
        {{ \\
            if (ch_flush_output() != 1) \\
                return -1; \\
        }} \\
        __builtin_memcpy(ch_output_buffer + ch_output_pos, value, size); \\
        ch_output_pos += size; \\
        return 1; \\
    }}

CH_DEFINE_RW(uint8, uint8_t)
CH_DEFINE_RW(uint16, uint16_t)
CH_DEFINE_RW(uint32, uint32_t)
CH_DEFINE_RW(uint64, uint64_t)
CH_DEFINE_RW(int8, int8_t)
CH_DEFINE_RW(int16, int16_t)
CH_DEFINE_RW(int32, int32_t)
CH_DEFINE_RW(int64, int64_t)
CH_DEFINE_RW(float, float)
CH_DEFINE_RW(double, double)

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

int main(void)
{{
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

        for (uint64_t row = 0; row != rows; ++row)
        {{
{arg_c_decls_str}
{arg_reads_str}
            {ret_c_type} result = user_function({user_call_args_str});
            if ({ret_writer}(&result) != 1)
            {{
                ch_error("write error\\n");
                return 3;
            }}
        }}

        if (ch_flush_output() != 1)
        {{
            ch_error("flush error\\n");
            return 3;
        }}
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


def docker_available():
    try:
        result = subprocess.run(["docker", "version"], capture_output=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False


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


def docker_user():
    return os.environ.get("CLICKHOUSE_C_DRIVER_DOCKER_USER", f"{os.getuid()}:{os.getgid()}")


def compile_with_docker(work_dir):
    """Compile wrapper.c -> user_func inside an isolated Docker build container."""
    image = docker_image_for_build()
    cmd = [
        "docker", "run", "--rm",
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
        "sh", "-c", "cc -O2 -static -o user_func wrapper.c && chmod 0755 user_func",
    ]
    run(cmd)


def compile_with_cc(work_dir):
    """Fallback compilation when Docker is unavailable - direct `cc` invocation."""
    cmd = ["cc", "-O2", "-o", os.path.join(work_dir, "user_func"), os.path.join(work_dir, "wrapper.c")]
    run(cmd)


def generate_xml_config(function_name, return_type, args, work_dir):
    """Produce an executable_pool UDF configuration that invokes the compiled binary
    inside a sandboxed Docker container, or directly if `CLICKHOUSE_C_DRIVER_FORCE_LOCAL=1`."""
    arguments_xml = "".join(
        "        <argument><name>{0}</name><type>{1}</type></argument>\n".format(
            xml_escape(name), xml_escape(ty))
        for name, ty in args
    )

    if os.environ.get("CLICKHOUSE_C_DRIVER_FORCE_LOCAL") == "1":
        runtime_command = f"{work_dir}/user_func"
    else:
        docker_image = docker_image_for_run()
        limits = docker_resource_limits()
        user = docker_user()
        # Tmp dir inside container is needed for some libc init even on a static binary.
        runtime_command = (
            f"docker run --rm -i "
            f"--network=none "
            f"--read-only "
            f"--tmpfs=/tmp:rw,size=16m "
            f"--cap-drop=ALL "
            f"--user {user} "
            f"--memory={limits['memory']} "
            f"--cpus={limits['cpus']} "
            f"--pids-limit={limits['pids']} "
            f"-v {work_dir}/user_func:/user_func:ro "
            f"{docker_image} "
            f"/user_func"
        )

    return f"""<functions>
    <function>
        <type>executable_pool</type>
        <name>{xml_escape(function_name)}</name>
        <return_type>{xml_escape(return_type)}</return_type>
{arguments_xml}        <format>RowBinary</format>
        <command>{xml_escape(runtime_command)}</command>
        <execute_direct>0</execute_direct>
        <pool_size>4</pool_size>
        <send_chunk_header>1</send_chunk_header>
        <command_read_timeout>10000</command_read_timeout>
        <command_write_timeout>10000</command_write_timeout>
        <command_termination_timeout>10</command_termination_timeout>
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

    if os.environ.get("CLICKHOUSE_C_DRIVER_FORCE_LOCAL") == "1" or not docker_available():
        compile_with_cc(work_dir)
    else:
        compile_with_docker(work_dir)

    if not os.path.exists(os.path.join(work_dir, "user_func")):
        sys.exit("Compilation produced no binary")

    sys.stdout.write(generate_xml_config(args.name, getattr(args, "return"), parsed_args, work_dir))


def cmd_drop(args):
    """Nothing to do at the driver level - ClickHouse will remove the working directory."""
    # Best-effort cleanup of any container that may have leaked the function name as a label.
    pass


def main():
    parser = argparse.ArgumentParser(description="Driver for C-body executable user-defined functions.")
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
