import re
import string
import sys

sys.path.append(".")
from ci.praktika.utils import Shell


class FuzzerLogParser:
    UNKNOWN_ERROR = "Unknown error"
    MAX_INLINE_REPRODUCE_COMMANDS = 20
    SQL_COMMANDS = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "WITH",
        "EXPLAIN",
        "DESCRIBE",
        "SHOW",
        "SET",
        "OPTIMIZE",
        "SYSTEM",
        "DETACH",
        "ATTACH",
        "FUNCTION",
    ]

    READ_SQL_COMMANDS = [
        "SELECT",
        "EXPLAIN",
        "DESCRIBE",
        "SHOW",
    ]

    WRITE_SQL_COMMANDS = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "SET",
    ]

    def __init__(self, server_log, fuzzer_log="", stderr_log="", stack_trace_str=None):
        self.server_log = server_log
        self.fuzzer_log = fuzzer_log
        self.stderr_log = stderr_log
        self.stack_trace_str = stack_trace_str

    def parse_failure(self):
        files = []
        is_logical_error = False
        is_sanitizer_error = False
        is_killed_by_signal = False
        is_segfault = False
        is_memory_limit_exceeded = False
        error_patterns = [
            (
                "Sanitizer",
                "is_sanitizer_error",
                r"(SUMMARY|ERROR|WARNING): [a-zA-Z]+Sanitizer:.*",
            ),
            ("Logical error", "is_logical_error", r"Logical error.*"),
            (
                "Assertion",
                "is_logical_error",
                r"Assertion.*failed|Failed assertion.*|.*_LIBCPP_ASSERT.*",
            ),
            (
                "Runtime error",
                "is_sanitizer_error",
                r".*runtime error: .*|.*is located.*",
            ),
            ("SegFault", "is_segfault", r"Segmentation fault.*"),
            (
                "Signal",
                "is_killed_by_signal",
                r"Received signal.*|.*Child process was terminated by signal 9.*",
            ),
            (
                "Memory limit exceeded",
                "is_memory_limit_exceeded",
                r".*\(total\) memory limit exceeded.*",
            ),
        ]

        error_output = None
        for name, flag_name, pattern in error_patterns:
            output = ""
            if self.stack_trace_str:
                output = Shell.get_output(
                    f"echo '{self.stack_trace_str}' | rg --text -A 10 -o '{pattern}' | head -n10",
                    strict=True,
                )
            else:
                if flag_name == "is_sanitizer_error":
                    assert self.stderr_log
                    file = self.stderr_log
                else:
                    assert self.server_log
                    file = self.server_log
                output = Shell.get_output(
                    f"rg --text -A 10 -o '{pattern}' {file} | head -n10",
                    strict=True,
                )

            if output:
                error_output = output
                if flag_name == "is_sanitizer_error":
                    is_sanitizer_error = True
                elif flag_name == "is_logical_error":
                    is_logical_error = True
                elif flag_name == "is_killed_by_signal":
                    is_killed_by_signal = True
                elif flag_name == "is_segfault":
                    is_segfault = True
                elif flag_name == "is_memory_limit_exceeded":
                    is_memory_limit_exceeded = True
                break

        if not error_output:
            return (
                self.UNKNOWN_ERROR,
                "Lost connection to server. See the logs.\n",
                files,
            )

        error_lines = error_output.splitlines()
        result_name = error_lines[0].removesuffix(".")
        format_message = ""
        for i, line in enumerate(error_lines):
            if "Format string: " in line:
                # Extract the format string content between quotes
                # Example: "... <Fatal> : Format string: 'Unknown numeric column of type: {}'."
                start_idx = line.find("Format string: ")
                if start_idx != -1:
                    substring = line[start_idx + len("Format string: ") :]
                    # Remove quotes and trailing period
                    substring = substring.strip().rstrip(".").strip("'\"")
                    format_message = substring
                break
        # keep all lines before next log line
        for i, line in enumerate(error_lines):
            if "] {" in line and "} <" in line or line.startswith("    #"):
                # it's a new log line or sanitizer frame - break
                error_lines = error_lines[:i]
                break
        error_output = "\n".join(error_lines)
        failed_query = ""
        reproduce_commands = []
        stack_trace = self.get_stack_trace()
        stack_trace_id = self.get_stack_trace_id(stack_trace)

        if is_logical_error:
            failed_query = self.get_failed_query()
            if failed_query and self.fuzzer_log:
                reproduce_commands = self.get_reproduce_commands(failed_query)
            if format_message and "Inconsistent AST formatting" not in result_name:
                # Replace {} placeholders with A, B, C, etc. to create a generic error pattern.
                # This normalization groups similar errors together for better tracking.
                # Exception: 'Inconsistent AST formatting' errors preserve original parameters
                # as they identify the specific problematic AST node.
                letters = string.ascii_uppercase
                letter_index = 0
                while "{}" in format_message and letter_index < len(letters):
                    format_message = format_message.replace(
                        "{}", letters[letter_index], 1
                    )
                    letter_index += 1
                result_name = f"Logical error: {format_message}"
            # For most logical errors, the Stack Trace ID is redundant since the error message
            # is sufficient to identify the issue. However, for certain errors, different stack
            # traces may indicate different root causes - include STID in the failure name to
            # distinguish them.
            result_name += f" (STID: {stack_trace_id})"
        elif is_killed_by_signal or is_segfault:
            result_name += f" (STID: {stack_trace_id})"
        elif is_memory_limit_exceeded:
            result_name = "Server unresponsive: memory limit exceeded"
        elif is_sanitizer_error:
            stack_trace = self.get_sanitizer_stack_trace()
            if not stack_trace:
                print("ERROR: Failed to parse sanitizer stack trace")
            stack_trace_id = self.get_stack_trace_id(stack_trace)
            error = ""
            if "AddressSanitizer" in error_output:
                if "heap-use-after-free" in error_output:
                    error = ": heap-use-after-free"
                elif "attempting double-free" in error_output:
                    error = ": double-free"
                elif "stack-use-after-scope" in error_output:
                    error = ": stack-use-after-scope"
                elif "stack-use-after-return" in error_output:
                    error = ": stack-use-after-return"
                result_name = f"AddressSanitizer{error} (STID: {stack_trace_id})"
            elif "ThreadSanitizer" in error_output:
                if "data race" in error_output:
                    error = ": data race"
                elif "thread leak" in error_output:
                    error = ": thread leak"
                elif "destroy of a locked mutex" in error_output:
                    error = ": destroy of a locked mutex"
                elif "unlock of an unlocked mutex" in error_output:
                    error = ": unlock of an unlocked mutex"
                elif (
                    "lock-order-inversion" in error_output
                    or "potential deadlock" in error_output
                ):
                    error = ": potential deadlock"
                elif "signal-unsafe call" in error_output:
                    error = ": signal-unsafe call"
                result_name = f"ThreadSanitizer{error} (STID: {stack_trace_id})"
            elif "UndefinedBehaviorSanitizer" in error_output:
                if "division by zero" in error_output:
                    error = ": division by zero"
                elif "signed integer overflow" in error_output:
                    error = ": signed integer overflow"
                elif (
                    "shift exponent" in error_output or "shift-exponent" in error_output
                ):
                    error = ": shift exponent overflow"
                elif "shift base" in error_output or "shift-base" in error_output:
                    error = ": shift base overflow"
                elif "null pointer" in error_output or "nullptr" in error_output:
                    error = ": null pointer"
                elif (
                    "misaligned address" in error_output
                    or "misaligned-pointer-use" in error_output
                ):
                    error = ": misaligned address"
                elif "invalid-bool-value" in error_output:
                    error = ": invalid bool value"
                elif "invalid-enum-value" in error_output:
                    error = ": invalid enum value"
                elif "float-cast-overflow" in error_output:
                    error = ": float cast overflow"
                elif "float-divide-by-zero" in error_output:
                    error = ": float divide by zero"
                elif "object-size-mismatch" in error_output:
                    error = ": object size mismatch"
                elif "vptr" in error_output:
                    error = ": bad vtable pointer"
                elif "undefined-behavior" in error_output:
                    error = ": undefined behavior"
                result_name = (
                    f"UndefinedBehaviorSanitizer{error} (STID: {stack_trace_id})"
                )
            elif "MemorySanitizer" in error_output:
                if "use-of-uninitialized-value" in error_output:
                    error = ": use-of-uninitialized-value"
                elif "use-of-uninitialized-memory" in error_output:
                    error = ": use-of-uninitialized-memory"
                result_name = f"MemorySanitizer{error} (STID: {stack_trace_id})"
            else:
                result_name = f"Sanitizer (STID: {stack_trace_id})"
        else:
            print(f"TODO: Unknown error {error_output}")

        info = f"Error:\n{error_output}\n"
        if failed_query:
            info += "---\n\nFailed query:\n"
            info += failed_query + "\n"
        if reproduce_commands:
            info += "---\n\nReproduce commands (auto-generated; may require manual adjustment):\n"
            if len(reproduce_commands) > self.MAX_INLINE_REPRODUCE_COMMANDS:
                reproduce_file_sql = "reproduce_commands.sql"
                try:
                    with open(reproduce_file_sql, "w") as f:
                        f.write("\n".join(reproduce_commands))
                    files.append(reproduce_file_sql)
                    info += f"See file: {reproduce_file_sql}\n"
                except IOError as write_error:
                    info += f"Failed to write reproduce commands file: {write_error}\n"
            else:
                info += "\n".join(reproduce_commands) + "\n"
        if stack_trace:
            info += "---\n\nStack trace:\n"
            info += stack_trace + "\n"

        return result_name, info, files

    def get_sanitizer_stack_trace(self):
        # return all lines after Sanitizer error starting with "    #DIGITS "
        def _extract_sanitizer_trace(log_file):
            lines = []
            stack_frame_pattern = re.compile(r"^\s+#\d+\s+")
            stack_frame_pattern_1st_line = re.compile(r"^\s+#0\s")
            # Pattern to remove ANSI escape codes (colors from tools like ripgrep)
            ansi_escape = re.compile(r"\x1b\[[0-9;]*m")

            with open(log_file, "r", errors="replace") as file:
                all_lines = file.readlines()

            in_sanitizer_trace = False
            for line in all_lines:
                # Strip ANSI color codes before pattern matching
                clean_line = ansi_escape.sub("", line)

                if not in_sanitizer_trace:
                    if stack_frame_pattern_1st_line.search(clean_line):
                        in_sanitizer_trace = True
                        lines.append(clean_line.strip())
                else:
                    if stack_frame_pattern.match(clean_line):
                        lines.append(clean_line.strip())
                    elif in_sanitizer_trace:
                        # End of stack trace
                        break
            return lines

        lines = []

        if self.stderr_log:
            lines = _extract_sanitizer_trace(self.stderr_log)
        else:
            assert False, "No stderr log provided"

        return "\n".join(lines) if lines else None

    def get_stack_trace(self):
        lines = []
        stack_trace_pattern = re.compile(r"<Fatal> BaseDaemon: \d+(?:\.\d+)*\.\s*")

        if self.stack_trace_str:
            all_lines = self.stack_trace_str.splitlines()
        else:
            with open(self.server_log, "r", errors="replace") as file:
                all_lines = file.readlines()

        for line in reversed(all_lines):
            if "<Fatal> BaseDaemon: Stack trace:" in line:
                break
            match = stack_trace_pattern.search(line)
            if match:
                # Extract only the part after the pattern
                extracted = line[match.end() :]
                # Only append if there's meaningful content after extraction
                if extracted.strip():
                    lines.append(extracted)
        lines = [line.strip().replace("\n", "") for line in lines]
        return "\n".join(reversed(lines)) if lines else None

    def get_stack_trace_id(self, stack_trace):
        """
        Generate a stack trace ID (hash) to match and connect related stack traces.

        Implementation aims to increase true-positive matches while minimizing false-positives by:
        - Counting only ClickHouse functions in DB:: namespace
        - Dropping templates and input arguments from function signatures
        - Limiting depth to top ST_MAX_DEPTH functions for broader matching
        - Excluding DB::Exception functions and everything above them (issue typically occurs before exception is thrown)

        Returns: ID in format DDDD-XXXX where:
            DDDD = 4-digit base-10 hash from first function name
            XXXX = 4-digit hex hash from all functions
        """
        ST_MAX_DEPTH = 5
        if not stack_trace:
            return None

        lines = stack_trace.splitlines()
        functions = []

        for line in lines:
            # Normalize multiple DB:: occurrences
            line = line.replace(", DB::", "")

            # Check if line contains DB:: namespace
            if " DB::" in line:
                start_idx = line.find(" DB::")
                substring = line[start_idx + 1 :]  # Skip the leading space
            elif line.startswith("DB::"):
                substring = line
            else:
                continue

            # Truncate at first '(' or '<' to keep only function name
            paren_idx = substring.find("(")
            angle_idx = substring.find("<")

            if paren_idx != -1 and angle_idx != -1:
                end_idx = min(paren_idx, angle_idx)
            elif paren_idx != -1:
                end_idx = paren_idx
            elif angle_idx != -1:
                end_idx = angle_idx
            else:
                end_idx = len(substring)

            substring = substring[:end_idx]
            functions.append(substring)

        # Remove exception functions and everything above them
        for i, func in enumerate(functions):
            if "DB::Exception" in func:
                functions = functions[i + 1 :]
                break
        # Remove all remaining DB::Exception functions
        functions = [f for f in functions if "DB::Exception" not in f]

        # Limit to top ST_MAX_DEPTH functions for broader matching
        functions = functions[:ST_MAX_DEPTH]

        if not functions:
            return None

        # Generate 4-digit base-10 hash from first function name
        func_hash = sum(ord(c) for c in functions[0]) % 10000
        func_part = f"{func_hash:04d}"

        # Generate 4-digit hex hash from all functions
        func_str = "".join(functions)
        st_hash = sum(ord(c) for c in func_str) % (16**4)
        st_part = f"{st_hash:04x}"

        stack_trace_id = f"{func_part}-{st_part}"
        print(f"Stack trace functions: {functions}")
        return stack_trace_id

    def get_failed_query(self):
        # TODO: Fetch the failed query from fuzzer.log instead of server.log to ensure exact matching.
        # The server.log may normalize whitespace or format queries differently, making it difficult
        # to locate the corresponding query and its dependencies in fuzzer.log.
        failure_output = Shell.get_output(
            f"rg --text -A10 'Logical error.*|Assertion.*failed|Failed assertion.*|.*runtime error: .*|.*is located.*|(SUMMARY|ERROR|WARNING): [a-zA-Z]+Sanitizer:.*|.*_LIBCPP_ASSERT.*' {self.server_log}",
            verbose=True,
        )
        if not failure_output:
            return None
        if "Inconsistent AST formatting: the query:" in failure_output:
            lines = failure_output.splitlines()
            if len(lines) > 1:
                query_command = lines[1]
                return query_command
            else:
                print("ERROR: Expected query on second line but not found")
                return None

        assert failure_output, "No failure found in server log"
        # Find the first line that has a proper log format with query ID.
        # rg may match continuation lines (e.g. SQL comments like "-- Logical error query")
        # that lack the "] {query_id}" prefix.
        query_id = None
        for line in failure_output.splitlines():
            if " ] {" in line and "} <" in line:
                query_id = line.split(" ] {")[1].split("}")[0]
                break
        if not query_id:
            print("ERROR: Query id not found")
            return None
        print(f"Query id: {query_id}")
        query_command = Shell.get_output(
            f"grep -a '{query_id}' {self.server_log} | head -n1"
        )
        if not query_command:
            print("Query not found in server log by query id")
            return None
        query_command = query_command.split(" (stage:")[0]

        min_pos = len(query_command)
        for keyword in self.SQL_COMMANDS:
            if keyword in query_command:
                keyword_pos = query_command.find(keyword)
                min_pos = min(min_pos, keyword_pos)
        if min_pos == len(query_command):
            print(f"No SQL keyword found in query command [{query_command}]")
            return None
        query_command = query_command[min_pos:]
        return query_command

    def get_reproduce_commands(self, failed_query):
        all_fuzzer_commands = self._get_all_fuzzer_commands()
        if not all_fuzzer_commands or failed_query not in all_fuzzer_commands:
            print("No fuzzer commands found or query command not found in fuzzer log")
            return None
        query_index = all_fuzzer_commands.index(failed_query)
        all_fuzzer_commands = all_fuzzer_commands[:query_index]

        # get all tables from the command
        # Match table names after FROM and various JOIN types (LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.)
        tables = set()
        table_files = set()
        # Match table names/functions after FROM and JOIN keywords
        # Captures complete function calls like file(...) or table names
        from_pattern = r"\bFROM\s+([a-zA-Z0-9_.]+(?:\([^()]*(?:\([^()]*\))*[^()]*\))?)"
        join_pattern = r"\bJOIN\s+([a-zA-Z0-9_.]+(?:\([^()]*(?:\([^()]*\))*[^()]*\))?)"
        from_matches = re.findall(from_pattern, failed_query, re.IGNORECASE)
        join_matches = re.findall(join_pattern, failed_query, re.IGNORECASE)

        table_functions = set()
        for match in from_matches + join_matches:
            if match.startswith("file("):
                # Extract filename from file(...) function, handling nested functions
                # Search for any quoted string within the file() call
                file_match = re.search(r"['\"]([^'\"]+)['\"]", match)
                if file_match:
                    table_files.add(file_match.group(1))
            if match.startswith("numbers(") or match.startswith("file("):
                table_functions.add(match)
            else:
                tables.add(match)

        if not (tables or table_files or table_functions):
            print("WARNING: No tables found in query command")
            return [
                failed_query + ";" if not failed_query.endswith(";") else failed_query
            ]

        # Get all write commands for found tables
        commands_to_reproduce = []
        for table in list(tables) + list(table_files):
            for command in all_fuzzer_commands:
                if command.endswith("FORMAT Values"):
                    # meaningless empty INSERT: "INSERT INTO test FORMAT Values"
                    continue
                if any(
                    command.startswith(write_command)
                    for write_command in self.WRITE_SQL_COMMANDS
                ) and (f" {table} " in command or f"'{table}'" in command):
                    commands_to_reproduce.append(command)

        commands_to_reproduce.append(failed_query)

        if tables:
            # Add table drop commands
            for table in tables:
                commands_to_reproduce.append(f"DROP TABLE IF EXISTS {table}")

        # Ensure all commands end with a semicolon
        commands_to_reproduce = [
            cmd + ";" if not cmd.endswith(";") else cmd for cmd in commands_to_reproduce
        ]

        return commands_to_reproduce

    def _get_all_fuzzer_commands(self):
        assert self.fuzzer_log, "Fuzzer log is not provided"
        error_logs = [
            "Fuzzing step",
            "Query succeeded",
            "Dump of fuzzed AST",
            "Got boring AST",
            "Using seed",
            "Left type:",
            "Timeout exceeded",
            "input block structure:",
            "Code:",
            "Error",
        ]
        lines = Shell.get_output(f"cat {self.fuzzer_log}").splitlines()
        result = []
        in_query = False
        for line in lines:
            if in_query:
                if (
                    any(line.startswith(err_cmd) for err_cmd in error_logs)
                    or not line.strip()
                ):
                    in_query = False
                else:
                    result[-1] += line
            else:
                if any(line.startswith(cmd) for cmd in self.SQL_COMMANDS):
                    in_query = True
                    result.append(line)
                else:
                    if line.startswith(" ") or not line.strip():
                        continue
                    assert line, f"line: [{line}]"
        # Normalize whitespace in commands: server.log collapses consecutive spaces while fuzzer.log may preserve them.
        # This normalization ensures commands from both logs can be matched correctly.
        result = [re.sub(r"\s+", " ", line.strip()) for line in result]
        return result


if __name__ == "__main__":
    # Test:
    fuzzer_log = "./asan_err/fuzzer.log"
    server_log = "./no_stid/server.log"
    FTG = FuzzerLogParser(server_log, fuzzer_log)
    # FTG2 = FuzzerLogParser("", "", stack_trace_str="...")
    result_name, info, files = FTG.parse_failure()
    print("Result name:", result_name)
    print("Info:\n", info)
