import re
import sys

sys.path.append(".")
from ci.praktika.utils import Shell


class FuzzerLogParser:
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

    def __init__(self, server_log, fuzzer_log):
        self.server_log = server_log
        self.fuzzer_log = fuzzer_log

    def parse_failure(self):
        is_logical_error = False
        is_sanitizer_error = False
        is_killed_by_signal = False
        is_segfault = False
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
        ]

        error_output = None
        for name, flag_name, pattern in error_patterns:
            output = Shell.get_output(
                f"rg --text -A 10 -o '{pattern}' {self.server_log} | head -n10"
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
                break

        if not error_output:
            return "Unknown error", "Lost connection to server. See the logs.\n"

        error_lines = error_output.splitlines()
        # keep all lines before next log line
        for i, line in enumerate(error_lines):
            if "] {" in line and "} <" in line or line.startswith("    #"):
                # it's a new log line or sanitizer frame - break
                error_lines = error_lines[:i]
                break
        error_output = "\n".join(error_lines)
        result_name = error_lines[0].removesuffix(".")
        failed_query = ""
        reproduce_commands = []
        stack_trace = self.get_stack_trace()
        stack_trace_id = self.get_stack_trace_id(stack_trace)

        if is_logical_error:
            failed_query = self.get_failed_query()
            if failed_query:
                reproduce_commands = self.get_reproduce_commands(failed_query)
        elif is_killed_by_signal or is_segfault:
            result_name += f" (STID: {stack_trace_id})"
        elif is_sanitizer_error:
            stack_trace = self.get_sanitizer_stack_trace()
            if not stack_trace:
                print("ERROR: Failed to parse sanitizer stack trace")
            stack_trace_id = self.get_sanitizer_stack_trace_id(stack_trace)
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
            assert False, "TODO"

        info = f"Error:\n{error_output}\n"
        if failed_query:
            info += "---\n\nFailed query:\n"
            info += failed_query + "\n"
        if reproduce_commands:
            info += "---\n\nReproduce commands (auto-generated; may require manual adjustment):\n"
            if len(reproduce_commands) > self.MAX_INLINE_REPRODUCE_COMMANDS:
                reproduce_file_sql = workspace_path / "reproduce_commands.sql"
                try:
                    with open(reproduce_file_sql, "w") as f:
                        f.write("\n".join(reproduce_commands))
                    paths.append(reproduce_file_sql)
                    info += f"See file: {reproduce_file_sql}\n"
                except IOError as write_error:
                    info += f"Failed to write reproduce commands file: {write_error}\n"
            else:
                info += "\n".join(reproduce_commands) + "\n"
        if stack_trace:
            info += "---\n\nStack trace:\n"
            info += stack_trace + "\n"

        return result_name, info

    def get_sanitizer_stack_trace(self):
        # return all lines after Sanitizer error starting with "    #DIGITS "
        lines = []
        sanitizer_pattern = re.compile(r"(SUMMARY|ERROR|WARNING): [a-zA-Z]+Sanitizer:")
        stack_frame_pattern = re.compile(r"^\s+#\d+\s+")
        stack_frame_pattern_1st_line = re.compile(r"^\s+#0\s")
        with open(self.server_log, "r", errors="replace") as file:
            all_lines = file.readlines()

        in_sanitizer_trace = False
        for line in all_lines:
            if not in_sanitizer_trace:
                if stack_frame_pattern_1st_line.search(line):
                    in_sanitizer_trace = True
                    lines.append(line.strip())
            else:
                if stack_frame_pattern.match(line):
                    lines.append(line.strip())
                elif in_sanitizer_trace:
                    # End of stack trace
                    break
        return "\n".join(lines) if lines else None

    def get_sanitizer_stack_trace_id(self, stack_trace):
        if not stack_trace:
            return None
        lines = stack_trace.splitlines()
        # keep substring starting from DB:: end all before first (. if DB:: not in the trace - ignore the line
        functions = []
        for line in lines:
            if "DB::" in line:
                # Extract substring starting from DB::
                start_idx = line.find("DB::")
                substring = line[start_idx:]
                # End at first '(' if present
                paren_idx = substring.find("(")
                if paren_idx != -1:
                    substring = substring[:paren_idx]
                functions.append(substring)
        if functions:
            # Generate 4-digit base 10 number from first function name
            func_hash = sum(ord(c) for c in functions[0]) % 10000
            func_part = f"{func_hash:04d}"

            # Generate 4-digit hex number (2 bytes = 4 hex chars)
            # from functions array
            func_str = "".join(functions)
            st_hash = sum(ord(c) for c in func_str) % (16**4)
            st_part = f"{st_hash:04x}"

            stack_trace_id = f"{func_part}-{st_part}"
        else:
            stack_trace_id = None

        return stack_trace_id

    def get_stack_trace(self, max_lines=1000):
        lines = []
        stack_trace_pattern = re.compile(r"<Fatal> BaseDaemon: \d+(?:\.\d+)*\.\s*")
        with open(self.server_log, "r", errors="replace") as file:
            all_lines = file.readlines()

        last_lines = all_lines[-max_lines:]

        for line in reversed(last_lines):
            if "<Fatal> BaseDaemon: Stack trace:" in line:
                break
            match = stack_trace_pattern.search(line)
            if match:
                # Extract only the part after the pattern
                extracted = line[match.end() :]
                # Remove everything before and including 'ClickHouse/' if present
                if "ClickHouse/" in extracted:
                    extracted = extracted.split("ClickHouse/")[-1]
                elif "/./" in extracted:
                    extracted = extracted.split("/./")[-1]
                # Only append if there's meaningful content after extraction
                if extracted.strip():
                    lines.append(extracted)
        lines = [line.strip().replace("\n", "") for line in lines]
        return "\n".join(reversed(lines)) if lines else None

    def get_stack_trace_id(self, stack_trace):
        if not stack_trace:
            return None
        lines = stack_trace.splitlines()
        # keep substring starting from DB:: end all before first (. if DB:: not in the trace - ignore the line
        functions = []
        for line in lines:
            if "DB::" in line:
                # Extract substring starting from DB::
                start_idx = line.find("DB::")
                substring = line[start_idx:]
                # End at first '(' if present
                paren_idx = substring.find("(")
                if paren_idx != -1:
                    substring = substring[:paren_idx]
                functions.append(substring.strip())

        # Exception functions are not really interesting in identifying the problem - so drop these lines
        for i, func in enumerate(reversed(functions)):
            if "DB::Exception" in func:
                cutoff = len(functions) - i
                functions = functions[:cutoff]
                break

        if functions:
            # Generate 4-digit base 10 number from first function name
            func_hash = sum(ord(c) for c in functions[0]) % 10000
            func_part = f"{func_hash:04d}"

            # Generate 4-digit hex number (2 bytes = 4 hex chars)
            # from functions array
            func_str = "".join(functions)
            st_hash = sum(ord(c) for c in func_str) % (16**4)
            st_part = f"{st_hash:04x}"

            stack_trace_id = f"{func_part}-{st_part}"
        else:
            stack_trace_id = None

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
            query_command = failure_output.splitlines()[1]
            return query_command

        assert failure_output, "No failure found in server log"
        failure_first_line = failure_output.splitlines()[0]
        assert failure_first_line, "No failure first line found in server log"
        print(f"Failure first line: {failure_first_line}")
        query_id = failure_first_line.split(" ] {")[1].split("}")[0]
        assert query_id, "No query id found in server log"
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
            raise Exception("No SQL keyword found in query command")
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

        table_finctions = set()
        for match in from_matches + join_matches:
            if match.startswith("file("):
                # Extract filename from file(...) function, handling nested functions
                # Search for any quoted string within the file() call
                file_match = re.search(r"['\"]([^'\"]+)['\"]", match)
                if file_match:
                    table_files.add(file_match.group(1))
            if match.startswith("numbers(") or match.startswith("file("):
                table_finctions.add(match)
            else:
                tables.add(match)
        assert (
            tables or table_files or table_finctions
        ), "No tables found in query command"

        # get all write commands for found tables
        commands_to_reproduce = []
        for table in list(tables) + list(table_files):
            for command in all_fuzzer_commands:
                if any(
                    command.startswith(write_command)
                    for write_command in self.WRITE_SQL_COMMANDS
                ) and (f" {table} " in command or f"'{table}'" in command):
                    commands_to_reproduce.append(command)
        commands_to_reproduce.append(failed_query)

        # add table drop commands
        for table in tables:
            commands_to_reproduce.append(f"DROP TABLE IF EXISTS {table}")

        return commands_to_reproduce

    def _get_all_fuzzer_commands(self):
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
    fuzzer_log = "././logical_error//fuzzer.log"
    server_log = "././logical_error//server.log"
    FTG = FuzzerLogParser(server_log, fuzzer_log)
    result_name, info = FTG.parse_failure()
    print("Result name:", result_name)
    print("Info:\n", info)
