import re
from pathlib import Path


class StackTraceReader(object):

    @staticmethod
    def get_stack_trace(file_path=None, stderr=None, max_lines=1000):
        lines = []
        stack_trace_pattern = re.compile(r"<Fatal> BaseDaemon: \d+(?:\.\d+)*\.\s*")
        if file_path:
            assert Path(file_path).is_file(), f"File {file_path} does not exist"
            with open(file_path, "r", errors="replace") as file:
                all_lines = file.readlines()
        elif stderr:
            all_lines = stderr.split("\n")
        else:
            raise Exception("Either file_path or stderr must be provided")

        # Only process last max_lines lines
        last_lines = all_lines[-max_lines:] if len(all_lines) > max_lines else all_lines

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

    @staticmethod
    def get_fatal_error(stderr=None):
        if not stderr:
            return ""

        lines = stderr.split("\n")
        result = []
        in_error = False

        for line in lines:
            if "Logical error:" in line:
                in_error = True
                # Extract the part starting from "Logical error:"
                error_start = line.find("Logical error:")
                result.append(line[error_start:])
            elif in_error:
                # Stop if we hit a line starting with '['
                if line.strip().startswith("["):
                    break
                # Continue collecting lines that are part of the error
                if line.strip():
                    result.append(line)

        return "\n".join(result) if result else ""

    @staticmethod
    def get_fuzzer_query(fuzzer_log, max_lines=200):
        assert Path(fuzzer_log).is_file(), f"File {fuzzer_log} does not exist"

        with open(fuzzer_log, "r", errors="replace") as file:
            all_lines = file.readlines()

        # Only process last max_lines lines
        last_lines = all_lines[-max_lines:] if len(all_lines) > max_lines else all_lines

        # Read backwards to find the last line that starts with SELECT
        sql_keywords = (
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TRUNCATE",
            "WITH",
        )
        for line in reversed(last_lines):
            stripped = line.strip()
            if any(stripped.startswith(keyword) for keyword in sql_keywords):
                return stripped
        return None

    @staticmethod
    def get_trace_and_errors(stderr, max_lines=1000):
        """
        Parse stderr to extract errors and stack traces.
        Returns: (errors, trace, unknown_lines)
        """
        lines = stderr.split("\n")
        if len(lines) > max_lines:
            print(f"Warning: stderr has more than {max_lines} lines. Truncating.")
            lines = lines[-max_lines:]

        lines = [line.strip() for line in lines if line.strip()]

        stack_trace_pattern_if_err = re.compile(r"^\d+(?:\.\d+)*\.\s*")
        stack_trace_pattern_in_log = re.compile(
            r"<Fatal> BaseDaemon: \d+(?:\.\d+)*\.\s*"
        )

        errors_before_trace = []
        errors_after_trace = []
        trace = ""
        changed_settings = []
        unknown_lines = []

        i = 0
        while i < len(lines):
            line = lines[i]

            # Check for error patterns
            if any(
                keyword in line
                for keyword in (
                    "Logical error:",
                    "Changed settings:",
                    "Received signal",
                    "Connection refused",
                )
            ):
                error_lines = [line]
                i += 1
                while i < len(lines):
                    if lines[i].startswith("["):
                        break
                    error_lines.append(lines[i])
                    i += 1
                if trace:
                    errors_after_trace.append(error_lines)
                else:
                    errors_before_trace.append(error_lines)
                continue

            # Check for stack trace pattern
            if stack_trace_pattern_if_err.search(line):
                while i < len(lines) and stack_trace_pattern_if_err.search(lines[i]):
                    match = stack_trace_pattern_if_err.search(lines[i])
                    trace_line = lines[i][match.end() :]
                    trace_line = trace_line.split("ClickHouse/")[-1]
                    trace += trace_line.strip() + "\n"
                    i += 1
                continue

            # Skip known log patterns
            if (
                stack_trace_pattern_in_log.search(line)
                or any(
                    keyword in line
                    for keyword in (
                        "BaseDaemon: ###############",
                        ": Stack trace",
                        "Integrity check of the executable skipped",
                    )
                )
                or line.rstrip().endswith("BaseDaemon:")
            ):
                i += 1
                continue

            # Collect unknown lines
            unknown_lines.append(line)
            i += 1

        for error in errors_before_trace + errors_after_trace:
            for idx, line in enumerate(error):
                if "<Fatal>" in line:
                    error[idx] = line.split("<Fatal>", 1)[1].strip()

        for idx, error in enumerate(errors_before_trace):
            errors_before_trace[idx] = "\n".join(error)
        for idx, error in enumerate(errors_after_trace):
            errors_after_trace[idx] = "\n".join(error)
        return errors_before_trace, trace, errors_after_trace, unknown_lines


if __name__ == "__main__":
    # test
    test_file = "/tmp/stack_trace.txt"
    with open(test_file, "r") as f:
        stderr = f.read()
    errors_before_trace, trace, errors_after_trace, unknown_lines = (
        StackTraceReader.get_trace_and_errors(stderr)
    )
    print(errors_before_trace)
    print("----")
    print(trace)
    print("----")
    print(errors_after_trace)
    print("----")
    print(unknown_lines)
