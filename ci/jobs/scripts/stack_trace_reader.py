import re
from pathlib import Path


class StackTraceReader(object):

    @staticmethod
    def get_stack_trace(file_path, max_lines=1000):
        assert Path(file_path).is_file(), f"File {file_path} does not exist"

        lines = []
        stack_trace_pattern = re.compile(r"<Fatal> BaseDaemon: \d{1,2}\. ")

        with open(file_path, "r", errors="replace") as file:
            all_lines = file.readlines()

        # Only process last max_lines lines
        last_lines = all_lines[-max_lines:] if len(all_lines) > max_lines else all_lines

        # Read backwards from the end
        for line in reversed(last_lines):
            if "<Fatal> BaseDaemon: Stack trace:" in line:
                break
            # Only keep lines that match the stack trace pattern
            match = stack_trace_pattern.search(line)
            if match:
                # Extract only the part after the pattern
                extracted = line[match.end() :]
                # Remove everything before and including 'ClickHouse/' if present
                if "ClickHouse/" in extracted:
                    clickhouse_idx = extracted.find("ClickHouse/")
                    extracted = extracted[clickhouse_idx + len("ClickHouse/") :]
                    clickhouse_idx = extracted.find("ClickHouse/")
                    extracted = extracted[clickhouse_idx + len("ClickHouse/") :]
                lines.append(extracted)

        # Reverse to get original order
        lines.reverse()
        return "".join(lines) if lines else None

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


if __name__ == "__main__":
    # test
    test_file = "fatal.log"
    print(StackTraceReader.get_stack_trace(test_file))
