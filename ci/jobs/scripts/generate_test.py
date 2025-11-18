import re
import sys

sys.path.append(".")
from ci.praktika.utils import Shell


class FuzzerTestGenerator:
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

    def get_failed_query(self):
        # TODO: Fetch the failed query from fuzzer.log instead of server.log to ensure exact matching.
        # The server.log may normalize whitespace or format queries differently, making it difficult
        # to locate the corresponding query and its dependencies in fuzzer.log.
        failure_output = Shell.get_output(
            f"grep -A10 -a 'Logical error:' {self.server_log}", verbose=True
        )
        assert failure_output, "No failure found in server log"
        failure_first_line = failure_output.splitlines()[0]
        assert failure_first_line, "No failure first line found in server log"
        query_id = failure_first_line.split(" ] {")[1].split("}")[0]
        assert query_id, "No query id found in server log"
        query_command = Shell.get_output(
            f"grep -a '{query_id}' {self.server_log} | head -n1"
        )
        assert query_command, "No query command found in server log"
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

    def get_reproduce_commands(self, failed_query=""):
        query_command = failed_query or self.get_failed_query()

        all_fuzzer_commands = self._get_all_fuzzer_commands()
        assert all_fuzzer_commands, "No fuzzer commands found"
        assert (
            query_command in all_fuzzer_commands
        ), f"Query command '{query_command}' not found in fuzzer log"
        query_index = all_fuzzer_commands.index(query_command)
        all_fuzzer_commands = all_fuzzer_commands[:query_index]

        # get all tables from the command
        # Match table names after FROM and various JOIN types (LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.)
        tables = set()
        from_pattern = r"\bFROM\s+([a-zA-Z0-9_.]+)"
        join_pattern = r"\bJOIN\s+([a-zA-Z0-9_.]+)"
        from_matches = re.findall(from_pattern, query_command, re.IGNORECASE)
        join_matches = re.findall(join_pattern, query_command, re.IGNORECASE)
        tables.update(from_matches)
        tables.update(join_matches)
        assert tables, "No tables found in query command"

        # get all write commands for found tables
        commands_to_reproduce = []
        for table in tables:
            for command in all_fuzzer_commands:
                if (
                    any(
                        command.startswith(write_command)
                        for write_command in self.WRITE_SQL_COMMANDS
                    )
                    and f" {table} " in command
                ):
                    commands_to_reproduce.append(command)
        commands_to_reproduce.append(query_command)

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
    fuzzer_log = "fuzzer.log"
    server_log = "server.log"
    FTG = FuzzerTestGenerator(server_log, fuzzer_log)
    failed_query = FTG.get_failed_query()
    print("Failed query:")
    print(failed_query)
    print("\nCommands to reproduce:")
    print("\n".join(FTG.get_reproduce_commands(failed_query)))
