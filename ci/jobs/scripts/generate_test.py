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

    def __init__(self, server_log, fuzzer_log):
        self.server_log = server_log
        self.fuzzer_log = fuzzer_log

    def get_reproduce_commands(self):

        # get query command
        failure_output = Shell.get_output(
            f"cat {self.server_log} | grep -A10 'Logical error:'"
        )
        assert failure_output, "No failure found in server log"
        failure_first_line = failure_output.splitlines()[0]
        assert failure_first_line, "No failure first line found in server log"
        query_id = failure_first_line.split(" ] {")[1].split("}")[0]
        assert query_id, "No query id found in server log"
        query_comand = Shell.get_output(
            f"cat {self.server_log} | grep '{query_id}' | head -n1"
        )
        assert query_comand, "No query command found in server log"
        query_comand = query_comand.split(" (stage:")[0]
        sql_keywords = [
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
        ]
        for keyword in sql_keywords:
            if keyword in query_comand:
                keyword_pos = query_comand.find(keyword)
                query_comand = query_comand[keyword_pos:]
                break
        else:
            raise Exception("No SQL keyword found in query command")

        all_fuzzer_commands = self._get_all_fuzzer_commands()
        assert all_fuzzer_commands, "No fuzzer commands found"
        assert (
            query_comand in all_fuzzer_commands
        ), f"Query command '{query_comand}' not found in fuzzer log"
        all_fuzzer_commands = all_fuzzer_commands[
            : list.index(all_fuzzer_commands, query_comand)
        ]

        # get all tabels from the command
        # Match table names after FROM and various JOIN types (LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.)
        tabeles = set()
        from_pattern = r"\bFROM\s+([a-zA-Z0-9_]+)"
        join_pattern = r"\bJOIN\s+([a-zA-Z0-9_]+)"
        from_matches = re.findall(from_pattern, query_comand, re.IGNORECASE)
        join_matches = re.findall(join_pattern, query_comand, re.IGNORECASE)
        tabeles.update(from_matches)
        tabeles.update(join_matches)
        assert tabeles, "No tables found in query command"

        # get all write commands for found tabels
        commands_to_reproduce = []
        for table in tabeles:
            for command in all_fuzzer_commands:
                if any(
                    command.startswith(read_cmd) for read_cmd in ["SELECT", "EXPLAIN"]
                ):
                    continue
                if f" {table} " in command:
                    commands_to_reproduce.append(command)
        assert commands_to_reproduce, "No write commands found for tables"
        commands_to_reproduce.append(query_comand)

        # add table drop commands
        for table in tabeles:
            commands_to_reproduce.append(f"DROP TABLE {table}")

        return commands_to_reproduce

    def _get_all_fuzzer_commands(self):
        lines = Shell.get_output(f"cat {self.fuzzer_log}").splitlines()
        result = []
        for line in lines:
            if line.startswith("Error") or line.startswith("[") or not line.strip():
                continue
            if re.match(r"^\d+\.", line):
                continue
            if any(
                line.startswith(cmd)
                for cmd in [
                    "Fuzzing step",
                    "Query succeeded",
                    "Dump of fuzzed AST",
                    "Got boring AST",
                    "Using seed",
                    "Left type:",
                    "Timeout exceeded",
                    "input block structure:",
                    "Code:",
                ]
            ):
                continue
            result.append(line)

        result_with_joned_multiline_commands = []
        for line in result:
            if any(line.startswith(cmd) for cmd in self.SQL_COMMANDS):
                result_with_joned_multiline_commands.append(line)
            elif line.startswith("("):
                result_with_joned_multiline_commands[-1] += " " + line
            else:
                assert (
                    False
                ), f"Unknown line: '{line}' | '{result_with_joned_multiline_commands[-1]}'"

        # sanity check
        for command in result_with_joned_multiline_commands:
            assert any(
                command.startswith(sql_cmd) for sql_cmd in self.SQL_COMMANDS
            ), f"No SQL command found in command: {command}"
        return result_with_joned_multiline_commands

    def generate_test(self):
        pass


if __name__ == "__main__":
    fuzzer_log = "fuzzer.log"
    server_log = "server.log"
    print(
        "\n".join(FuzzerTestGenerator(server_log, fuzzer_log).get_reproduce_commands())
    )
