import argparse
import ast
import os
import re
import sys
from pathlib import Path

sys.path.append("./")

from ci.jobs.scripts.find_symbols import DiffToSymbols
from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# Query to fetch failed tests from CIDB for a given PR.
# Only returns tests from commit_sha/check_name combinations with fewer than 20 failures.
# This helps filter out commits with widespread test failures.
FAILED_TESTS_QUERY = """ \
 select distinct test_name
 from (
          select test_name, commit_sha, check_name
          from checks
          where 1
            and pull_request_number = {PR_NUMBER}
            and check_name LIKE '{JOB_TYPE}%'
            and check_status = 'failure'
            and match(test_name, '{TEST_NAME_PATTERN}')
            and test_status = 'FAIL'
            and check_start_time >= now() - interval 300 day
          order by check_start_time desc
              limit 10000
      )
 where (commit_sha, check_name) IN (
     select commit_sha, check_name
     from checks
     where 1
       and pull_request_number = {PR_NUMBER}
   and check_name LIKE '{JOB_TYPE}%'
   and check_status = 'failure'
   and test_status = 'FAIL'
   and check_start_time >= now() - interval 300 day
 group by commit_sha, check_name
 having count(test_name) < 20
     ) \
"""


class Targeting:
    INTEGRATION_JOB_TYPE = "Integration"
    STATELESS_JOB_TYPE = "Stateless"

    def __init__(self, info: Info):
        self.info = info
        if "stateless" in info.job_name.lower():
            self.job_type = self.STATELESS_JOB_TYPE
        elif "integration" in info.job_name.lower():
            self.job_type = self.INTEGRATION_JOB_TYPE
        else:
            self.job_type = None

    def get_changed_tests(self):
        # TODO: add support for integration tests
        result = set()
        if self.info.is_local_run:
            changed_files = Shell.get_output(
                "git diff --name-only $(git merge-base master HEAD)"
            ).splitlines()
        else:
            changed_files = self.info.get_changed_files()
        assert changed_files, "No changed files"

        for fpath in changed_files:
            if re.match(r"tests/queries/0_stateless/\d{5}", fpath):
                if not Path(fpath).exists():
                    print(f"File '{fpath}' was removed — skipping")
                    continue

                print(f"Detected changed test file: '{fpath}'")

                fname = os.path.basename(fpath)
                fname_without_ext = os.path.splitext(fname)[0]

                # Add '.' suffix to precisely match this test only
                result.add(f"{fname_without_ext}.")

            elif fpath.startswith("tests/queries/"):
                # Log any other changed file under tests/queries for future debugging
                print(
                    f"File '{fpath}' changed, but doesn't match expected test pattern"
                )

        return sorted(result)

    def get_previously_failed_tests(self):
        from ci.praktika.cidb import CIDB
        from ci.praktika.settings import Settings

        assert self.job_type, "Unsupported job type"
        assert (
            self.info.pr_number > 0
        ), "Find tests by previous failures applicable only for PRs"

        tests = []
        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
        if self.job_type == self.INTEGRATION_JOB_TYPE:
            test_name_pattern = "^test_"
        elif self.job_type == self.STATELESS_JOB_TYPE:
            test_name_pattern = "^[0-9]{5}_"
        else:
            assert False, f"Not supported job type [{self.job_type}]"
        query = FAILED_TESTS_QUERY.format(
            PR_NUMBER=self.info.pr_number,
            JOB_TYPE=self.job_type,
            TEST_NAME_PATTERN=test_name_pattern,
        )
        query_result = cidb.query(query, log_level="")
        # Parse test names from the query result
        for line in query_result.strip().split("\n"):
            if line.strip():
                # Split by whitespace and get the first column (test_name)
                parts = line.split()
                if parts:
                    test_name = parts[0]
                    tests.append(test_name)
        print(f"Parsed {len(tests)} test names: {tests}")
        tests = list(set(tests))
        return sorted(tests)

    def get_tests_by_changed_symbols(self, symbols):
        """
        Returns a mapping from symbol to a list of tests that cover it.
        """
        SYMBOL_TO_TESTS_QUERY = """
        SELECT groupArray(test_name) as tests
        from checks_coverage_inverted
        where 1
        and check_start_time > now() - interval 3 days
        and check_name LIKE '{JOB_TYPE}%'
        and symbol = '{SYMBOL}'
        """
        symbol_to_tests = {}
        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
        for symbol in symbols:
            query = SYMBOL_TO_TESTS_QUERY.format(JOB_TYPE=self.job_type, SYMBOL=symbol)
            result = cidb.query(query, log_level="")
            # Parse the ClickHouse Array result
            if result.strip():
                try:
                    tests = ast.literal_eval(result.strip())
                    symbol_to_tests[symbol] = tests if isinstance(tests, list) else []
                except (ValueError, SyntaxError):
                    print(f"Failed to parse tests for symbol '{symbol}': {result}")
                    symbol_to_tests[symbol] = []
            else:
                symbol_to_tests[symbol] = []

        return symbol_to_tests

    def get_changed_or_new_tests_with_info(self):
        tests = self.get_changed_tests()
        info = f"Found {len(tests)} changed or new tests:\n"
        for test in tests[:200]:
            info += f" - {test}\n"
        return tests, Result(
            name="tests that were changed or added",
            status=Result.StatusExtended.OK,
            info=info,
        )

    def get_previously_failed_tests_with_info(self):
        tests = self.get_previously_failed_tests()
        # TODO: add job name to the result.info
        info = f"Found {len(tests)} previously failed tests:\n"
        for test in tests[:200]:
            info += f" - {test}\n"
        return tests, Result(
            name="tests that failed in previous runs",
            status=Result.StatusExtended.OK,
            info=info,
        )

    def get_map_file_line_to_symbol_tests(self, binary_path):
        """
        Build a mapping from (file, line) to (resolved symbol, [tests]).
        Returns:
            dict: {(file, line): (symbol or None, [tests])}
        """
        assert self.info.pr_number > 0, "Find tests by diff applicable for PRs only"
        dts = DiffToSymbols(binary_path, self.info.pr_number)
        file_line_to_address_linkagename_symbol = dts.get_map_line_to_symbol()
        not_resolved_file_lines = {}
        symbols_to_file_lines = {}

        for (file_, line_), (
            address,
            linkage_name,
            symbol,
        ) in file_line_to_address_linkagename_symbol.items():
            if symbol in symbols_to_file_lines:
                continue
            if not symbol:
                if file_ not in not_resolved_file_lines:
                    not_resolved_file_lines[file_] = set()
                if (
                    line_ - 1 in not_resolved_file_lines[file_]
                ):  # skip consecutive lines
                    continue
                not_resolved_file_lines[file_].add(line_)
            else:
                symbols_to_file_lines[symbol] = (file_, line_)

        # Fetch mapping of symbols to tests from the coverage database
        symbol_to_tests = self.get_tests_by_changed_symbols(
            list(symbols_to_file_lines.keys())
        )
        map_file_line_to_test = {}
        for symbol, tests in symbol_to_tests.items():
            map_file_line_to_test[
                (symbols_to_file_lines[symbol][0], symbols_to_file_lines[symbol][1])
            ] = (symbol, list(set(tests)))
        for file_, lines in not_resolved_file_lines.items():
            for line in lines:
                map_file_line_to_test[(file_, line)] = (None, [])

        return map_file_line_to_test

    def get_most_relevant_tests(self, binary_path, max_tests_per_symbol=100):
        """
        1. Makes a best effort to get changed symbols by reading the PR diff and the ClickHouse binary DWARF.
        2. Gets a list of tests that cover each found symbol from the coverage database.
        3. Skips symbols with more than 'max_tests_per_symbol' tests (too common code).
        4. Returns the unique tests and a Result with info about the findings.
        """

        file_line_to_symbol_tests = self.get_map_file_line_to_symbol_tests(binary_path)
        not_resolved_file_lines = {}
        resolved_file_lines = {}
        symbols_to_tests = {}
        selected_tests = set()

        for (file_, line_), (symbol, tests) in file_line_to_symbol_tests.items():
            if not tests:
                if (file_, line_) not in not_resolved_file_lines:
                    not_resolved_file_lines[(file_, line_)] = []
                not_resolved_file_lines[(file_, line_)] = symbol
            else:
                if symbol in symbols_to_tests:
                    continue
                symbols_to_tests[symbol] = tests
                resolved_file_lines[(file_, line_)] = (symbol, tests)

        info = "Tests not found for lines:\n"
        for (file_, line), symbol in not_resolved_file_lines.items():
            info += f"  {file_}:{line} -> symbol: {symbol[:70] + '...' if symbol else 'NOT FOUND'}\n"
        info = "Tests found for lines:\n"
        if not resolved_file_lines:
            info += "  No updates in source code\n"
        else:
            for (file_, line), (symbol, tests) in resolved_file_lines.items():
                info += f"  {file_}:{line} -> symbol: {symbol[:70]}...\n"
                if len(tests) > max_tests_per_symbol:
                    info += f"    skipping {len(tests)} tests (too common code)\n"
                else:
                    selected_tests.update(tests)
            for test in tests[:10]:
                info += f"  - {test}\n"
            if len(tests) > 10:
                info += f"    ... and {len(tests) - 10} more tests\n"
        info += f"Total unique tests: {len(selected_tests)}\n"
        selected_tests = list(selected_tests)
        return selected_tests, Result(
            name="tests found by coverage", status=Result.StatusExtended.OK, info=info
        )

    def get_all_relevant_tests_with_info(self, ch_path):
        tests = set()
        results = []

        # Integration tests run changed test suboptimally (entire module), it might be too long
        # limit it to stateless tests only
        if self.job_type == self.STATELESS_JOB_TYPE:
            changed_tests, result = self.get_changed_or_new_tests_with_info()
            tests.update(changed_tests)
            results.append(result)

        previously_failed_tests, result = self.get_previously_failed_tests_with_info()
        tests.update(previously_failed_tests)
        results.append(result)

        # TODO: Add coverage supoort for Integration tests
        if self.job_type == self.STATELESS_JOB_TYPE:
            try:
                covering_tests, result = self.get_most_relevant_tests(ch_path)
                tests.update(covering_tests)
                results.append(result)
            except Exception as e:
                print(
                    f"WARNING: Failed to get coverage-based tests (best effort): {e}",
                    file=sys.stderr,
                )
                results.append(
                    Result(
                        name="tests found by coverage",
                        status=Result.StatusExtended.OK,
                        info=f"Skipped: {e}",
                    )
                )

        return tests, Result(
            name="Fetch relevant tests",
            status=Result.Status.SUCCESS,
            info=f"Found {len(tests)} relevant tests",
            results=results,
        )


if __name__ == "__main__":
    # local run tests
    parser = argparse.ArgumentParser(
        description="List changed symbols for a PR by parsing the diff and querying ClickHouse."
    )
    parser.add_argument("pr", help="Pull request number")
    parser.add_argument(
        "clickhouse_path",
        help='Path to the clickhouse binary (executed as "clickhouse local")',
    )
    args = parser.parse_args()

    class InfoLocalTest:
        pr_number = int(args.pr)
        is_local_run = True
        job_name = "Stateless"

    info = InfoLocalTest()
    targeting = Targeting(info)
    file_line_to_symbol_tests = targeting.get_map_file_line_to_symbol_tests(
        args.clickhouse_path
    )

    print("\nNo tests found for lines:")
    for (file, line), (symbol, tests) in file_line_to_symbol_tests.items():
        if tests:
            continue
        print(
            f"{file}:{line} -> symbol [{symbol[:70] + '...' if symbol else 'NOT FOUND'}"
        )

    print("\nTests found for lines:")
    for (file, line), (symbol, tests) in file_line_to_symbol_tests.items():
        if not tests:
            continue
        print(f"{file}:{line} -> symbol [{symbol[:70]}...]:")
        for test in tests[:10]:
            print(f" - {test}")
        if len(tests) > 10:
            print(f" - ... and {len(tests) - 10} more tests")
