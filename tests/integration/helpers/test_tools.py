import difflib
import logging
import time
from io import IOBase
from typing import Any, Callable

from .cluster import ClickHouseInstance


class TSV:
    """Helper to get pretty diffs between expected and actual tab-separated value files"""

    def __init__(self, contents: IOBase | str | list[list[str] | str] | "TSV") -> None:
        raw_lines: list[str]
        if isinstance(contents, IOBase):
            raw_lines = contents.readlines()
        elif isinstance(contents, str) or isinstance(contents, str):
            raw_lines = contents.splitlines(True)
        elif isinstance(contents, list):
            raw_lines = [
                "\t".join(map(str, l)) if isinstance(l, list) else str(l)
                for l in contents
            ]
        elif isinstance(contents, TSV):
            self.lines: list[str] = contents.lines
            return
        else:
            raise TypeError(
                "contents must be either file or string or list, actual type: "
                + type(contents).__name__
            )
        self.lines = [l.strip() for l in raw_lines if l.strip()]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TSV):
            return self == TSV(other)
        return self.lines == other.lines

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, TSV):
            return self != TSV(other)
        return self.lines != other.lines

    def diff(self, other: "TSV" | str, n1: str = "", n2: str = "") -> list[str]:
        if not isinstance(other, TSV):
            return self.diff(TSV(other), n1=n1, n2=n2)
        return list(
            line.rstrip()
            for line in difflib.unified_diff(
                self.lines, other.lines, fromfile=n1, tofile=n2
            )
        )[2:]

    def __str__(self) -> str:
        return "\n".join(self.lines)

    def __repr__(self) -> str:
        return self.__str__()

    def __len__(self) -> int:
        return len(self.lines)

    @staticmethod
    def toMat(contents: str) -> list[list[str]]:
        return [line.split("\t") for line in contents.split("\n") if line.strip()]


def assert_eq_with_retry(
    instance: ClickHouseInstance,
    query: str,
    expectation: str,
    retry_count: int = 20,
    sleep_time: float = 0.5,
    stdin: str | None = None,
    timeout: float | None = None,
    settings: dict[str, Any] | None = None,
    user: str | None = None,
    ignore_error: bool = False,
    get_result: Callable[[Any], Any] = lambda x: x,
) -> None:
    expectation_tsv: TSV = TSV(expectation)
    for i in range(retry_count):
        try:
            if (
                TSV(
                    get_result(
                        instance.query(
                            query,
                            user=user,
                            stdin=stdin,
                            timeout=timeout,
                            settings=settings,
                            ignore_error=ignore_error,
                        )
                    )
                )
                == expectation_tsv
            ):
                break
            time.sleep(sleep_time)
        except Exception as ex:
            logging.exception(f"assert_eq_with_retry retry {i+1} exception {ex}")
            time.sleep(sleep_time)
    else:
        val: TSV = TSV(
            get_result(
                instance.query(
                    query,
                    user=user,
                    stdin=stdin,
                    timeout=timeout,
                    settings=settings,
                    ignore_error=ignore_error,
                )
            )
        )
        if expectation_tsv != val:
            raise AssertionError(
                "'{}' != '{}'\n{}".format(
                    expectation_tsv,
                    val,
                    "\n".join(expectation_tsv.diff(val, n1="expectation", n2="query")),
                )
            )


def assert_logs_contain(instance: ClickHouseInstance, substring: str) -> None:
    if not instance.contains_in_log(substring):
        raise AssertionError("'{}' not found in logs".format(substring))


def assert_logs_contain_with_retry(instance: ClickHouseInstance, substring: str, retry_count: int = 20, sleep_time: float = 0.5) -> None:
    for i in range(retry_count):
        try:
            if instance.contains_in_log(substring):
                break
            time.sleep(sleep_time)
        except Exception as ex:
            logging.exception(f"contains_in_log_with_retry retry {i+1} exception {ex}")
            time.sleep(sleep_time)
    else:
        raise AssertionError("'{}' not found in logs".format(substring))


def exec_query_with_retry(
    instance: ClickHouseInstance,
    query: str,
    retry_count: int = 40,
    sleep_time: float = 0.5,
    silent: bool = False,
    settings: dict[str, Any] = {},
    timeout: float = 30,
) -> Any:
    exception: Exception | None = None
    for cnt in range(retry_count):
        try:
            res: Any = instance.query(
                query, timeout=timeout, settings=settings)
            if not silent:
                logging.debug(f"Result of {query} on {cnt} try is {res}")
            return res
        except Exception as ex:
            exception = ex
            if not silent:
                logging.exception(
                    f"Failed to execute query '{query}' on  {cnt} try on instance '{instance.name}' will retry"
                )
            time.sleep(sleep_time)
    else:
        raise exception


def csv_compare(result: str, expected: str) -> str:
    csv_result: TSV = TSV(result)
    csv_expected: TSV = TSV(expected)
    mismatch: list[str] = []
    max_len: int = (
        len(csv_result) if len(csv_result) > len(csv_expected) else len(csv_expected)
    )
    for i in range(max_len):
        if i >= len(csv_result):
            mismatch.append("-[%d]=%s" % (i, csv_expected.lines[i]))
        elif i >= len(csv_expected):
            mismatch.append("+[%d]=%s" % (i, csv_result.lines[i]))
        elif csv_expected.lines[i] != csv_result.lines[i]:
            mismatch.append("-[%d]=%s" % (i, csv_expected.lines[i]))
            mismatch.append("+[%d]=%s" % (i, csv_result.lines[i]))

    return "\n".join(mismatch)


def wait_condition(func: Callable[[], Any], condition: Callable[[Any], bool], max_attempts: int = 10, delay: float = 0.1) -> Any:
    attempts: int = 0
    result: Any = None
    while attempts < max_attempts:
        result = func()
        if condition(result):
            return result
        attempts += 1
        if attempts < max_attempts:
            time.sleep(delay)

    raise Exception(
        f"Function did not satisfy condition after {max_attempts} attempts. Last result:\n{result}"
    )


def get_retry_number(request: Any, fallback: int = 1) -> int:
    retry_number: Any = getattr(request.node, "callspec", None)
    if retry_number is not None:
        retry_number = retry_number.params.get("__pytest_repeat_step_number", fallback)
    else:
        retry_number = fallback
    return retry_number
