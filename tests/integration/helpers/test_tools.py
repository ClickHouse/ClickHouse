import difflib
import logging
import re
import time
from io import IOBase

import pytest


class TSV:
    """Helper to get pretty diffs between expected and actual tab-separated value files"""

    def __init__(self, contents):
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
            self.lines = contents.lines
            return
        else:
            raise TypeError(
                "contents must be either file or string or list, actual type: "
                + type(contents).__name__
            )
        self.lines = [l.strip() for l in raw_lines if l.strip()]

    def __eq__(self, other):
        if not isinstance(other, TSV):
            return self == TSV(other)
        return self.lines == other.lines

    def __ne__(self, other):
        if not isinstance(other, TSV):
            return self != TSV(other)
        return self.lines != other.lines

    def diff(self, other, n1="", n2=""):
        if not isinstance(other, TSV):
            return self.diff(TSV(other), n1=n1, n2=n2)
        return list(
            line.rstrip()
            for line in difflib.unified_diff(
                self.lines, other.lines, fromfile=n1, tofile=n2
            )
        )[2:]

    def __str__(self):
        return "\n".join(self.lines)

    def __repr__(self):
        return self.__str__()

    def __len__(self):
        return len(self.lines)

    @staticmethod
    def toMat(contents):
        return [line.split("\t") for line in contents.split("\n") if line.strip()]


def assert_eq_with_retry(
    instance,
    query,
    expectation,
    retry_count=20,
    sleep_time=0.5,
    stdin=None,
    timeout=None,
    settings=None,
    user=None,
    ignore_error=False,
    get_result=lambda x: x,
):
    expectation_tsv = TSV(expectation)
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
            logging.exception("assert_eq_with_retry retry %d exception %s", i + 1, str(ex))
            time.sleep(sleep_time)
    else:
        val = TSV(
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


def assert_logs_contain(instance, substring):
    if not instance.contains_in_log(substring):
        raise AssertionError(f"'{substring}' not found in logs")


def assert_logs_contain_with_retry(instance, substring, retry_count=20, sleep_time=0.5):
    for i in range(retry_count):
        try:
            if instance.contains_in_log(substring):
                break
            time.sleep(sleep_time)
        except Exception as ex:
            logging.exception("contains_in_log_with_retry retry %d exception %s", i + 1, str(ex))
            time.sleep(sleep_time)
    else:
        raise AssertionError(f"'{substring}' not found in logs")


def exec_query_with_retry(
    instance, query, retry_count=40, sleep_time=0.5, silent=False, settings=None
):
    exception = None
    for cnt in range(retry_count):
        try:
            res = instance.query(query, timeout=30, settings=settings if settings is not None else {})
            if not silent:
                logging.debug("Result of %s on %d try is %s", query, cnt, res)
            break
        except Exception as ex:
            exception = ex
            if not silent:
                logging.exception(
                    "Failed to execute query '%s' on %d try on instance '%s' will retry", query, cnt, instance.name
                )
            time.sleep(sleep_time)
    else:
        raise exception


def csv_compare(result, expected):
    csv_result = TSV(result)
    csv_expected = TSV(expected)
    mismatch = []
    max_len = (
        len(csv_result) if len(csv_result) > len(csv_expected) else len(csv_expected)
    )
    for i in range(max_len):
        if i >= len(csv_result):
            mismatch.append(f"-[{i}]={csv_expected.lines[i]}")
        elif i >= len(csv_expected):
            mismatch.append(f"+[{i}]={csv_result.lines[i]}")
        elif csv_expected.lines[i] != csv_result.lines[i]:
            mismatch.append(f"-[{i}]={csv_expected.lines[i]}")
            mismatch.append(f"+[{i}]={csv_result.lines[i]}")

    return "\n".join(mismatch)


def assert_query_fails(instance, sql, code=None, message=None):
    match = "" if code is None else f"\nCode: {code}\\."
    if message is not None:
        match += f".*{message}"
    with pytest.raises(
        Exception, match=re.compile(match, re.MULTILINE)
    ):
        instance.query(f"/* expecting error: {match} */ {sql}")
