# pylint: disable=unused-argument
# pylint: disable=line-too-long
# pylint: disable=call-var-from-loop
# pylint: disable=redefined-outer-name

import logging
import pytest
from helpers.cluster import ClickHouseCluster, CLICKHOUSE_CI_MIN_TESTED_VERSION
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
upstream = cluster.add_instance("upstream", use_old_analyzer=True)
backward = cluster.add_instance(
    "backward",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_aggregate_states(start_cluster):
    """
    This test goes though all aggregate functions that:
    - has only one argument
    - support string as an argument

    And do a simple check by creating the aggregate state with one string.

    Yes this is not covers everything (does not cover functions with
    different number of arguments, types, different states in case of
    multiple values - uniqCombined, but as for uniqCombined it will be
    checked via uniqHLL12), but at least something.

    And about type, String had been selected, since it more likely that
    there will be used some hash function internally.
    """

    aggregate_functions = backward.query(
        """
        SELECT if(NOT empty(alias_to), alias_to, name)
        FROM system.functions
        WHERE is_aggregate = 1
    """
    )
    aggregate_functions = aggregate_functions.strip().split("\n")
    aggregate_functions = map(lambda x: x.strip(), aggregate_functions)

    aggregate_functions = list(aggregate_functions)
    logging.info("Got %s aggregate functions", len(aggregate_functions))

    skipped = 0
    failed = 0
    passed = 0

    def get_aggregate_state_hex(node, function_name):
        return node.query(
            f"select hex(initializeAggregation('{function_name}State', 'foo'))"
        ).strip()

    for aggregate_function in aggregate_functions:
        logging.info("Checking %s", aggregate_function)

        try:
            backward_state = get_aggregate_state_hex(backward, aggregate_function)
        except QueryRuntimeException as e:
            error_message = str(e)
            allowed_errors = [
                "NUMBER_OF_ARGUMENTS_DOESNT_MATCH",
                "ILLEGAL_TYPE_OF_ARGUMENT",
                # sequenceNextNode() and friends
                "UNKNOWN_AGGREGATE_FUNCTION",
                # Function X takes exactly one parameter:
                # The function 'X' can only be used as a window function
                "BAD_ARGUMENTS",
                # aggThrow
                "AGGREGATE_FUNCTION_THROW",
            ]
            if any(map(lambda x: x in error_message, allowed_errors)):
                logging.info("Skipping %s", aggregate_function)
                skipped += 1
                continue
            logging.exception("Failed %s", aggregate_function)
            failed += 1
            continue

        upstream_state = get_aggregate_state_hex(upstream, aggregate_function)
        if upstream_state != backward_state:
            logging.info(
                "Failed %s, %s (backward) != %s (upstream)",
                aggregate_function,
                backward_state,
                upstream_state,
            )
            failed += 1
        else:
            logging.info("OK %s", aggregate_function)
            passed += 1

    logging.info(
        "Aggregate functions: %s, Failed: %s, skipped: %s, passed: %s",
        len(aggregate_functions),
        failed,
        skipped,
        passed,
    )
    assert failed == 0
    assert passed > 0
    assert failed + passed + skipped == len(aggregate_functions)


def test_string_functions(start_cluster):
    functions = backward.query(
        """
        SELECT if(NOT empty(alias_to), alias_to, name)
        FROM system.functions
        WHERE is_aggregate = 0
    """
    )
    functions = functions.strip().split("\n")
    functions = map(lambda x: x.strip(), functions)

    excludes = [
        "rand",
        "rand64",
        "randConstant",
        "generateUUIDv4",
        # Syntax error otherwise
        "position",
        "substring",
        "CAST",
        "getTypeSerializationStreams",
        # NOTE: no need to ignore now()/now64() since they will fail because they don't accept any argument
        # 22.8 Backward Incompatible Change: Extended range of Date32
        "toDate32OrZero",
        "toDate32OrDefault",
        # 23.9 changed the base64-handling library from Turbo base64 to aklomp-base64. They differ in the way they deal with base64 values
        # that are not properly padded by '=', for example below test value v='foo'. (Depending on the specification/context, padding is
        # mandatory or optional). The former lib produces a value based on implicit padding, the latter lib throws an error.
        "FROM_BASE64",
        "base64Decode",
        # PR #56913 (in v23.11) corrected the way tryBase64Decode() behaved with invalid inputs. Old versions return garbage, new versions
        # return an empty string (as it was always documented).
        "tryBase64Decode",
        # Removed in 23.9
        "meiliMatch",
    ]
    functions = filter(lambda x: x not in excludes, functions)

    functions = list(functions)
    logging.info("Got %s functions", len(functions))

    skipped = 0
    failed = 0
    passed = 0

    def get_function_value(node, function_name, value):
        return node.query(f"select {function_name}('{value}')").strip()

    v = "foo"
    for function in functions:
        logging.info("Checking %s('%s')", function, v)

        try:
            backward_value = get_function_value(backward, function, v)
        except QueryRuntimeException as e:
            error_message = str(e)
            allowed_errors = [
                # Messages
                "Cannot load time zone ",
                "No macro ",
                "Should start with ",  # POINT/POLYGON/...
                "Cannot read input: expected a digit but got something else:",
                # ErrorCodes
                "NUMBER_OF_ARGUMENTS_DOESNT_MATCH",
                "ILLEGAL_TYPE_OF_ARGUMENT",
                "TOO_FEW_ARGUMENTS_FOR_FUNCTION",
                "DICTIONARIES_WAS_NOT_LOADED",
                "CANNOT_PARSE_UUID",
                "CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING",
                "ILLEGAL_COLUMN",
                "TYPE_MISMATCH",
                "SUPPORT_IS_DISABLED",
                "CANNOT_PARSE_DATE",
                "UNKNOWN_SETTING",
                "CANNOT_PARSE_BOOL",
                "FILE_DOESNT_EXIST",
                "NOT_IMPLEMENTED",
                "BAD_GET",
                "UNKNOWN_TYPE",
                # addressToSymbol
                "FUNCTION_NOT_ALLOWED",
                # Date functions
                "CANNOT_PARSE_TEXT",
                "CANNOT_PARSE_DATETIME",
                # Function X takes exactly one parameter:
                # The function 'X' can only be used as a window function
                "BAD_ARGUMENTS",
            ]
            if any(map(lambda x: x in error_message, allowed_errors)):
                logging.info("Skipping %s", function)
                skipped += 1
                continue
            logging.exception("Failed %s", function)
            failed += 1
            continue

        upstream_value = get_function_value(upstream, function, v)
        if upstream_value != backward_value:
            logging.warning(
                "Failed %s('%s') %s (backward) != %s (upstream)",
                function,
                v,
                backward_value,
                upstream_value,
            )
            failed += 1
        else:
            logging.info("OK %s", function)
            passed += 1

    logging.info(
        "Functions: %s, failed: %s, skipped: %s, passed: %s",
        len(functions),
        failed,
        skipped,
        passed,
    )
    assert failed == 0
    assert passed > 0
    assert failed + passed + skipped == len(functions)
