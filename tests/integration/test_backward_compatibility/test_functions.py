# pylint: disable=unused-argument
# pylint: disable=line-too-long
# pylint: disable=call-var-from-loop
# pylint: disable=redefined-outer-name

import logging

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

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

    def get_final_value_unhex(node, function_name, value):
        return node.query(
            f"select finalizeAggregation(unhex('{value}')::AggregateFunction({function_name}, String))"
        ).strip()

    for aggregate_function in aggregate_functions:
        logging.info("Checking %s", aggregate_function)

        try:
            backward_state = get_aggregate_state_hex(backward, aggregate_function)
        except QueryRuntimeException as e:
            error_message = str(e)
            allowed_errors = [
                "ILLEGAL_TYPE_OF_ARGUMENT",
                # sequenceNextNode() and friends
                "UNKNOWN_AGGREGATE_FUNCTION",
                # Function X takes exactly one parameter:
                "NUMBER_OF_ARGUMENTS_DOESNT_MATCH",
                # Function X takes at least one argument
                "TOO_FEW_ARGUMENTS_FOR_FUNCTION",
                # Function X accepts at most 3 arguments, Y given
                "TOO_MANY_ARGUMENTS_FOR_FUNCTION",
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
            allowed_changes_if_result_is_the_same = ["anyHeavy"]

            if aggregate_function in allowed_changes_if_result_is_the_same:
                backward_final_from_upstream = get_final_value_unhex(
                    backward, aggregate_function, upstream_state
                )
                upstream_final_from_backward = get_final_value_unhex(
                    upstream, aggregate_function, backward_state
                )

                if backward_final_from_upstream == upstream_final_from_backward:
                    logging.info(
                        "OK %s (but different intermediate states)", aggregate_function
                    )
                    passed += 1
                else:
                    logging.error(
                        "Failed %s, Intermediate: %s (backward) != %s (upstream). Final from intermediate: %s (backward from upstream state) != %s (upstream from backward state)",
                        aggregate_function,
                        backward_state,
                        upstream_state,
                        backward_final_from_upstream,
                        upstream_final_from_backward,
                    )
                    failed += 1
            else:
                logging.error(
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
    if (
        upstream.is_built_with_thread_sanitizer()
        or upstream.is_built_with_memory_sanitizer()
        or upstream.is_built_with_address_sanitizer()
    ):
        pytest.skip("The test is slow in builds with sanitizer")

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
        # The argument of this function is not a seed, but an arbitrary expression needed for bypassing common subexpression elimination.
        "rand",
        "rand64",
        "randConstant",
        "randCanonical",
        "generateUUIDv4",
        "generateULID",
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
        # These functions require more than one argument.
        "parseDateTimeInJodaSyntaxOrZero",
        "parseDateTimeInJodaSyntaxOrNull",
        "parseDateTimeOrNull",
        "parseDateTimeOrZero",
        "parseDateTime",
        # The argument is effectively a disk name (and we don't have one with name foo)
        "filesystemUnreserved",
        "filesystemCapacity",
        "filesystemAvailable",
        # Exclude it for now. Looks like the result depends on the build type.
        "farmHash64",
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
                "ILLEGAL_TYPE_OF_ARGUMENT",
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
                "NUMBER_OF_ARGUMENTS_DOESNT_MATCH",
                # Function X takes at least one argument
                "TOO_FEW_ARGUMENTS_FOR_FUNCTION",
                # Function X accepts at most 3 arguments, Y given
                "TOO_MANY_ARGUMENTS_FOR_FUNCTION",
                # The function 'X' can only be used as a window function
                "BAD_ARGUMENTS",
                # String foo is obviously not a valid IP address.
                "CANNOT_PARSE_IPV4",
                "CANNOT_PARSE_IPV6",
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
