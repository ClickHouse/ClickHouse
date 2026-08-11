"""
Tests for the `server_data_manipulation_in_stateless_tests` style check in
`ci/jobs/check_style.py`.

Stateless tests must not modify the server's data on disk; the check flags stateless `.sh`
tests that fetch a server-side filesystem path from a system table and also run
file-modifying shell commands. The mutation command must be recognized in every position a
command can appear in - in particular a one-liner wrapped in shell keywords
(`if true; then rm ...; fi`) must not slip through.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `praktika` is imported as a top-level module by the style-check job.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs import check_style

FETCH_PART_PATH = (
    'path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts'
    " WHERE table = 't' AND active LIMIT 1\")\n"
)


def _run(tmp_path, content):
    test_file = tmp_path / "0_stateless" / "09999_probe.sh"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text(content)
    return check_style.check_no_server_data_manipulation([str(test_file)])


def test_plain_mutation_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'rm -f "$path/data.bin"\n')


def test_mutation_after_semicolon_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'true; rm -f "$path/data.bin"\n')


def test_mutation_wrapped_in_if_then_is_flagged(tmp_path):
    # Regression case from the review of #114070: `then` was not a command boundary,
    # so a compressed one-line wrapper bypassed the check.
    assert _run(
        tmp_path, FETCH_PART_PATH + 'if true; then rm -f "$path/data.bin"; fi\n'
    )


def test_mutation_in_while_do_loop_is_flagged(tmp_path):
    assert _run(
        tmp_path,
        FETCH_PART_PATH + 'while true; do rm -f "$path/data.bin"; break; done\n',
    )


def test_mutation_in_subshell_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + '(cd "$path" && rm -f data.bin)\n')


def test_mutation_in_group_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + '{ rm -f "$path/data.bin"; }\n')


def test_redirect_into_server_path_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'echo broken > "$path/data.bin"\n')


def test_no_server_path_fetch_is_not_flagged(tmp_path):
    # File mutations over the test's own scratch files are fine.
    assert not _run(tmp_path, 'f=$(mktemp)\nrm -f "$f"\n')


def test_fetch_without_mutation_is_not_flagged(tmp_path):
    assert not _run(tmp_path, FETCH_PART_PATH + 'echo "$path"\n')


def test_mktemp_scratch_redirect_is_not_flagged(tmp_path):
    # Command substitution in a redirect only counts when the same line pulls the
    # path out of a system table; `$(mktemp)` stays allowed.
    assert not _run(tmp_path, FETCH_PART_PATH + 'echo x > "$(mktemp)"\n')
