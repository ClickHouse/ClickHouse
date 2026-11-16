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


def test_mutation_behind_command_wrapper_is_flagged(tmp_path):
    # Regression case from the review of #114070: only a bare verb (optionally after
    # `sudo`) was recognized, so any wrapper word in front of it bypassed the check.
    assert _run(tmp_path, FETCH_PART_PATH + 'command rm -f "$path/data.bin"\n')


def test_mutation_behind_time_wrapper_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'time rm -f "$path/data.bin"\n')


def test_mutation_behind_leading_assignment_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'FOO=1 rm -f "$path/data.bin"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'FOO="a b" rm -f "$path/data.bin"\n')


def test_mutation_behind_wrapper_with_arguments_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'timeout 60 rm -f "$path/data.bin"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'sudo -n rm -f "$path/data.bin"\n')
    assert _run(
        tmp_path,
        FETCH_PART_PATH + 'timeout --signal=KILL 60 rm -f "$path/data.bin"\n',
    )


def test_mutation_behind_wrapper_option_with_value_is_flagged(tmp_path):
    # Regression case from the review of #114070: a wrapper option that consumes a
    # following string value stopped the skipped prefix before the mutation verb.
    assert _run(tmp_path, FETCH_PART_PATH + 'env -u HOME rm -f "$path/data.bin"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'sudo -u nobody rm -f "$path/data.bin"\n')
    assert _run(
        tmp_path, FETCH_PART_PATH + 'timeout -s KILL 60 rm -f "$path/data.bin"\n'
    )


def test_mutation_behind_nested_wrappers_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'FOO=1 env time rm -f "$path/data.bin"\n')


def test_redirect_into_server_path_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'echo broken > "$path/data.bin"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'echo broken >| "$path/data.bin"\n')
    assert _run(
        tmp_path,
        'echo broken > `${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts'
        " WHERE table = 't' AND active LIMIT 1\"`/data.bin\n",
    )
    assert _run(
        tmp_path,
        'get_path() { ${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts'
        " WHERE table = 't' AND active LIMIT 1\"; }\n"
        'echo broken > "$(get_path)/data.bin"\n',
    )
    assert _run(
        tmp_path,
        'get_path() { ${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts'
        " WHERE table = 't' AND active LIMIT 1\"; }\n"
        'cat <<\'EOF\' > "$(get_path)/data.bin"\n'
        "broken\nEOF\n",
    )
    assert _run(
        tmp_path,
        'get_path() { ${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts'
        " WHERE table = 't' AND active LIMIT 1\"; }\n"
        'echo broken > "$(get_path)/$(mktemp -u)"\n',
    )


def test_tee_into_server_path_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'echo broken | tee "$path/data.bin"\n')


def test_quoted_hash_payload_is_flagged(tmp_path):
    # Regression case from the review of #114070: a `#` inside quotes was treated as a
    # comment, truncating the line before the redirection into the server path.
    assert _run(tmp_path, FETCH_PART_PATH + "printf '# broken' > \"$path/data.bin\"\n")
    assert _run(tmp_path, FETCH_PART_PATH + 'echo "# broken" > "$path/data.bin"\n')


def test_commented_out_mutation_is_not_flagged(tmp_path):
    assert not _run(tmp_path, FETCH_PART_PATH + '# rm -f "$path/data.bin"\n')
    assert not _run(tmp_path, FETCH_PART_PATH + 'true # rm -f "$path/data.bin"\n')


def test_commented_out_fetch_does_not_arm_check(tmp_path):
    commented_fetch = "# " + FETCH_PART_PATH
    assert not _run(tmp_path, commented_fetch + 'tmp=$(mktemp)\nrm -f "$tmp"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'tmp=$(mktemp)\nrm -f "$tmp"\n')


def test_clickhouse_disks_mutations_are_flagged(tmp_path):
    fetch_metadata_path = (
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.tables'
        " WHERE table = 't' AND database = 'default' LIMIT 1\")\n"
    )
    assert _run(
        tmp_path,
        fetch_metadata_path
        + '/usr/bin/clickhouse disks --disk default --query "w --path-to ${path}"\n',
    )
    assert _run(
        tmp_path,
        fetch_metadata_path + 'clickhouse-disks -q "rm --path-to ${path}"\n',
    )
    assert not _run(
        tmp_path,
        fetch_metadata_path + 'clickhouse disks --query "list --recursive $path" | sed -n 1p\n',
    )


def test_mutation_in_case_branch_is_flagged(tmp_path):
    assert _run(
        tmp_path, FETCH_PART_PATH + 'case 1 in 1) rm -f "$path/data.bin";; esac\n'
    )


def test_derived_path_expression_is_flagged(tmp_path):
    # The fetched expression need not be the bare column: a derived expression such as
    # concat(path, '/data.bin') still pulls a server-side path into the shell.
    assert _run(
        tmp_path,
        "p=$(${CLICKHOUSE_CLIENT} -q \"SELECT concat(path, '/data.bin')"
        " FROM system.parts WHERE table = 't' LIMIT 1\")\n"
        'rm -f "$p"\n',
    )


def test_select_star_from_system_parts_is_flagged(tmp_path):
    # `SELECT *` includes the `path` column and must not bypass the check when a shell
    # command extracts that field before modifying the part directory. This includes the
    # default tab-separated output; it need not spell out `FORMAT TSVRaw`.
    assert _run(
        tmp_path,
        'row=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.parts'
        " WHERE table = 't' AND active\")\n"
        'path=$(printf "%s" "$row" | cut -f22)\n'
        'rm -f "$path/data.bin"\n',
    )


def test_decorated_or_qualified_star_from_system_parts_is_flagged(tmp_path):
    # `DISTINCT *` and an aliased `p.*` also include the `path` column.
    assert _run(
        tmp_path,
        'row=$(${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT * FROM system.parts'
        " WHERE table = 't' AND active\")\n"
        'path=$(printf "%s" "$row" | cut -f22)\n'
        'rm -f "$path/data.bin"\n',
    )
    assert _run(
        tmp_path,
        'row=$(${CLICKHOUSE_CLIENT} -q "SELECT p.* FROM system.parts AS p'
        " WHERE table = 't' AND active\")\n"
        'path=$(printf "%s" "$row" | cut -f22)\n'
        'rm -f "$path/data.bin"\n',
    )
    assert _run(
        tmp_path,
        'row=$(${CLICKHOUSE_CLIENT} -q "SELECT 1, p.* FROM system.parts AS p'
        " WHERE table = 't' AND active\")\n"
        'path=$(printf "%s" "$row" | cut -f23)\n'
        'rm -f "$path/data.bin"\n',
    )


def test_mutation_after_negation_is_flagged(tmp_path):
    # `!` is a shell command introducer, commonly used when a failure is expected.
    assert _run(tmp_path, FETCH_PART_PATH + '! rm -f "$path/data.bin"\n')


def test_server_root_fetch_is_flagged(tmp_path):
    assert _run(
        tmp_path,
        'root=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.server_settings'
        " WHERE name = 'path'\")\n"
        'rm -f "$root/flags/force_drop_table"\n',
    )
    assert _run(
        tmp_path,
        'root=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.server_settings'
        " WHERE 'path' = name\")\n"
        'rm -f "$root/flags/force_drop_table"\n',
    )
    assert _run(
        tmp_path,
        'root=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.server_settings'
        " WHERE name IN ('path')\")\n"
        'rm -f "$root/flags/force_drop_table"\n',
    )
    assert _run(
        tmp_path,
        'row=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.server_settings'
        " WHERE name = 'path'\")\n"
        'root=$(printf "%s" "$row" | cut -f2)\n'
        'rm -f "$root/flags/force_drop_table"\n',
    )
    assert _run(
        tmp_path,
        'row=$(${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT * FROM system.server_settings'
        " WHERE name = 'path'\")\n"
        'root=$(printf "%s" "$row" | cut -f2)\n'
        'rm -f "$root/flags/force_drop_table"\n',
    )
    assert _run(
        tmp_path,
        'row=$(${CLICKHOUSE_CLIENT} -q "SELECT 1, * FROM system.server_settings'
        " WHERE name = 'path'\")\n"
        'root=$(printf "%s" "$row" | cut -f3)\n'
        'rm -f "$root/flags/force_drop_table"\n',
    )


def test_other_server_path_carriers_are_flagged(tmp_path):
    assert _run(
        tmp_path,
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.databases'
        " WHERE name = currentDatabase()\")\n"
        'rm -f "$path.sql"\n',
    )
    assert _run(
        tmp_path,
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.detached_tables'
        " WHERE database = currentDatabase() LIMIT 1\")\n"
        'rm -f "$path"\n',
    )
    assert _run(
        tmp_path,
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT data_path FROM system.distribution_queue LIMIT 1")\n'
        'rm -f "$path/bad"\n',
    )


def test_quoted_server_path_identifiers_are_flagged(tmp_path):
    assert _run(
        tmp_path,
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT `path` FROM system.parts'
        " WHERE table = 't' AND active LIMIT 1\")\n"
        'rm -f "$path/data.bin"\n',
    )
    assert _run(
        tmp_path,
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.`parts`'
        " WHERE table = 't' AND active LIMIT 1\")\n"
        'rm -f "$path/data.bin"\n',
    )
    assert _run(
        tmp_path,
        'root=$(${CLICKHOUSE_CLIENT} -q \'SELECT "value" FROM system.`server_settings`'
        " WHERE `name` = 'path'\")\n"
        'rm -f "$root/flags/force_drop_table"\n',
    )


def test_server_root_fetch_with_multiple_setting_values_is_flagged(tmp_path):
    assert _run(
        tmp_path,
        'root=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.server_settings'
        " WHERE name IN ('path', 'tmp_path')\")\n"
        'rm -f "$root/flags/force_drop_table"\n',
    )


def test_sed_in_place_variants_are_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'sed -Ei s/a/b/ "$path/data.bin"\n')
    assert _run(
        tmp_path, FETCH_PART_PATH + 'sed --in-place=.bak s/a/b/ "$path/data.bin"\n'
    )
    assert _run(
        tmp_path,
        FETCH_PART_PATH + 'sed --follow-symlinks -i s/a/b/ "$path/data.bin"\n',
    )


def test_non_executable_payload_does_not_arm_check(tmp_path):
    assert not _run(
        tmp_path,
        'echo "SELECT path FROM system.parts WHERE table = \'t\'"\n'
        'tmp=$(mktemp)\nrm -f "$tmp"\n',
    )
    assert not _run(
        tmp_path,
        "cat <<'EOF'\nSELECT path FROM system.parts WHERE table = 't'\nEOF\n"
        'tmp=$(mktemp)\nrm -f "$tmp"\n',
    )


def test_shell_command_string_executor_is_flagged(tmp_path):
    assert _run(tmp_path, FETCH_PART_PATH + 'sh -c "rm -f \\"$path/data.bin\\""\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c "rm -f \\"$path/data.bin\\""\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'eval "rm -f \\"$path/data.bin\\""\n')
    # A payload that is not a literal command word stays opaque.
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c "$cmd"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c "$(build_cmd)"\n')
    # A plain call must not cover for a second executor, or for a mutation, on the same line.
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c worker; eval "$cmd"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c worker; rm -f "$path/data.bin"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c "worker; rm -f $path/data.bin"\n')
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c worker > "$path/data.bin"\n')


def test_shell_command_string_calling_a_function_is_not_flagged(tmp_path):
    # `bash -c <function>` is how stress tests spawn background threads. The payload hides
    # nothing: the function body is in the same file and is scanned line by line anyway.
    assert not _run(
        tmp_path,
        FETCH_PART_PATH + "bash -c insert_thread 2> /dev/null &\n",
    )
    assert not _run(
        tmp_path,
        FETCH_PART_PATH + 'bash -c "sync_replica_with_retries $i" &\n',
    )
    # A mutation verb as the payload is still a mutation.
    assert _run(tmp_path, FETCH_PART_PATH + 'bash -c "rm $path/data.bin"\n')


def test_server_settings_inspection_without_value_is_not_flagged(tmp_path):
    # Merely inspecting the setting without materializing the path is not a fetch.
    assert not _run(
        tmp_path,
        'x=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.server_settings'
        " WHERE name = 'path'\")\n"
        "tmp=$(mktemp)\n"
        'rm -f "$tmp"\n',
    )


def test_no_server_path_fetch_is_not_flagged(tmp_path):
    # File mutations over the test's own scratch files are fine.
    assert not _run(tmp_path, 'f=$(mktemp)\nrm -f "$f"\n')


def test_fetch_without_mutation_is_not_flagged(tmp_path):
    assert not _run(tmp_path, FETCH_PART_PATH + 'echo "$path"\n')


def test_arrow_diagnostic_is_not_treated_as_redirection(tmp_path):
    # A diagnostic can contain `-> $output`; this is not shell redirection.
    assert not _run(
        tmp_path,
        FETCH_PART_PATH + 'echo "Query failed: $* -> $output"\n',
    )


def test_mktemp_scratch_redirect_is_not_flagged(tmp_path):
    # The only allowed command substitution in a redirect is a `mktemp` scratch path.
    assert not _run(tmp_path, FETCH_PART_PATH + 'echo x > "$(mktemp)"\n')


def test_mktemp_in_server_path_is_flagged(tmp_path):
    assert _run(
        tmp_path,
        FETCH_PART_PATH + 'echo broken > "$(mktemp --tmpdir="$path" tmp.XXXXXX)"\n',
    )
    assert _run(
        tmp_path,
        FETCH_PART_PATH + 'echo broken > "$(mktemp "$path/part.XXXXXX")"\n',
    )


def test_remote_data_paths_are_flagged(tmp_path):
    assert _run(
        tmp_path,
        'cache=$(${CLICKHOUSE_CLIENT} -q "SELECT cache_paths[1] FROM '
        'system.remote_data_paths LIMIT 1")\nrm -f "$cache"\n',
    )
    assert _run(
        tmp_path,
        'local=$(${CLICKHOUSE_CLIENT} -q "SELECT local_path FROM '
        'system.remote_data_paths LIMIT 1")\nrm -f "$local"\n',
    )


def test_filesystem_cache_settings_path_is_flagged(tmp_path):
    assert _run(
        tmp_path,
        'cache_dir=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM '
        'system.filesystem_cache_settings LIMIT 1")\nrm -f "$cache_dir/file"\n',
    )


def test_empty_command_substitution_in_output_command_does_not_raise(tmp_path):
    # An `echo` / `printf` payload can contain an empty command substitution - a pair of
    # backticks in a `grep -qF '```'` pattern, or `$()`. The scanner keeps only the
    # executable fragments of such a line, and an empty one must contribute an empty
    # string rather than `None`, which used to abort the whole check.
    assert not _run(
        tmp_path,
        "printf '%s' \"$out\" | grep -qF '```' && echo FAIL || echo OK\n",
    )
    assert not _run(tmp_path, 'echo "empty $() substitution"\n')


def test_empty_command_substitution_does_not_hide_a_violation(tmp_path):
    assert _run(
        tmp_path,
        FETCH_PART_PATH + "echo '```' \nrm -f \"$path/data.bin\"\n",
    )


def test_clickhouse_local_path_is_not_a_server_path(tmp_path):
    # `clickhouse-local` queries its own `--path`, so the paths it reports belong to the
    # test's scratch directory and removing them is not a server data manipulation.
    assert not _run(
        tmp_path,
        'data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"\n'
        'part_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT path FROM system.parts WHERE active")\n'
        'rm -rf "${data_path:?}"\n',
    )


def test_clickhouse_local_multiline_query_is_not_a_server_path(tmp_path):
    # The query of a `clickhouse-local` invocation can continue on the following lines.
    assert not _run(
        tmp_path,
        '$CLICKHOUSE_LOCAL --path "$data_path" -m -q "\n'
        "SELECT path FROM system.parts WHERE active\n"
        '"\n'
        'rm -rf "${data_path:?}"\n',
    )


def test_clickhouse_local_does_not_mask_a_server_path_fetch(tmp_path):
    # A `clickhouse-local` invocation elsewhere in the file, or later on the same line,
    # must not exempt a path fetched from the server.
    assert _run(
        tmp_path,
        '$CLICKHOUSE_LOCAL -q "SELECT 1"\n'
        + FETCH_PART_PATH
        + 'rm -f "$path/data.bin"\n',
    )
    assert _run(
        tmp_path,
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts LIMIT 1"); '
        '$CLICKHOUSE_LOCAL -q "SELECT 1"\n'
        'rm -f "$path/data.bin"\n',
    )


def test_server_path_fetch_after_clickhouse_local_on_the_same_line_is_flagged(tmp_path):
    # Regression case from the review of #114070: the exemption truncated the line at the
    # invocation, so a server-side fetch that follows it on the same line was never seen.
    assert _run(
        tmp_path,
        '$CLICKHOUSE_LOCAL -q "SELECT 1"; '
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts LIMIT 1")\n'
        'rm -f "$path/data.bin"\n',
    )


def test_server_path_fetch_after_multiline_clickhouse_local_is_flagged(tmp_path):
    # The same bypass on the line where a multiline `clickhouse-local` query closes.
    assert _run(
        tmp_path,
        '$CLICKHOUSE_LOCAL --path "$data_path" -m -q "\n'
        "SELECT 1\n"
        '"; path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts LIMIT 1")\n'
        'rm -f "$path/data.bin"\n',
    )


def test_clickhouse_local_in_command_substitution_ends_at_the_closing_paren(tmp_path):
    # The invocation ends with its command substitution; the assignment that follows on the
    # same line queries the server.
    assert _run(
        tmp_path,
        'local_path=$($CLICKHOUSE_LOCAL -q "SELECT 1") '
        'path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts LIMIT 1")\n'
        'rm -f "$path/data.bin"\n',
    )


def test_clickhouse_local_continued_with_a_backslash_is_not_a_server_path(tmp_path):
    # An invocation split over lines with a trailing backslash is still one command.
    assert not _run(
        tmp_path,
        "$CLICKHOUSE_LOCAL --path \"$data_path\" \\\n"
        '    -q "SELECT path FROM system.parts WHERE active"\n'
        'rm -rf "${data_path:?}"\n',
    )
