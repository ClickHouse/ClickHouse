import difflib
from pathlib import Path

import pytest

from helpers.cluster import ClickHouseCluster


BASE_DIR = Path(__file__).resolve().parent
QUERIES_DIR = BASE_DIR / "queries"
SPECIAL_QUERIES_DIR = QUERIES_DIR / "special"
EXPECTED_DIR = BASE_DIR / "expected"
SPECIAL_EXPECTED_DIR = EXPECTED_DIR / "special"

# Contents of this file are frm the tests/queries/0_stateless/ directory, with only tuple related queries kept.
BASE_CASES = [
    "02731_analyzer_join_resolve_nested",
    "02940_variant_text_deserialization",
    "02941_variant_type_1",
    "03036_dynamic_read_subcolumns_small",
    "03036_dynamic_read_shared_subcolumns_small",
    "03040_dynamic_type_alters_1_compact_merge_tree",
    "03040_dynamic_type_alters_1_memory",
    "03040_dynamic_type_alters_1_wide_merge_tree",
    "03040_dynamic_type_alters_2_compact_merge_tree",
    "03040_dynamic_type_alters_2_wide_merge_tree",
    "03041_dynamic_type_check_table",
    "03162_dynamic_type_nested",
    "03290_nullable_json",
    "03369_variant_escape_filename_merge_tree",
    "03913_tuple_inside_nullable_subcolumns",
    "03915_tuple_inside_nullable_variant_dynamic_element",
    "03916_tuple_inside_nullable_json_subcolumns",
    "03917_tuple_inside_nullable_tuple_subcolumns",
]

# When the setting is disabled, some queries throw errors; when it is enabled, they do not.
# So we keep them separate so we can check expected errors using trailing `serverError` in the .sql file.
SPECIAL_OFF_ONLY_CASES = [
    "03913_tuple_inside_nullable_subcolumns_off_only",
]
SPECIAL_ON_ONLY_CASES = [
    "03913_tuple_inside_nullable_subcolumns_on_only",
]

cluster = ClickHouseCluster(__file__)
node_off = cluster.add_instance(
    "node_off",
    main_configs=["configs/allow_nullable_tuple_subcolumns_off.xml"],
)
node_on = cluster.add_instance(
    "node_on",
    main_configs=["configs/allow_nullable_tuple_subcolumns_on.xml"],
)
node_reload = cluster.add_instance(
    "node_reload",
    main_configs=["configs/allow_nullable_tuple_subcolumns_off.xml"],
)


def _assert_reference(reference_path: Path, actual: str) -> None:
    expected = reference_path.read_text(encoding="utf-8")
    if actual == expected:
        return

    diff_lines = list(
        difflib.unified_diff(
            expected.splitlines(),
            actual.splitlines(),
            fromfile=f"{reference_path} (expected)",
            tofile="actual",
            lineterm="",
        )
    )
    max_diff_lines = 200
    if len(diff_lines) > max_diff_lines:
        diff_lines = diff_lines[:max_diff_lines] + ["... (diff truncated)"]

    raise AssertionError(
        f"Reference mismatch for {reference_path}.\n" + "\n".join(diff_lines)
    )


def _run_case(node, mode: str, case: str, special: bool = False) -> None:
    sql_dir = SPECIAL_QUERIES_DIR if special else QUERIES_DIR
    expected_dir = SPECIAL_EXPECTED_DIR if special else EXPECTED_DIR
    sql_file = sql_dir / f"{case}.sql"
    container_sql_file = f"/tmp/{case}.sql"
    container_out_file = f"/tmp/{case}.out"
    container_err_file = f"/tmp/{case}.err"
    container_rc_file = f"/tmp/{case}.rc"

    node.copy_file_to_container(str(sql_file), container_sql_file)
    node.exec_in_container(
        [
            "bash",
            "-lc",
            (
                "/usr/bin/clickhouse client "
                f"--queries-file {container_sql_file} "
                f"> {container_out_file} 2> {container_err_file}; "
                f"echo -n $? > {container_rc_file}"
            ),
        ],
        nothrow=True,
    )
    client_rc = node.exec_in_container(["bash", "-lc", f"cat {container_rc_file}"], nothrow=True).strip()

    if client_rc != "0":
        stderr = node.exec_in_container(["bash", "-lc", f"cat {container_err_file}"], nothrow=True)
        raise AssertionError(
            f"Case '{case}' failed in mode '{mode}' with exit code {client_rc}.\n{stderr}"
        )

    actual = node.exec_in_container(["bash", "-lc", f"cat {container_out_file}"])

    reference_path = expected_dir / f"{case}.{mode}.reference"
    _assert_reference(reference_path, actual)


def _run_mode(node, mode: str) -> None:
    for case in BASE_CASES:
        _run_case(node, mode, case)

    special_cases = SPECIAL_OFF_ONLY_CASES if mode == "off" else SPECIAL_ON_ONLY_CASES
    for case in special_cases:
        _run_case(node, mode, case, special=True)


def _assert_references_exist() -> None:
    expected_files = []
    for case in BASE_CASES:
        expected_files.append(EXPECTED_DIR / f"{case}.off.reference")
        expected_files.append(EXPECTED_DIR / f"{case}.on.reference")
    for case in SPECIAL_OFF_ONLY_CASES:
        expected_files.append(SPECIAL_EXPECTED_DIR / f"{case}.off.reference")
    for case in SPECIAL_ON_ONLY_CASES:
        expected_files.append(SPECIAL_EXPECTED_DIR / f"{case}.on.reference")

    missing = [str(path) for path in expected_files if not path.exists()]
    assert not missing, (
        "Missing reference files:\n"
        + "\n".join(missing)
        + "\nAdd the missing .reference files."
    )


@pytest.fixture(scope="module", autouse=True)
def check_references():
    _assert_references_exist()
    yield


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def test_queries_for_off_server_mode(started_cluster):
    _run_mode(node_off, "off")


def test_queries_for_on_server_mode(started_cluster):
    _run_mode(node_on, "on")


def test_setting_is_restart_only_does_not_change_on_config_reload(started_cluster):
    server_setting_name = "allow_nullable_tuple_in_extracted_subcolumns"
    reload_probe_query = """
SELECT toTypeName(v.`Tuple(UInt64, String)`), v.`Tuple(UInt64, String)`
FROM (SELECT 42::Variant(Tuple(UInt64, String), UInt64) AS v)
FORMAT TSV
"""

    changeable = node_reload.query(
        f"SELECT changeable_without_restart FROM system.server_settings WHERE name = '{server_setting_name}'"
    )
    assert changeable == "No\n"

    value_before = node_reload.query(
        f"SELECT value FROM system.server_settings WHERE name = '{server_setting_name}'"
    )
    probe_before = node_reload.query(reload_probe_query)

    config_path = (
        BASE_DIR
        / cluster.instances_dir_name
        / "node_reload"
        / "configs"
        / "config.d"
        / "allow_nullable_tuple_subcolumns_off.xml"
    )
    assert config_path.exists(), f"Missing config file: {config_path}"

    original_config = config_path.read_text(encoding="utf-8")
    updated_config = original_config.replace(
        "<allow_nullable_tuple_in_extracted_subcolumns>0</allow_nullable_tuple_in_extracted_subcolumns>",
        "<allow_nullable_tuple_in_extracted_subcolumns>1</allow_nullable_tuple_in_extracted_subcolumns>",
    )
    assert updated_config != original_config

    config_path.write_text(updated_config, encoding="utf-8")
    node_reload.query("SYSTEM RELOAD CONFIG")

    value_after = node_reload.query(
        f"SELECT value FROM system.server_settings WHERE name = '{server_setting_name}'"
    )
    probe_after = node_reload.query(reload_probe_query)

    # This server setting is expected to be restart-only.
    assert value_after == value_before
    assert probe_after == probe_before
