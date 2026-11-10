from ci.jobs.promql_compliance_job import _suite_row
from ci.jobs.scripts.job_hooks.promql_compliance_comment_hook import _build_body
from ci.jobs.scripts.job_hooks.promql_compliance_s3 import (
    SUITE_COMPLIANCE,
    SUITE_EXTENDED,
    _baseline_payload_ok,
    build_result_payload,
    suites_from_payload,
)


def test_legacy_payload_is_compliance_only():
    legacy = {"pct": 95.18, "passed": 512, "failed": 20, "unsupported": 7, "total": 539}
    assert _baseline_payload_ok(legacy)
    suites = suites_from_payload(legacy)
    assert list(suites) == [SUITE_COMPLIANCE]
    assert suites[SUITE_COMPLIANCE]["passed"] == 512


def test_v2_payload_round_trip():
    payload = build_result_payload(
        {"pct": 95.18, "passed": 512, "failed": 20, "unsupported": 7, "total": 539},
        {
            "pct": 40.0,
            "passed": 400,
            "failed": 500,
            "unsupported": 100,
            "total": 1000,
            "upstream_sha": "abc",
            "excluded_native_histogram": 12,
            "excluded_assertions": 8,
            "excluded_files": ["info.test"],
        },
    )
    assert payload["schema_version"] == 2
    assert _baseline_payload_ok(payload)
    suites = suites_from_payload(payload)
    assert SUITE_EXTENDED in suites
    assert suites[SUITE_EXTENDED]["excluded_native_histogram"] == 12


def test_missing_extended_suite_has_no_baseline():
    current = {"pct": 41.0, "passed": 41, "failed": 50, "unsupported": 9, "total": 100}
    row = _suite_row(SUITE_EXTENDED, current, None)
    assert row["has_baseline"] is False
    assert row["base_pct"] is None
    assert row["delta"] is None


def test_comment_renders_two_rows_and_na_baseline():
    body = _build_body(
        {
            "baseline_source": "S3 master `abcdef012`",
            "from_zero": False,
            "s3_sha": "a" * 40,
            "result_json_url": "https://example.test/result.json",
            "suites": [
                _suite_row(
                    SUITE_COMPLIANCE,
                    {"pct": 95.18, "passed": 512, "failed": 20, "unsupported": 7},
                    {"pct": 95.18, "passed": 512, "failed": 20, "unsupported": 7},
                ),
                _suite_row(
                    SUITE_EXTENDED,
                    {
                        "pct": 40.0,
                        "passed": 400,
                        "failed": 500,
                        "unsupported": 100,
                        "excluded_native_histogram": 3,
                        "excluded_assertions": 2,
                        "upstream_sha": "3da2514eff5ce503de2e8f355cae8ca58a2a35ed",
                    },
                    None,
                ),
            ],
        }
    )
    assert "| Prometheus compliance |" in body
    assert "| Extended support (upstream engine tests) |" in body
    assert "n/a" in body
    assert "Uploaded compliance JSON" in body
    assert "native-histogram exclusions: 3" in body
    assert "Made with" not in body
