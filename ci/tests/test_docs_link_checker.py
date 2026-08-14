"""Regression tests for links extracted from generated docs components."""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.docs import locale_components_check, lychee_check


def write_settings_explorer(docs_root, rendered_href):
    component = (
        docs_root
        / "snippets/components/SessionSettingsExplorer/SessionSettingsExplorer.jsx"
    )
    component.parent.mkdir(parents=True)
    component.write_text(
        'const entries = [{"name":"example_setting",'
        '"href":"/reference/settings/session-settings/example#example_setting"}];\n'
        f"<a {rendered_href}>example_setting</a>\n",
        encoding="utf-8",
    )


def test_settings_explorer_links_are_materialized_for_lychee(tmp_path):
    docs_root = tmp_path / "docs"
    output = tmp_path / "output"
    output.mkdir()
    write_settings_explorer(
        docs_root,
        'href={`https://clickhouse.com/docs${item.value.href}`}',
    )

    output_name, errors = lychee_check.write_settings_explorer_links(
        docs_root, output)

    assert errors == 0
    materialized = (output / output_name).read_text(encoding="utf-8")
    assert (
        "](/reference/settings/session-settings/example#example_setting)"
        in materialized
    )
    assert "https://clickhouse.com/docs/reference/settings" not in materialized


def test_settings_explorer_links_require_an_absolute_production_url(
        tmp_path, capsys):
    docs_root = tmp_path / "docs"
    output = tmp_path / "output"
    output.mkdir()
    write_settings_explorer(
        docs_root,
        'href={`/docs${item.value.href}`}',
    )

    _output_name, errors = lychee_check.write_settings_explorer_links(
        docs_root, output)

    assert errors == 1
    report = capsys.readouterr().out
    assert (
        "rendered settings links must start with "
        "`https://clickhouse.com/docs/`"
    ) in report
    assert "/docs/reference/settings/session-settings/example#example_setting" in report


def test_locale_link_parsers_ignore_absolute_templates():
    rendered_href = 'href={`https://clickhouse.com/docs${item.value.href}`}'

    assert locale_components_check.HREF.search(rendered_href) is None
    assert locale_components_check.TEMPLATE.search(rendered_href) is None
