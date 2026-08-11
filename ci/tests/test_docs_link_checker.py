"""Regression tests for links extracted from generated docs components."""

import os
import re
import sys
from pathlib import Path


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.docs import locale_components_check, lychee_check


REPO_ROOT = Path(__file__).resolve().parents[2]


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
        'href={`/docs${item.value.href}`}',
    )

    output_name, errors = lychee_check.write_settings_explorer_links(
        docs_root, output)

    assert errors == 0
    materialized = (output / output_name).read_text(encoding="utf-8")
    assert (
        "](/reference/settings/session-settings/example#example_setting)"
        in materialized
    )
    assert "/docs/reference/settings" not in materialized


def test_settings_explorer_links_require_the_production_docs_mount(
        tmp_path, capsys):
    docs_root = tmp_path / "docs"
    output = tmp_path / "output"
    output.mkdir()
    write_settings_explorer(docs_root, "href={item.value.href}")

    _output_name, errors = lychee_check.write_settings_explorer_links(
        docs_root, output)

    assert errors == 1
    report = capsys.readouterr().out
    assert "rendered settings links must start with `/docs/`" in report
    assert "/reference/settings/session-settings/example#example_setting" in report


def test_locale_static_link_parser_leaves_templates_to_template_parser():
    rendered_href = 'href={`/docs${item.value.href}`}'

    assert locale_components_check.HREF.search(rendered_href) is None
    template = locale_components_check.TEMPLATE.search(rendered_href)
    assert template is not None
    assert template.group(1) == "/docs"


def test_cloud_not_supported_badges_link_to_published_page():
    page = (
        REPO_ROOT / "docs/products/cloud/guides/cloud-compatibility.mdx"
    ).read_text(encoding="utf-8")
    slug = re.search(r"^slug: (\S+)$", page, re.MULTILINE)
    assert slug is not None
    anchor = "list-of-unsupported-features"
    assert f"{{#{anchor}}}" in page
    expected_href = f'href="/docs{slug.group(1)}#{anchor}"'

    snippets = REPO_ROOT / "docs/snippets"
    components = list(
        snippets.glob(
            "**/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx"
        )
    )
    components += list(
        snippets.glob("**/components/Badges/CloudNotSupportedBadge.jsx")
    )
    assert components
    for component in components:
        assert expected_href in component.read_text(encoding="utf-8")
