"""Regression tests for links extracted from generated docs components."""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.docs import (
    canonical_links_check,
    locale_components_check,
    lychee_check,
)


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


def test_canonical_link_checker_rewrites_rendered_links_only():
    redirects = {
        "/sql-reference/functions/plus": "/reference/functions/plus",
        "/operations/settings": "/reference/settings",
    }
    text = """[Plus](/sql-reference/functions/plus#examples)
<Card href="/operations/settings" />
const item = { href: "/sql-reference/functions/plus" };
`[Example](/sql-reference/functions/plus)`
```md
[Example](/sql-reference/functions/plus)
```
{/* [Hidden](/operations/settings) */}
"""

    aliases = canonical_links_check.find_aliases_in_text(text, redirects)

    assert [(old, new) for _start, _end, old, new in aliases] == [
        (
            "/sql-reference/functions/plus#examples",
            "/reference/functions/plus#examples",
        ),
        ("/operations/settings", "/reference/settings"),
        ("/sql-reference/functions/plus", "/reference/functions/plus"),
    ]


def test_canonical_link_checker_follows_redirect_chains():
    redirects = {
        "/old": "/older",
        "/older": "/reference/current",
    }

    assert (
        canonical_links_check.canonicalize_url("/old#details", redirects)
        == "/reference/current#details"
    )


def test_canonical_link_checker_rewrites_source_code_urls():
    redirects = {
        "/engines/table-engines/mergetree-family/mergetree": (
            "/reference/engines/table-engines/mergetree-family/mergetree"
        ),
        "/sql-reference/data-types/date": "/reference/data-types/date",
    }
    text = """See https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree/.
[Date](/sql-reference/data-types/date.md)
"""

    aliases = canonical_links_check.find_aliases_in_text(
        text, redirects, include_public_urls=True
    )

    assert [(old, new) for _start, _end, old, new in aliases] == [
        (
            "https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree/",
            "https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree",
        ),
        ("/sql-reference/data-types/date.md", "/reference/data-types/date"),
    ]
