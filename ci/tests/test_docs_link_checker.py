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


def test_source_embedded_doc_links_are_materialized_for_lychee(tmp_path):
    repo_root = tmp_path / "repo"
    source = repo_root / "src/Functions/example.cpp"
    source.parent.mkdir(parents=True)
    source.write_text(
        '''R"DOCS_MD(
[Relative](/reference/functions/example#syntax)
[Public](https://clickhouse.com/docs/reference/data-types/newjson)
<Card href="/concepts/best-practices/json-type" />
[External](https://example.com/reference/not-checked)
```md
[Code sample](/reference/not-checked)
```
)DOCS_MD";
''',
        encoding="utf-8",
    )
    readme = repo_root / "src/Functions/README.md"
    readme.write_text("[Out of scope](/reference/not-checked)\n", encoding="utf-8")
    output = tmp_path / "output"
    output.mkdir()

    output_name, count = lychee_check.write_source_doc_links(repo_root, output)

    assert count == 3
    materialized = (output / output_name).read_text(encoding="utf-8")
    assert "](/reference/functions/example#syntax)" in materialized
    assert "](/reference/data-types/newjson)" in materialized
    assert "](/concepts/best-practices/json-type)" in materialized
    assert "clickhouse.com/docs" not in materialized
    assert "not-checked" not in materialized


def test_broken_source_doc_target_is_materialized_for_lychee(tmp_path):
    repo_root = tmp_path / "repo"
    source = repo_root / "src/example.h"
    source.parent.mkdir(parents=True)
    source.write_text(
        "[Broken](https://clickhouse.com/docs/reference/not-a-real-page)\n",
        encoding="utf-8",
    )
    output = tmp_path / "output"
    output.mkdir()

    output_name, count = lychee_check.write_source_doc_links(repo_root, output)

    assert count == 1
    assert "](/reference/not-a-real-page)" in (
        output / output_name
    ).read_text(encoding="utf-8")


def test_directory_index_route_materializes_anchors_only(tmp_path):
    index = tmp_path / "reference/operators/index.mdx"
    index.parent.mkdir(parents=True)
    index.write_text(
        '<a id="interval"></a>\n[Do not duplicate](/reference/missing)\n',
        encoding="utf-8",
    )

    lychee_check.materialize_index_routes(tmp_path)

    route = (tmp_path / "reference/operators.mdx").read_text(encoding="utf-8")
    assert '<a id="interval"></a>' in route
    assert "reference/missing" not in route


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


def test_canonical_link_checker_rewrites_template_route_base():
    aliases = canonical_links_check.find_aliases_in_text(
        "const x = { href: `/old/${id}` }", {"/old": "/new"}
    )

    assert [(old, new) for _start, _end, old, new in aliases] == [
        ("/old/", "/new/")
    ]


def test_canonical_link_checker_rewrites_source_code_urls():
    redirects = {
        "/engines/table-engines/mergetree-family/mergetree": (
            "/reference/engines/table-engines/mergetree-family/mergetree"
        ),
        "/sql-reference/data-types/date": "/reference/data-types/date",
    }
    text = """[MergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree/)
See https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree/.\\n
[Date](/sql-reference/data-types/date.md)
[Dynamic](https://clickhouse.com/docs/reference/data-types/dynamic)
"""

    aliases = canonical_links_check.find_aliases_in_text(
        text, redirects, include_public_urls=True
    )

    assert [(old, new) for _start, _end, old, new in aliases] == [
        (
            "https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree/",
            "/reference/engines/table-engines/mergetree-family/mergetree",
        ),
        (
            "https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree/",
            "https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree",
        ),
        ("/sql-reference/data-types/date.md", "/reference/data-types/date"),
        (
            "https://clickhouse.com/docs/reference/data-types/dynamic",
            "/reference/data-types/dynamic",
        ),
    ]
