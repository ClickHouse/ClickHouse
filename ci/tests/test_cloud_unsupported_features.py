import importlib.util
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_PATH = (
    REPO_ROOT / "ci/jobs/scripts/docs/generate_cloud_unsupported_features.py"
)
SPEC = importlib.util.spec_from_file_location(
    "generate_cloud_unsupported_features", GENERATOR_PATH
)
assert SPEC and SPEC.loader
generator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = generator
SPEC.loader.exec_module(generator)

def _write(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_collects_direct_and_imported_badges(tmp_path):
    docs = tmp_path / "docs"
    _write(
        docs / "direct.mdx",
        "---\ntitle: 'Direct feature'\n---\n<CloudNotSupportedBadge />\n",
    )
    _write(
        docs / "snippets/unsupported.mdx",
        "## Imported section {#imported-section}\n<CloudNotSupportedBadge/>\n",
    )
    _write(
        docs / "guide/index.mdx",
        "---\ntitle: 'Guide'\n---\n"
        "import Unsupported from '/snippets/unsupported.mdx';\n"
        "<Unsupported />\n",
    )
    _write(
        docs / "ru/ignored.mdx",
        "---\ntitle: 'Ignored'\n---\n<CloudNotSupportedBadge />\n",
    )
    _write(
        docs / "_partial.mdx",
        "---\ntitle: 'Partial'\n---\n<CloudNotSupportedBadge />\n",
    )

    assert generator.collect_features(docs) == [
        generator.Feature("Direct feature", "/direct"),
        generator.Feature("Guide: Imported section", "/guide#imported-section"),
    ]


def test_updates_only_the_marked_list(tmp_path):
    docs = tmp_path / "docs"
    _write(
        docs / "feature.mdx",
        "---\ntitle: 'Feature'\n---\n<CloudNotSupportedBadge />\n",
    )
    target = docs / generator.TARGET
    _write(
        target,
        "Before\n"
        f"{generator.START_MARKER}\nold\n{generator.END_MARKER}\n"
        "After\n",
    )

    updated, count = generator.updated_target_content(docs)

    assert count == 1
    assert updated == (
        "Before\n"
        f"{generator.START_MARKER}\n- [Feature](/feature)\n"
        f"{generator.END_MARKER}\nAfter\n"
    )


def test_all_badge_components_link_to_cloud_compatibility():
    components = list(
        (REPO_ROOT / "docs/snippets").glob(
            "**/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx"
        )
    ) + list(
        (REPO_ROOT / "docs/snippets").glob(
            "**/components/Badges/CloudNotSupportedBadge.jsx"
        )
    )
    assert len(components) == 18
    for component in components:
        relative = component.relative_to(REPO_ROOT / "docs/snippets")
        locale = (
            relative.parts[0] if relative.parts[0] in generator.LOCALES else None
        )
        locale_prefix = f"/{locale}" if locale else ""
        href = (
            f'href="{locale_prefix}/products/cloud/guides/cloud-compatibility'
            '#clickhouse-cloud-architecture"'
        )
        content = component.read_text(encoding="utf-8")
        badge = re.search(
            rf'<a\s+{re.escape(href)}\s+className="cloudNotSupportedBadge">'
            r"(?P<contents>.*?)</a>",
            content,
            re.DOTALL,
        )
        assert badge
        badge_contents = badge.group("contents")
        assert (
            "cloudNotSupportedIcon" in badge_contents
            or "<Icon />" in badge_contents
        )


def test_weekly_workflow_is_standalone_github_actions():
    workflow = (
        REPO_ROOT / ".github/workflows/weekly_cloud_unsupported_features.yml"
    ).read_text(encoding="utf-8")

    assert "praktika" not in workflow.lower()
    assert "cron: '17 8 * * 1'" in workflow
    assert "generate_cloud_unsupported_features.py --write" in workflow
    assert "secrets.ROBOT_CLICKHOUSE_COMMIT_TOKEN" in workflow
    assert "gh pr create" in workflow
