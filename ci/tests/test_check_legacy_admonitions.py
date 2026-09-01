"""Regression tests for the canonical documentation admonition guard."""

import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).parents[1]
    / "jobs"
    / "scripts"
    / "docs"
    / "check_legacy_admonitions.py"
)
SPEC = importlib.util.spec_from_file_location("check_legacy_admonitions", SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class TestLegacyAdmonitionGuard(unittest.TestCase):
    @staticmethod
    def temporary_repo():
        scratch_root = Path(__file__).parents[2] / "tmp"
        scratch_root.mkdir(exist_ok=True)
        return tempfile.TemporaryDirectory(dir=scratch_root)

    def test_untitled_opener_split_across_literals_is_rejected(self):
        fixtures = [
            (
                Path("src/example.cpp"),
                'constexpr auto doc = ":::note" "\\nBody\\n:::";\n',
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT ':::note' || '\\nBody\\n:::';\n",
            ),
            (
                Path("src/example.cpp"),
                'constexpr auto doc = ":::note" /* separator */ "\\nBody\\n:::";\n',
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT ':::note' /* separator */ || '\\nBody\\n:::';\n",
            ),
            (
                Path("src/example.cpp"),
                'constexpr auto doc = ":::note" // separator\n    "\\nBody\\n:::";\n',
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT ':::note' -- separator\n    || '\\nBody\\n:::';\n",
            ),
            (
                Path("src/example.cpp"),
                'constexpr auto doc = "Introduction\\n" ":::note\\nBody\\n:::";\n',
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT 'Introduction\\n' || ':::note\\nBody\\n:::';\n",
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT (':::note') || ('\\nBody\\n:::');\n",
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT (( ':::note' /* inner */ )) /* outer */ "
                "|| /* outer */ (( /* inner */ '\\nBody\\n:::' ));\n",
            ),
        ]
        for relative_path, content in fixtures:
            with self.subTest(relative_path=relative_path), self.temporary_repo() as root:
                repo_root = Path(root)
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")

                findings = CHECK.find_legacy_admonitions(repo_root)

                self.assertEqual(len(findings), 1)
                self.assertTrue(findings[0].startswith(f"{relative_path}:1:"))

    def test_syntax_mentions_are_not_rejected(self):
        fixtures = [
            (Path("src/example.cpp"), 'constexpr auto syntax = ":::note";\n'),
            (Path("src/example.cpp"), 'constexpr auto syntax = ":::note" "suffix";\n'),
            (
                Path("src/example.cpp"),
                'constexpr auto syntax = ":::note" /* separator */ "suffix";\n',
            ),
            (
                Path("src/example.cpp"),
                'constexpr auto syntax = "Use :::note\\nBody\\n::: as legacy syntax.";\n',
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT ':::note' || 'suffix';\n",
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT ':::note' /* separator */ || 'suffix';\n",
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT 'Use :::note\\nBody\\n::: as legacy syntax.';\n",
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT (':::note') || ('suffix');\n",
            ),
            (
                Path("ci/jobs/scripts/docs/autogenerate/sql/example.sql"),
                "SELECT ('Use :::note\\nBody\\n::: as legacy syntax.');\n",
            ),
        ]
        for relative_path, content in fixtures:
            with self.subTest(relative_path=relative_path), self.temporary_repo() as root:
                repo_root = Path(root)
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")

                self.assertEqual(CHECK.find_legacy_admonitions(repo_root), [])


if __name__ == "__main__":
    unittest.main()
