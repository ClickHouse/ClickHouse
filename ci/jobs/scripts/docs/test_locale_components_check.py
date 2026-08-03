#!/usr/bin/env python3

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from locale_components_check import main


class LocaleComponentsCheckTest(unittest.TestCase):
    def test_legacy_badge_paths_are_reported_and_fixed(self):
        scratch = Path.cwd() / "tmp"
        scratch.mkdir(exist_ok=True)

        with tempfile.TemporaryDirectory(dir=scratch) as temp_dir:
            docs_root = Path(temp_dir)
            components = {
                "BetaBadge": "beta-features",
                "ExperimentalBadge": "experimental-features",
            }
            for component_name, anchor in components.items():
                component = (
                    docs_root
                    / "snippets"
                    / "ru"
                    / "components"
                    / component_name
                    / f"{component_name}.jsx"
                )
                component.parent.mkdir(parents=True)
                component.write_text(
                    '<a href="/docs/beta-and-experimental-features#'
                    f'{anchor}">{component_name}</a>\n',
                    encoding="utf-8",
                )

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main([str(docs_root)]), 1)
            self.assertIn("legacy-badge-path", output.getvalue())
            self.assertIn("#beta-features", output.getvalue())
            self.assertIn("#experimental-features", output.getvalue())

            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main([str(docs_root), "--fix"]), 0)
            for component_name, anchor in components.items():
                component = (
                    docs_root
                    / "snippets"
                    / "ru"
                    / "components"
                    / component_name
                    / f"{component_name}.jsx"
                )
                self.assertIn(
                    f"/ru/reference/settings/beta-and-experimental-features#{anchor}",
                    component.read_text(encoding="utf-8"),
                )


if __name__ == "__main__":
    unittest.main()
