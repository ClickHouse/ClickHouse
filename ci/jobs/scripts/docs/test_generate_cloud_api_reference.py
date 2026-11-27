#!/usr/bin/env python3
"""Regression tests for Cloud API reference URL preservation."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path


HERE = Path(__file__).resolve().parent
GENERATOR = HERE / "generate_cloud_api_reference.py"
LOCALES = ("ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh")
API_PREFIX = "/products/cloud/api-reference/widgets"


def spec(summary: str, *, beta: bool = False, deprecated: bool = False) -> dict:
    description = "This endpoint is in beta." if beta else "Returns widgets."
    return {
        "x-tagGroups": [{"name": "Widgets", "tags": ["Widgets"]}],
        "paths": {
            "/v1/widgets": {
                "get": {
                    "operationId": "getWidget",
                    "summary": summary,
                    "description": description,
                    "tags": ["Widgets"],
                    "deprecated": deprecated,
                }
            }
        },
    }


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def create_docs_fixture(docs: Path, initial_spec: dict) -> None:
    write_json(docs / "_specs/cloud-openapi.json", initial_spec)
    write_json(docs / "_site/redirects.json", [])
    write_json(
        docs / "products/cloud/navigation.json",
        [
            {
                "group": "API reference",
                "openapi": "_specs/cloud-openapi.json",
                "pages": [],
            }
        ],
    )
    for locale in LOCALES:
        write_json(docs / locale / "products/cloud/api-reference/navigation.json", [])


def run_generator(docs: Path, input_spec: Path, *mode: str) -> None:
    subprocess.run(
        [sys.executable, str(GENERATOR), "--spec", str(input_spec), "--docs-dir", str(docs), *mode],
        check=True,
    )


def redirects(docs: Path) -> dict[str, str]:
    return {
        redirect["source"]: redirect["destination"]
        for redirect in json.loads((docs / "_site/redirects.json").read_text(encoding="utf-8"))
    }


def main() -> int:
    with tempfile.TemporaryDirectory() as root:
        root_path = Path(root)
        docs = root_path / "docs"
        beta_spec = spec("Get widgets", beta=True)
        create_docs_fixture(docs, beta_spec)
        beta_input = root_path / "beta.json"
        write_json(beta_input, beta_spec)
        run_generator(docs, beta_input, "--write")

        # A beta endpoint becomes GA, so its operation-ID badge page is removed
        # and the former public URL redirects to the summary-based URL.
        ga_input = root_path / "ga.json"
        write_json(ga_input, spec("List widgets"))
        run_generator(docs, ga_input, "--write")
        first_redirects = redirects(docs)
        badge_url = f"{API_PREFIX}/get-widget"
        ga_url = f"{API_PREFIX}/list-widgets"
        assert first_redirects[badge_url] == ga_url
        run_generator(docs, ga_input, "--check")

        # A later GA summary rename keeps the old canonical English and locale
        # URLs, and reconciles the original beta alias to the new destination.
        renamed_input = root_path / "renamed.json"
        write_json(renamed_input, spec("Browse widgets"))
        run_generator(docs, renamed_input, "--write")
        renamed_redirects = redirects(docs)
        renamed_url = f"{API_PREFIX}/browse-widgets"
        assert renamed_redirects[badge_url] == renamed_url
        assert renamed_redirects[ga_url] == renamed_url
        for locale in LOCALES:
            old_locale_url = f"/{locale}{API_PREFIX}/list-widgets"
            new_locale_url = f"/{locale}{API_PREFIX}/browse-widgets"
            assert renamed_redirects[old_locale_url] == new_locale_url
        run_generator(docs, renamed_input, "--check")

        # When the operation becomes deprecated its badge URL is canonical
        # again, so it is never retained as an alias or redirect cycle source.
        deprecated_input = root_path / "deprecated.json"
        write_json(deprecated_input, spec("Browse widgets", deprecated=True))
        run_generator(docs, deprecated_input, "--write")
        deprecated_redirects = redirects(docs)
        assert badge_url not in deprecated_redirects
        assert deprecated_redirects[ga_url] == badge_url
        for locale in LOCALES:
            renamed_locale_url = f"/{locale}{API_PREFIX}/browse-widgets"
            assert deprecated_redirects[renamed_locale_url] == badge_url
        assert all(source != destination for source, destination in deprecated_redirects.items())
        run_generator(docs, deprecated_input, "--check")

    print("OK: Cloud API reference redirects preserve endpoint URLs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
