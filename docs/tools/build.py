#!/usr/bin/env python3

from pathlib import Path
import argparse
import logging
import shutil
import sys

import livereload


def write_redirect_html(output_path: Path, to_url: str) -> None:
    output_dir = output_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        f"""<!--[if IE 6]> Redirect: {to_url} <![endif]-->
<!DOCTYPE HTML>
<html lang="en-US">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="0; url={to_url}">
        <script type="text/javascript">
            window.location.href = "{to_url}";
        </script>
        <title>Page Redirection</title>
    </head>
    <body>
        If you are not redirected automatically, follow this <a href="{to_url}">link</a>.
    </body>
</html>"""
    )


def build_static_redirects(output_dir: Path):
    for static_redirect in [
        ("benchmark.html", "/benchmark/dbms/"),
        ("benchmark_hardware.html", "/benchmark/hardware/"),
        (
            "tutorial.html",
            "/docs/en/getting_started/tutorial/",
        ),
        (
            "reference_en.html",
            "/docs/en/single/",
        ),
        (
            "reference_ru.html",
            "/docs/ru/single/",
        ),
        (
            "docs/index.html",
            "/docs/en/",
        ),
    ]:
        write_redirect_html(output_dir / static_redirect[0], static_redirect[1])


def build(root_dir: Path, output_dir: Path):
    if output_dir.exists():
        shutil.rmtree(args.output_dir)

    (output_dir / "data").mkdir(parents=True)

    logging.info("Building website")

    # This file can be requested to check for available ClickHouse releases.
    shutil.copy2(
        root_dir / "utils" / "list-versions" / "version_date.tsv",
        output_dir / "data" / "version_date.tsv",
    )

    # This file can be requested to install ClickHouse.
    shutil.copy2(
        root_dir / "docs" / "_includes" / "install" / "universal.sh",
        output_dir / "data" / "install.sh",
    )

    build_static_redirects(output_dir)


if __name__ == "__main__":
    root_dir = Path(__file__).parent.parent.parent
    docs_dir = root_dir / "docs"

    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    arg_parser.add_argument(
        "--output-dir",
        type=Path,
        default=docs_dir / "build",
        help="path to the output dir",
    )
    arg_parser.add_argument("--livereload", type=int, default="0")
    arg_parser.add_argument("--verbose", action="store_true")

    args = arg_parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr
    )

    build(root_dir, args.output_dir)

    if args.livereload:
        server = livereload.Server()
        server.serve(root=args.output_dir, host="0.0.0.0", port=args.livereload)
        sys.exit(0)
