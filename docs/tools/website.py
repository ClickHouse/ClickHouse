import hashlib
import json
import logging
import os
import shutil
import subprocess

import util


def build_website(args):
    logging.info("Building website")
    env = util.init_jinja2_env(args)

    shutil.copytree(
        args.website_dir,
        args.output_dir,
        ignore=shutil.ignore_patterns(
            "*.md",
            "*.sh",
            "*.css",
            "*.json",
            "js/*.js",
            "build",
            "docs",
            "public",
            "node_modules",
            "src",
            "templates",
            "locale",
            ".gitkeep",
        ),
    )

    # This file can be requested to check for available ClickHouse releases.
    shutil.copy2(
        os.path.join(args.src_dir, "utils", "list-versions", "version_date.tsv"),
        os.path.join(args.output_dir, "data", "version_date.tsv"),
    )

    # This file can be requested to install ClickHouse.
    shutil.copy2(
        os.path.join(args.src_dir, "docs", "_includes", "install", "universal.sh"),
        os.path.join(args.output_dir, "data", "install.sh"),
    )

    for root, _, filenames in os.walk(args.output_dir):
        for filename in filenames:
            if filename == "main.html":
                continue

            path = os.path.join(root, filename)
            if not filename.endswith(".html"):
                continue
            logging.info("Processing %s", path)
            with open(path, "rb") as f:
                content = f.read().decode("utf-8")

            template = env.from_string(content)
            content = template.render(args.__dict__)

            with open(path, "wb") as f:
                f.write(content.encode("utf-8"))
