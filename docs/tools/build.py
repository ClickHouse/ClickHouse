#!/usr/bin/env python3

import argparse
import logging
import os
import shutil
import subprocess
import sys

import livereload

import redirects
import website


def build(args):
    if os.path.exists(args.output_dir):
        shutil.rmtree(args.output_dir)

    if not args.skip_website:
        website.build_website(args)

    if not args.skip_website:
        website.process_benchmark_results(args)
        website.minify_website(args)
        redirects.build_static_redirects(args)


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(__file__), ".."))

    # A root path to ClickHouse source code.
    src_dir = ".."

    website_dir = os.path.join(src_dir, "website")

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--lang", default="en,ru,zh,ja")
    arg_parser.add_argument("--theme-dir", default=website_dir)
    arg_parser.add_argument("--website-dir", default=website_dir)
    arg_parser.add_argument("--src-dir", default=src_dir)
    arg_parser.add_argument("--output-dir", default="build")
    arg_parser.add_argument("--nav-limit", type=int, default="0")
    arg_parser.add_argument("--skip-multi-page", action="store_true")
    arg_parser.add_argument("--skip-website", action="store_true")
    arg_parser.add_argument("--htmlproofer", action="store_true")
    arg_parser.add_argument("--livereload", type=int, default="0")
    arg_parser.add_argument("--verbose", action="store_true")

    args = arg_parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr
    )

    logging.getLogger("MARKDOWN").setLevel(logging.INFO)

    args.rev = (
        subprocess.check_output("git rev-parse HEAD", shell=True)
        .decode("utf-8")
        .strip()
    )
    args.rev_short = (
        subprocess.check_output("git rev-parse --short HEAD", shell=True)
        .decode("utf-8")
        .strip()
    )
    args.rev_url = f"https://github.com/ClickHouse/ClickHouse/commit/{args.rev}"

    build(args)

    if args.livereload:
        new_args = [arg for arg in sys.argv if not arg.startswith("--livereload")]
        new_args = sys.executable + " " + " ".join(new_args)

        server = livereload.Server()
        server.watch(
            args.website_dir + "**/*",
            livereload.shell(new_args, cwd="tools", shell=True),
        )
        server.serve(root=args.output_dir, host="0.0.0.0", port=args.livereload)
        sys.exit(0)
