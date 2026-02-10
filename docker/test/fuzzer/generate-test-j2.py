#!/usr/bin/env python3

import itertools
import os
from argparse import ArgumentParser

import jinja2


def removesuffix(text, suffix):
    """
    Added in python 3.9
    https://www.python.org/dev/peps/pep-0616/
    """
    if suffix and text.endswith(suffix):
        return text[: -len(suffix)]
    else:
        return text[:]


def render_test_template(j2env, suite_dir, test_name):
    """
    Render template for test and reference file if needed
    """

    test_base_name = removesuffix(test_name, ".sql.j2")

    reference_file_name = test_base_name + ".reference.j2"
    reference_file_path = os.path.join(suite_dir, reference_file_name)
    if os.path.isfile(reference_file_path):
        tpl = j2env.get_template(reference_file_name)
        tpl.stream().dump(os.path.join(suite_dir, test_base_name) + ".gen.reference")

    if test_name.endswith(".sql.j2"):
        tpl = j2env.get_template(test_name)
        generated_test_name = test_base_name + ".gen.sql"
        tpl.stream().dump(os.path.join(suite_dir, generated_test_name))
        return generated_test_name

    return test_name


def main(args):
    suite_dir = args.path

    print(f"Scanning {suite_dir} directory...")

    j2env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(suite_dir),
        keep_trailing_newline=True,
    )
    j2env.globals.update(product=itertools.product)

    test_names = os.listdir(suite_dir)
    for test_name in test_names:
        if not test_name.endswith(".sql.j2"):
            continue
        new_name = render_test_template(j2env, suite_dir, test_name)
        print(f"File {new_name} generated")


if __name__ == "__main__":
    parser = ArgumentParser(description="Jinja2 test generator")
    parser.add_argument("-p", "--path", help="Path to test dir", required=True)
    main(parser.parse_args())
