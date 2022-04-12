import logging
import os
import re
import shutil
import subprocess
import yaml

import bs4
import mkdocs.commands.build

import test
import util
import website

TEMPORARY_FILE_NAME = "single.md"


def recursive_values(item):
    if isinstance(item, dict):
        for _, value in list(item.items()):
            yield from recursive_values(value)
    elif isinstance(item, list):
        for value in item:
            yield from recursive_values(value)
    elif isinstance(item, str):
        yield item


anchor_not_allowed_chars = re.compile(r"[^\w\-]")


def generate_anchor_from_path(path):
    return re.sub(anchor_not_allowed_chars, "-", path)


absolute_link = re.compile(r"^https?://")


def replace_link(match, path):
    title = match.group(1)
    link = match.group(2)

    # Not a relative link
    if re.search(absolute_link, link):
        return match.group(0)

    if link.endswith("/"):
        link = link[0:-1] + ".md"

    return "{}(#{})".format(
        title,
        generate_anchor_from_path(
            os.path.normpath(os.path.join(os.path.dirname(path), link))
        ),
    )


# Concatenates Markdown files to a single file.
def concatenate(lang, docs_path, single_page_file, nav):
    lang_path = os.path.join(docs_path, lang)

    proj_config = f"{docs_path}/toc_{lang}.yml"
    if os.path.exists(proj_config):
        with open(proj_config) as cfg_file:
            nav = yaml.full_load(cfg_file.read())["nav"]

    files_to_concatenate = list(recursive_values(nav))
    files_count = len(files_to_concatenate)
    logging.info(
        f"{files_count} files will be concatenated into single md-file for {lang}."
    )
    logging.debug("Concatenating: " + ", ".join(files_to_concatenate))
    assert files_count > 0, f"Empty single-page for {lang}"

    link_regexp = re.compile(r"(\[[^\]]+\])\(([^)#]+)(?:#[^\)]+)?\)")

    for path in files_to_concatenate:
        try:
            with open(os.path.join(lang_path, path)) as f:
                # Insert a horizontal ruler. Then insert an anchor that we will link to. Its name will be a path to the .md file.
                single_page_file.write(
                    '\n______\n<a name="%s"></a>\n' % generate_anchor_from_path(path)
                )

                in_metadata = False
                for line in f:
                    # Skip YAML metadata.
                    if line == "---\n":
                        in_metadata = not in_metadata
                        continue

                    if not in_metadata:
                        # Increase the level of headers.
                        if line.startswith("#"):
                            line = "#" + line

                        # Replace links within the docs.

                        if re.search(link_regexp, line):
                            line = re.sub(
                                link_regexp,
                                lambda match: replace_link(match, path),
                                line,
                            )

                            # If failed to replace the relative link, print to log
                            # But with some exceptions:
                            # - "../src/" -- for cmake-in-clickhouse.md (link to sources)
                            # - "../usr/share" -- changelog entry that has "../usr/share/zoneinfo"
                            if (
                                "../" in line
                                and (not "../usr/share" in line)
                                and (not "../src/" in line)
                            ):
                                logging.info("Failed to resolve relative link:")
                                logging.info(path)
                                logging.info(line)

                        single_page_file.write(line)

        except IOError as e:
            logging.warning(str(e))

    single_page_file.flush()


def get_temporary_file_name(lang, args):
    return os.path.join(args.docs_dir, lang, TEMPORARY_FILE_NAME)


def remove_temporary_files(lang, args):
    single_md_path = get_temporary_file_name(lang, args)
    if os.path.exists(single_md_path):
        os.unlink(single_md_path)


def build_single_page_version(lang, args, nav, cfg):
    logging.info(f"Building single page version for {lang}")
    os.environ["SINGLE_PAGE"] = "1"
    extra = cfg.data["extra"]
    extra["single_page"] = True
    extra["is_amp"] = False

    single_md_path = get_temporary_file_name(lang, args)
    with open(single_md_path, "w") as single_md:
        concatenate(lang, args.docs_dir, single_md, nav)

        with util.temp_dir() as site_temp:
            with util.temp_dir() as docs_temp:
                docs_src_lang = os.path.join(args.docs_dir, lang)
                docs_temp_lang = os.path.join(docs_temp, lang)
                shutil.copytree(docs_src_lang, docs_temp_lang)
                for root, _, filenames in os.walk(docs_temp_lang):
                    for filename in filenames:
                        if filename != "single.md" and filename.endswith(".md"):
                            os.unlink(os.path.join(root, filename))

                cfg.load_dict(
                    {
                        "docs_dir": docs_temp_lang,
                        "site_dir": site_temp,
                        "extra": extra,
                        "nav": [{cfg.data.get("site_name"): "single.md"}],
                    }
                )

                if not args.test_only:
                    mkdocs.commands.build.build(cfg)

                    single_page_output_path = os.path.join(
                        args.docs_dir, args.docs_output_dir, lang, "single"
                    )

                    if os.path.exists(single_page_output_path):
                        shutil.rmtree(single_page_output_path)

                    shutil.copytree(
                        os.path.join(site_temp, "single"), single_page_output_path
                    )

                    single_page_index_html = os.path.join(
                        single_page_output_path, "index.html"
                    )
                    single_page_content_js = os.path.join(
                        single_page_output_path, "content.js"
                    )

                    with open(single_page_index_html, "r") as f:
                        sp_prefix, sp_js, sp_suffix = f.read().split("<!-- BREAK -->")

                    with open(single_page_index_html, "w") as f:
                        f.write(sp_prefix)
                        f.write(sp_suffix)

                    with open(single_page_content_js, "w") as f:
                        if args.minify:
                            import jsmin

                            sp_js = jsmin.jsmin(sp_js)
                        f.write(sp_js)

                logging.info(f"Re-building single page for {lang} pdf/test")
                with util.temp_dir() as test_dir:
                    extra["single_page"] = False
                    cfg.load_dict(
                        {
                            "docs_dir": docs_temp_lang,
                            "site_dir": test_dir,
                            "extra": extra,
                            "nav": [{cfg.data.get("site_name"): "single.md"}],
                        }
                    )
                    mkdocs.commands.build.build(cfg)

                    css_in = " ".join(website.get_css_in(args))
                    js_in = " ".join(website.get_js_in(args))
                    subprocess.check_call(
                        f"cat {css_in} > {test_dir}/css/base.css", shell=True
                    )
                    subprocess.check_call(
                        f"cat {js_in} > {test_dir}/js/base.js", shell=True
                    )

                    if args.save_raw_single_page:
                        shutil.copytree(test_dir, args.save_raw_single_page)

                    logging.info(f"Running tests for {lang}")
                    test.test_single_page(
                        os.path.join(test_dir, "single", "index.html"), lang
                    )

        logging.info(f"Finished building single page version for {lang}")

        remove_temporary_files(lang, args)
