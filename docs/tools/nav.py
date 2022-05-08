import collections
import datetime
import hashlib
import logging
import os

import mkdocs.structure.nav

import util


def find_first_header(content):
    for line in content.split("\n"):
        if line.startswith("#"):
            no_hash = line.lstrip("#")
            return no_hash.split("{", 1)[0].strip()


def build_nav_entry(root, args):
    if root.endswith("images"):
        return None, None, None
    result_items = []
    index_meta, index_content = util.read_md_file(os.path.join(root, "index.md"))
    current_title = index_meta.get("toc_folder_title", index_meta.get("toc_title"))
    current_title = current_title or index_meta.get(
        "title", find_first_header(index_content)
    )
    for filename in os.listdir(root):
        path = os.path.join(root, filename)
        if os.path.isdir(path):
            prio, title, payload = build_nav_entry(path, args)
            if title and payload:
                result_items.append((prio, title, payload))
        elif filename.endswith(".md"):
            path = os.path.join(root, filename)

            meta = ""
            content = ""

            try:
                meta, content = util.read_md_file(path)
            except:
                print("Error in file: {}".format(path))
                raise

            path = path.split("/", 2)[-1]
            title = meta.get("toc_title", find_first_header(content))
            if title:
                title = title.strip().rstrip(".")
            else:
                title = meta.get("toc_folder_title", "hidden")
            prio = meta.get("toc_priority", 9999)
            logging.debug(f"Nav entry: {prio}, {title}, {path}")
            if meta.get("toc_hidden") or not content.strip():
                title = "hidden"
            if title == "hidden":
                title = "hidden-" + hashlib.sha1(content.encode("utf-8")).hexdigest()
            if args.nav_limit and len(result_items) >= args.nav_limit:
                break
            result_items.append((prio, title, path))
    result_items = sorted(result_items, key=lambda x: (x[0], x[1]))
    result = collections.OrderedDict([(item[1], item[2]) for item in result_items])
    if index_meta.get("toc_hidden_folder"):
        current_title += "|hidden-folder"
    return index_meta.get("toc_priority", 10000), current_title, result


def build_docs_nav(lang, args):
    docs_dir = os.path.join(args.docs_dir, lang)
    _, _, nav = build_nav_entry(docs_dir, args)
    result = []
    index_key = None
    for key, value in list(nav.items()):
        if key and value:
            if value == "index.md":
                index_key = key
                continue
            result.append({key: value})
        if args.nav_limit and len(result) >= args.nav_limit:
            break
    if index_key:
        key = list(result[0].keys())[0]
        result[0][key][index_key] = "index.md"
        result[0][key].move_to_end(index_key, last=False)
    return result


def build_blog_nav(lang, args):
    blog_dir = os.path.join(args.blog_dir, lang)
    years = sorted(os.listdir(blog_dir), reverse=True)
    result_nav = [{"hidden": "index.md"}]
    post_meta = collections.OrderedDict()
    for year in years:
        year_dir = os.path.join(blog_dir, year)
        if not os.path.isdir(year_dir):
            continue
        result_nav.append({year: collections.OrderedDict()})
        posts = []
        post_meta_items = []
        for post in os.listdir(year_dir):
            post_path = os.path.join(year_dir, post)
            if not post.endswith(".md"):
                raise RuntimeError(
                    f"Unexpected non-md file in posts folder: {post_path}"
                )
            meta, _ = util.read_md_file(post_path)
            post_date = meta["date"]
            post_title = meta["title"]
            if datetime.date.fromisoformat(post_date) > datetime.date.today():
                continue
            posts.append(
                (
                    post_date,
                    post_title,
                    os.path.join(year, post),
                )
            )
            if post_title in post_meta:
                raise RuntimeError(f"Duplicate post title: {post_title}")
            if not post_date.startswith(f"{year}-"):
                raise RuntimeError(
                    f"Post date {post_date} doesn't match the folder year {year}: {post_title}"
                )
            post_url_part = post.replace(".md", "")
            post_meta_items.append(
                (
                    post_date,
                    {
                        "date": post_date,
                        "title": post_title,
                        "image": meta.get("image"),
                        "url": f"/blog/{lang}/{year}/{post_url_part}/",
                    },
                )
            )
        for _, title, path in sorted(posts, reverse=True):
            result_nav[-1][year][title] = path
        for _, post_meta_item in sorted(
            post_meta_items, reverse=True, key=lambda item: item[0]
        ):
            post_meta[post_meta_item["title"]] = post_meta_item
    return result_nav, post_meta


def _custom_get_navigation(files, config):
    nav_config = config["nav"] or mkdocs.structure.nav.nest_paths(
        f.src_path for f in files.documentation_pages()
    )
    items = mkdocs.structure.nav._data_to_navigation(nav_config, files, config)
    if not isinstance(items, list):
        items = [items]

    pages = mkdocs.structure.nav._get_by_type(items, mkdocs.structure.nav.Page)

    mkdocs.structure.nav._add_previous_and_next_links(pages)
    mkdocs.structure.nav._add_parent_links(items)

    missing_from_config = [
        file for file in files.documentation_pages() if file.page is None
    ]
    if missing_from_config:
        files._files = [
            file for file in files._files if file not in missing_from_config
        ]

    links = mkdocs.structure.nav._get_by_type(items, mkdocs.structure.nav.Link)
    for link in links:
        scheme, netloc, path, params, query, fragment = mkdocs.structure.nav.urlparse(
            link.url
        )
        if scheme or netloc:
            mkdocs.structure.nav.log.debug(
                "An external link to '{}' is included in "
                "the 'nav' configuration.".format(link.url)
            )
        elif link.url.startswith("/"):
            mkdocs.structure.nav.log.debug(
                "An absolute path to '{}' is included in the 'nav' configuration, "
                "which presumably points to an external resource.".format(link.url)
            )
        else:
            msg = (
                "A relative path to '{}' is included in the 'nav' configuration, "
                "which is not found in the documentation files".format(link.url)
            )
            mkdocs.structure.nav.log.warning(msg)
    return mkdocs.structure.nav.Navigation(items, pages)


mkdocs.structure.nav.get_navigation = _custom_get_navigation
