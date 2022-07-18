#!/usr/bin/env python3

import logging
import os
import sys
import bs4
import subprocess


def test_single_page(input_path, lang):
    if not (lang == "en"):
        return

    with open(input_path) as f:
        soup = bs4.BeautifulSoup(f, features="html.parser")

        anchor_points = set()

        duplicate_anchor_points = 0
        links_to_nowhere = 0

        for tag in soup.find_all():
            for anchor_point in [tag.attrs.get("name"), tag.attrs.get("id")]:
                if anchor_point:
                    anchor_points.add(anchor_point)

        for tag in soup.find_all():
            href = tag.attrs.get("href")
            if href and href.startswith("#") and href != "#":
                if href[1:] not in anchor_points:
                    links_to_nowhere += 1
                    logging.info("Tag %s", tag)
                    logging.info("Link to nowhere: %s" % href)

        if links_to_nowhere:
            logging.error(f"Found {links_to_nowhere} links to nowhere in {lang}")
            sys.exit(1)

        if len(anchor_points) <= 10:
            logging.error("Html parsing is probably broken")
            sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    test_single_page(sys.argv[1], sys.argv[2])
