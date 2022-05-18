import collections
import copy
import io
import logging
import os
import random
import sys
import tarfile
import time

import requests

import util


def get_events(args):
    events = []
    skip = True
    with open(os.path.join(args.docs_dir, "..", "README.md")) as f:
        for line in f:
            if skip:
                if "Upcoming Events" in line:
                    skip = False
            else:
                if not line:
                    continue
                line = line.strip().split("](")
                if len(line) == 2:
                    tail = line[1].split(") ")
                    events.append(
                        {
                            "signup_link": tail[0],
                            "event_name": line[0].replace("* [", ""),
                            "event_date": tail[1].replace("on ", "").replace(".", ""),
                        }
                    )
    return events


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
