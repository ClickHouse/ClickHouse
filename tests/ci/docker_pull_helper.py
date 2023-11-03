#!/usr/bin/env python3

import subprocess
import logging

from typing import Optional


def pull_image(image: str, tag: Optional[str] = None) -> str:
    try:
        tag = tag or "latest"
        image_tag = f"{image}:{tag}"
        logging.info("Pulling image %s - start", image_tag)
        subprocess.check_output(
            f"docker pull {image_tag}",
            stderr=subprocess.STDOUT,
            shell=True,
        )
        logging.info("Pulling image %s - done", image_tag)
    except Exception as ex:
        logging.info("Got execption pulling docker %s", ex)
    return image_tag
