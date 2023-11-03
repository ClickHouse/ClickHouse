import argparse
import hashlib
import json

from typing import Dict, List, Optional
import digest_helper
from docker_images_helper import get_images_info

DOCKER_DIGEST_LEN = 12
JOB_DIGEST_LEN = 10


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    parser.add_argument(
        "--docker",
        default="",
        required=False,
        help="To digest dokers. Provide docker image name or all - for all images, or total - for total digest over all images",
    )
    parser.add_argument(
        "--job",
        default="",
        required=False,
        help="To digest jobs. Provide job name or all - for all jobs",
    )
    parser.add_argument(
        "--outfile",
        default="",
        type=str,
        required=False,
        help="output file to write result to",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="makes output pretty formated",
    )
    return parser.parse_args()


class DockerDigester:
    EXCLUDE_FILES = [".md"]

    def __init__(self):
        self.images_info = get_images_info()
        assert self.images_info, "Fetch image info error"

    def get_image_digest(self, name: str) -> str:
        assert isinstance(name, str)
        deps = [name]
        digest = None
        while deps:
            dep_name = deps.pop(0)
            digest = digest_helper.digest_path(
                self.images_info[dep_name]["path"],
                digest,
                exclude_files=self.EXCLUDE_FILES,
            )
            deps += self.images_info[dep_name]["deps"]
        assert digest
        return digest.hexdigest()[0:DOCKER_DIGEST_LEN]

    def get_all_digests(self) -> Dict:
        res = {}
        for image_name in self.images_info:
            res[image_name] = self.get_image_digest(image_name)
        return res

    def get_total_digest(self) -> str:
        res = []
        for image_name in self.images_info:
            res += [self.get_image_digest(image_name)]
        res.sort()
        return digest_helper.digest_string("-".join(res))[0:DOCKER_DIGEST_LEN]


class JobDigester:
    def __init__(self):
        self.dd = DockerDigester()
        self.cache = {}

    @staticmethod
    def _dict_to_hash_string(data: dict) -> str:
        json_string = json.dumps(data, sort_keys=True)
        hash_obj = hashlib.md5()
        hash_obj.update(json_string.encode())
        hash_string = hash_obj.hexdigest()
        return hash_string

    def get_job_digest(self, digest_config: dict) -> str:
        if digest_config == {}:
            # job is not for digest
            res = "f" * JOB_DIGEST_LEN
        else:
            cache_key = self._dict_to_hash_string(digest_config)
            if cache_key in self.cache:
                return self.cache[cache_key]
            digest_str: List[str] = []
            if "include_paths" in digest_config:
                digest = digest_helper.digest_paths(
                    digest_config["include_paths"],
                    hash_object=None,
                    exclude_files=digest_config["exclude_files"]
                    if "exclude_files" in digest_config
                    else None,
                    exclude_dirs=digest_config["exclude_dirs"]
                    if "exclude_dirs" in digest_config
                    else None,
                )
                digest_str += (digest.hexdigest(),)
            if "docker" in digest_config:
                for image_name in digest_config["docker"]:
                    image_digest = self.dd.get_image_digest(image_name)
                    digest_str += (image_digest,)
            res = digest_helper.digest_string("-".join(digest_str))[0:JOB_DIGEST_LEN]
            self.cache[cache_key] = res
        return res
