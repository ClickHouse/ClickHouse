import os
import random
import re
import string
import tempfile
import threading
import xml.etree.ElementTree

import minio


# By default the exceptions that was throwed in threads will be ignored
# (they will not mark the test as failed, only printed to stderr).
# Wrap thrading.Thread and re-throw exception on join()
class SafeThread(threading.Thread):
    def __init__(self, target):
        super().__init__()
        self.target = target
        self.exception = None

    def run(self):
        try:
            self.target()
        except Exception as e:  # pylint: disable=broad-except
            self.exception = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exception:
            raise self.exception


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def replace_xml_by_xpath(input_path, output_path, replace_text={}, remove_node=[]):
    def no_warn(xpath):
        if xpath.startswith("//"):
            return f".{xpath}"
        else:
            return xpath
    tree = xml.etree.ElementTree.parse(input_path)
    for xpath, value in replace_text.items():
        nodes = tree.findall(no_warn(xpath))
        assert nodes, f"{repr(xpath)} could not be found in {input_path}"
        for node in nodes:
            node.text = value
    if remove_node:
        parent_map = {c: p for p in tree.iter() for c in p}
        for xpath in remove_node:
            nodes = tree.findall(no_warn(xpath))
            assert nodes, f"{repr(xpath)} could not be found in {input_path}"
            for node in nodes:
                parent_map[node].remove(node)
    tree.write(output_path)


class StorageConfigurator:
    def __init__(self):
        self.shall_override_storage = False

        if os.environ.get("CLICKHOUSE_AWS_ENDPOINT_URL_OVERRIDE") and os.environ.get("CLICKHOUSE_AWS_ACCESS_KEY_ID") and os.environ.get("CLICKHOUSE_AWS_SECRET_ACCESS_KEY"):
            self.url = os.environ["CLICKHOUSE_AWS_ENDPOINT_URL_OVERRIDE"]
            self.key_id = os.environ["CLICKHOUSE_AWS_ACCESS_KEY_ID"]
            self.secret_key = os.environ["CLICKHOUSE_AWS_SECRET_ACCESS_KEY"]

            parts = re.findall("^((https?)://(s3\\.(.*?)\\.amazonaws\\.com))(/(.*?)(/.*/))$", self.url)
            if parts:
                [[self.base_url, self.scheme, self.host_name, self.region, self.path, self.bucket, self.key]] = parts
                self.shall_override_storage = True

            parts = re.findall("^((https?)://(storage\\.googleapis\\.com))(/(.*?)(/.*/))$", self.url)
            if parts:
                [[self.base_url, self.scheme, self.host_name, self.bucket, self.path, self.key]] = parts
                self.region = "us-east-1"
                self.shall_override_storage = True

        if self.shall_override_storage:
            self.minio_client = minio.Minio(self.host_name, access_key=self.key_id, secret_key=self.secret_key, region=self.region, secure=False)
            self.temporary_directory = tempfile.TemporaryDirectory()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.temporary_directory.cleanup()

    def new_file_name(self, base_name):
        assert "/" not in base_name
        return os.path.join(self.temporary_directory.name, base_name)
