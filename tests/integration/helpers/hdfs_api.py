# -*- coding: utf-8 -*-
import gzip
import io
import logging
import time

import requests


class HDFSApi(object):
    """A minimal WebHDFS client used to prepare data for HDFS integration tests.

    Kerberos support existed here once, but it was removed together with the
    support for Kerberized HDFS in ClickHouse itself.
    """

    def __init__(
        self,
        user,
        host,
        proxy_port,
        data_port,
        timeout=100,
        protocol="http",
        hdfs_ip=None,
    ):
        self.host = host
        self.protocol = protocol
        self.proxy_port = proxy_port
        self.data_port = data_port
        self.user = user
        self.timeout = timeout
        self.hdfs_ip = hdfs_ip

    def req_wrapper(self, func, expected_code, cnt=2, **kwargs):
        # Bound every request, so that a nonresponsive NameNode or DataNode fails
        # the test quickly instead of hanging the whole shard.
        kwargs.setdefault("timeout", self.timeout)
        for i in range(0, cnt):
            logging.debug(f"CALL: {str(kwargs)}")
            response_data = func(**kwargs)
            logging.debug(
                f"response_data:{response_data.content} headers:{response_data.headers}"
            )
            if response_data.status_code == expected_code:
                return response_data
            else:
                logging.error(
                    f"unexpected response_data.status_code {response_data.status_code} != {expected_code}"
                )
                time.sleep(1)
        response_data.raise_for_status()

    def _redirect_to_data_port(self, location):
        # The namenode redirects to the datanode by its hostname, which is not
        # resolvable from outside the docker network.
        return location.replace(
            "{}:{}".format(self.host, self.data_port),
            "{}:{}".format(self.hdfs_ip, self.data_port),
        )

    def read_data(self, path, universal_newlines=True):
        logging.debug(
            "read_data protocol:{} host:{} ip:{} proxy port:{} data port:{} path: {}".format(
                self.protocol,
                self.host,
                self.hdfs_ip,
                self.proxy_port,
                self.data_port,
                path,
            )
        )
        response = self.req_wrapper(
            requests.get,
            307,
            url="{protocol}://{ip}:{port}/webhdfs/v1{path}?op=OPEN".format(
                protocol=self.protocol, ip=self.hdfs_ip, port=self.proxy_port, path=path
            ),
            headers={"host": str(self.hdfs_ip)},
            allow_redirects=False,
            verify=False,
        )
        location = self._redirect_to_data_port(response.headers["Location"])
        logging.debug("redirected to {}".format(location))

        response_data = self.req_wrapper(
            requests.get,
            200,
            url=location,
            headers={"host": self.hdfs_ip},
            verify=False,
        )

        if universal_newlines:
            return response_data.text
        else:
            return response_data.content

    def write_data(self, path, content):
        logging.debug(
            "write_data protocol:{} host:{} port:{} path: {} user:{}".format(
                self.protocol, self.host, self.proxy_port, path, self.user
            )
        )
        if isinstance(content, str):
            content = content.encode()

        response = self.req_wrapper(
            requests.put,
            307,
            url="{protocol}://{ip}:{port}/webhdfs/v1{path}?op=CREATE".format(
                protocol=self.protocol,
                ip=self.hdfs_ip,
                port=self.proxy_port,
                path=path,
            ),
            allow_redirects=False,
            headers={"host": str(self.hdfs_ip)},
            params={"overwrite": "true"},
            verify=False,
        )

        logging.debug("HDFS api response:{}".format(response.headers))
        location = self._redirect_to_data_port(response.headers["Location"])

        response = self.req_wrapper(
            requests.put,
            201,
            url=location,
            data=content,
            headers={"content-type": "text/plain", "host": str(self.hdfs_ip)},
            params={"file": path, "user.name": self.user},
            allow_redirects=False,
            verify=False,
        )
        logging.debug(f"{response.content} {response.headers}")

    def write_file(self, path, local_path):
        with open(local_path, mode="rb") as fh:
            self.write_data(path, fh.read())

    def write_gzip_data(self, path, content):
        if isinstance(content, str):
            content = content.encode()
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
            f.write(content)
        self.write_data(path, out.getvalue())

    def read_gzip_data(self, path):
        return (
            gzip.GzipFile(
                fileobj=io.BytesIO(self.read_data(path, universal_newlines=False))
            )
            .read()
            .decode()
        )
