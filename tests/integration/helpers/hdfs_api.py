#-*- coding: utf-8 -*-
import StringIO
import gzip
import requests
import subprocess
from tempfile import NamedTemporaryFile

class HDFSApi(object):
    def __init__(self, user):
        self.host = "localhost"
        self.http_proxy_port = "50070"
        self.http_data_port = "50075"
        self.user = user

    def read_data(self, path):
        response = requests.get("http://{host}:{port}/webhdfs/v1{path}?op=OPEN".format(host=self.host, port=self.http_proxy_port, path=path), allow_redirects=False)
        if response.status_code != 307:
            response.raise_for_status()
        additional_params = '&'.join(response.headers['Location'].split('&')[1:2])
        response_data = requests.get("http://{host}:{port}/webhdfs/v1{path}?op=OPEN&{params}".format(host=self.host, port=self.http_data_port, path=path, params=additional_params))
        if response_data.status_code != 200:
            response_data.raise_for_status()

        return response_data.content

    # Requests can't put file
    def _curl_to_put(self, filename, path, params):
        url = "http://{host}:{port}/webhdfs/v1{path}?op=CREATE&{params}".format(host=self.host, port=self.http_data_port, path=path, params=params)
        cmd = "curl -s -i -X PUT -T {fname} '{url}'".format(fname=filename, url=url)
        output = subprocess.check_output(cmd, shell=True)
        return output

    def write_data(self, path, content):
        named_file = NamedTemporaryFile()
        fpath = named_file.name
        named_file.write(content)
        named_file.flush()
        response = requests.put(
            "http://{host}:{port}/webhdfs/v1{path}?op=CREATE".format(host=self.host, port=self.http_proxy_port, path=path, user=self.user),
            allow_redirects=False
        )
        if response.status_code != 307:
            response.raise_for_status()

        additional_params = '&'.join(response.headers['Location'].split('&')[1:2] + ["user.name={}".format(self.user), "overwrite=true"])
        output = self._curl_to_put(fpath, path, additional_params)
        if "201 Created" not in output:
            raise Exception("Can't create file on hdfs:\n {}".format(output))

    def write_gzip_data(self, path, content):
        out = StringIO.StringIO()
        with gzip.GzipFile(fileobj=out, mode="w") as f:
            f.write(content)
        self.write_data(path, out.getvalue())

    def read_gzip_data(self, path):
        return gzip.GzipFile(fileobj=StringIO.StringIO(self.read_data(path))).read()
