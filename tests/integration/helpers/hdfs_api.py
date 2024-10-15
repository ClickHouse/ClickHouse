# -*- coding: utf-8 -*-
import gzip
import io
import logging
import os
import socket
import subprocess
import tempfile
import time
from tempfile import NamedTemporaryFile

import requests
import requests_kerberos as reqkerb


class mk_krb_conf(object):
    def __init__(self, krb_conf, kdc_ip):
        self.krb_conf = krb_conf
        self.kdc_ip = kdc_ip
        self.amended_krb_conf = None

    def __enter__(self):
        with open(self.krb_conf) as f:
            content = f.read()
        amended_content = content.replace("hdfskerberos", self.kdc_ip)
        self.amended_krb_conf = tempfile.NamedTemporaryFile(delete=False, mode="w+")
        self.amended_krb_conf.write(amended_content)
        self.amended_krb_conf.close()
        return self.amended_krb_conf.name

    def __exit__(self, type, value, traceback):
        if self.amended_krb_conf is not None:
            self.amended_krb_conf.close()


class HDFSApi(object):
    def __init__(
        self,
        user,
        host,
        proxy_port,
        data_port,
        timeout=100,
        kerberized=False,
        principal=None,
        keytab=None,
        krb_conf=None,
        protocol="http",
        hdfs_ip=None,
        kdc_ip=None,
    ):
        self.host = host
        self.protocol = protocol
        self.proxy_port = proxy_port
        self.data_port = data_port
        self.user = user
        self.kerberized = kerberized
        self.principal = principal
        self.keytab = keytab
        self.timeout = timeout
        self.hdfs_ip = hdfs_ip
        self.kdc_ip = kdc_ip
        self.krb_conf = krb_conf

        # logging.basicConfig(level=logging.DEBUG)
        # logging.getLogger().setLevel(logging.DEBUG)
        # requests_log = logging.getLogger("requests.packages.urllib3")
        # requests_log.setLevel(logging.INFO)
        # requests_log.propagate = True
        # kerb_log = logging.getLogger("requests_kerberos")
        # kerb_log.setLevel(logging.DEBUG)
        # kerb_log.propagate = True

        if kerberized:
            self._run_kinit()
            self.kerberos_auth = reqkerb.HTTPKerberosAuth(
                mutual_authentication=reqkerb.DISABLED,
                hostname_override=self.host,
                principal=self.principal,
            )
            if self.kerberos_auth is None:
                print("failed to obtain kerberos_auth")
        else:
            self.kerberos_auth = None

    def _run_kinit(self):
        if self.principal is None or self.keytab is None:
            raise Exception("kerberos principal and keytab are required")

        with mk_krb_conf(self.krb_conf, self.kdc_ip) as instantiated_krb_conf:
            logging.debug("instantiated_krb_conf {}".format(instantiated_krb_conf))

            os.environ["KRB5_CONFIG"] = instantiated_krb_conf

            cmd = "(kinit -R -t {keytab} -k {principal} || (sleep 5 && kinit -R -t {keytab} -k {principal})) ; klist".format(
                instantiated_krb_conf=instantiated_krb_conf,
                keytab=self.keytab,
                principal=self.principal,
            )

            start = time.time()

            while time.time() - start < self.timeout:
                try:
                    res = subprocess.run(cmd, shell=True)
                    if res.returncode != 0:
                        # check_call(...) from subprocess does not print stderr, so we do it manually
                        logging.debug(
                            "Stderr:\n{}\n".format(res.stderr.decode("utf-8"))
                        )
                        logging.debug(
                            "Stdout:\n{}\n".format(res.stdout.decode("utf-8"))
                        )
                        raise Exception(
                            "Command {} return non-zero code {}: {}".format(
                                cmd, res.returncode, res.stderr.decode("utf-8")
                            )
                        )

                    logging.debug("KDC started, kinit successfully run")
                    return
                except Exception as ex:
                    logging.debug("Can't run kinit ... waiting {}".format(str(ex)))
                    time.sleep(1)

        raise Exception("Kinit running failure")

    @staticmethod
    def req_wrapper(func, expected_code, cnt=2, **kwargs):
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
            auth=self.kerberos_auth,
        )
        # additional_params = '&'.join(response.headers['Location'].split('&')[1:2])
        location = None
        if self.kerberized:
            location = response.headers["Location"].replace(
                "kerberizedhdfs1:1006", "{}:{}".format(self.hdfs_ip, self.data_port)
            )
        else:
            location = response.headers["Location"].replace(
                "hdfs1:50075", "{}:{}".format(self.hdfs_ip, self.data_port)
            )
        logging.debug("redirected to {}".format(location))

        response_data = self.req_wrapper(
            requests.get,
            200,
            url=location,
            headers={"host": self.hdfs_ip},
            verify=False,
            auth=self.kerberos_auth,
        )

        if universal_newlines:
            return response_data.text
        else:
            return response_data.content

    def write_data(self, path, content):
        logging.debug(
            "write_data protocol:{} host:{} port:{} path: {} user:{}, principal:{}".format(
                self.protocol,
                self.host,
                self.proxy_port,
                path,
                self.user,
                self.principal,
            )
        )
        named_file = NamedTemporaryFile(mode="wb+")
        fpath = named_file.name
        if isinstance(content, str):
            content = content.encode()
        named_file.write(content)
        named_file.flush()

        response = self.req_wrapper(
            requests.put,
            307,
            url="{protocol}://{ip}:{port}/webhdfs/v1{path}?op=CREATE".format(
                protocol=self.protocol,
                ip=self.hdfs_ip,
                port=self.proxy_port,
                path=path,
                user=self.user,
            ),
            allow_redirects=False,
            headers={"host": str(self.hdfs_ip)},
            params={"overwrite": "true"},
            verify=False,
            auth=self.kerberos_auth,
        )

        logging.debug("HDFS api response:{}".format(response.headers))

        if self.kerberized:
            location = response.headers["Location"].replace(
                "kerberizedhdfs1:1006", "{}:{}".format(self.hdfs_ip, self.data_port)
            )
        else:
            location = response.headers["Location"].replace(
                "hdfs1:50075", "{}:{}".format(self.hdfs_ip, self.data_port)
            )

        with open(fpath, mode="rb") as fh:
            file_data = fh.read()
            protocol = "http"  # self.protocol
            response = self.req_wrapper(
                requests.put,
                201,
                url="{location}".format(location=location),
                data=file_data,
                headers={"content-type": "text/plain", "host": str(self.hdfs_ip)},
                params={"file": path, "user.name": self.user},
                allow_redirects=False,
                verify=False,
                auth=self.kerberos_auth,
            )
            logging.debug(f"{response.content} {response.headers}")

    def write_file(self, path, local_path):
        logging.debug(
            "write_data protocol:{} host:{} port:{} path: {} user:{}, principal:{}".format(
                self.protocol,
                self.host,
                self.proxy_port,
                path,
                self.user,
                self.principal,
            )
        )

        response = self.req_wrapper(
            requests.put,
            307,
            url="{protocol}://{ip}:{port}/webhdfs/v1{path}?op=CREATE".format(
                protocol=self.protocol,
                ip=self.hdfs_ip,
                port=self.proxy_port,
                path=path,
                user=self.user,
            ),
            allow_redirects=False,
            headers={"host": str(self.hdfs_ip)},
            params={"overwrite": "true"},
            verify=False,
            auth=self.kerberos_auth,
        )

        logging.debug("HDFS api response:{}".format(response.headers))

        if self.kerberized:
            location = response.headers["Location"].replace(
                "kerberizedhdfs1:1006", "{}:{}".format(self.hdfs_ip, self.data_port)
            )
        else:
            location = response.headers["Location"].replace(
                "hdfs1:50075", "{}:{}".format(self.hdfs_ip, self.data_port)
            )

        with open(local_path, mode="rb") as fh:
            file_data = fh.read()
            protocol = "http"  # self.protocol
            response = self.req_wrapper(
                requests.put,
                201,
                url="{location}".format(location=location),
                data=file_data,
                headers={"content-type": "text/plain", "host": str(self.hdfs_ip)},
                params={"file": path, "user.name": self.user},
                allow_redirects=False,
                verify=False,
                auth=self.kerberos_auth,
            )
            logging.debug(f"{response.content} {response.headers}")

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
