# -*- coding: utf-8 -*-
import io
import gzip
import subprocess
import time
from tempfile import NamedTemporaryFile
import requests
import requests_kerberos as reqkerb
import socket
import tempfile
import logging
import os

class mk_krb_conf(object):
    def __init__(self, krb_conf, kdc_ip):
        self.krb_conf = krb_conf
        self.kdc_ip = kdc_ip
        self.amended_krb_conf = None
    def __enter__(self):
        with open(self.krb_conf) as f:
            content = f.read()
        amended_content = content.replace('hdfskerberos', self.kdc_ip)
        self.amended_krb_conf = tempfile.NamedTemporaryFile(delete=False, mode="w+")
        self.amended_krb_conf.write(amended_content)
        self.amended_krb_conf.close()
        return self.amended_krb_conf.name
    def __exit__(self, type, value, traceback):
        if self.amended_krb_conf is not None:
            self.amended_krb_conf.close()

class HDFSApi(object):
    def __init__(self, user, timeout=100, kerberized=False, principal=None,
                 keytab=None, krb_conf=None,
                 host = "localhost", protocol = "http",
                 proxy_port = 50070, data_port = 50075, hdfs_ip = None, kdc_ip = None):
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
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

        if kerberized:
            self._run_kinit()
            self.kerberos_auth = reqkerb.HTTPKerberosAuth(mutual_authentication=reqkerb.DISABLED, hostname_override=self.host, principal=self.principal)
                #principal=self.principal,
                #hostname_override=self.host, principal=self.principal)
                # , mutual_authentication=reqkerb.REQUIRED, force_preemptive=True)
        else:
            self.kerberos_auth = None

    def _run_kinit(self):
        if self.principal is None or self.keytab is None:
            raise Exception("kerberos principal and keytab are required")

        with mk_krb_conf(self.krb_conf, self.kdc_ip) as instantiated_krb_conf:
            logging.debug("instantiated_krb_conf ", instantiated_krb_conf)

            os.environ["KRB5_CONFIG"] = instantiated_krb_conf

            cmd = "(kinit -R -t {keytab} -k {principal} || (sleep 5 && kinit -R -t {keytab} -k {principal})) ; klist".format(instantiated_krb_conf=instantiated_krb_conf, keytab=self.keytab, principal=self.principal)

            logging.debug(cmd)

            start = time.time()

            while time.time() - start < self.timeout:
                try:
                    subprocess.call(cmd, shell=True)
                    print("KDC started, kinit successfully run")
                    return
                except Exception as ex:
                    print("Can't run kinit ... waiting {}".format(str(ex)))
                    time.sleep(1)

        raise Exception("Kinit running failure")

    def read_data(self, path, universal_newlines=True):
        logging.debug("read_data protocol:{} host:{} port:{} path: {}".format(self.protocol, self.host, self.proxy_port, path))
        response = requests.get("{protocol}://{host}:{port}/webhdfs/v1{path}?op=OPEN".format(protocol=self.protocol, host=self.host, port=self.proxy_port, path=path), headers={'host': 'localhost'}, allow_redirects=False, verify=False, auth=self.kerberos_auth)
        if response.status_code != 307:
            response.raise_for_status()
        # additional_params = '&'.join(response.headers['Location'].split('&')[1:2])
        url = "{location}".format(location=response.headers['Location'].replace("hdfs1:50075", "{}:{}".format(self.host, self.data_port)))
        logging.debug("redirected to {}".format(url))
        response_data = requests.get(url, headers={'host': 'localhost'},
                                     verify=False, auth=self.kerberos_auth)
        if response_data.status_code != 200:
            response_data.raise_for_status()
        if universal_newlines:
            return response_data.text
        else:
            return response_data.content

    def write_data(self, path, content):
        logging.debug("write_data protocol:{} host:{} port:{} path: {} user:{}".format(self.protocol, self.host, self.proxy_port, path, self.user))
        named_file = NamedTemporaryFile(mode='wb+')
        fpath = named_file.name
        if isinstance(content, str):
            content = content.encode()
        named_file.write(content)
        named_file.flush()

        if self.kerberized:
            self._run_kinit()
            self.kerberos_auth = reqkerb.HTTPKerberosAuth(mutual_authentication=reqkerb.DISABLED, hostname_override=self.host, principal=self.principal)
            logging.debug(self.kerberos_auth)

        response = requests.put(
            "{protocol}://{host}:{port}/webhdfs/v1{path}?op=CREATE".format(protocol=self.protocol, host='localhost',
                                                                            port=self.proxy_port,
                                                                            path=path, user=self.user),
            allow_redirects=False,
            headers={'host': 'localhost'},
            params={'overwrite' : 'true'},
            verify=False, auth=self.kerberos_auth
        )

        logging.debug("HDFS api response:{}".format(response.headers))

        if response.status_code != 307:
            response.raise_for_status()

        # additional_params = '&'.join(
        #     response.headers['Location'].split('&')[1:2] + ["user.name={}".format(self.user), "overwrite=true"])
        location = response.headers['Location'].replace("hdfs1:50075", "{}:{}".format(self.host, self.data_port))

        with open(fpath, mode="rb") as fh:
            file_data = fh.read()
            protocol = "http" # self.protocol
            response = requests.put(
                "{location}".format(location=location),
                data=file_data,
                headers={'content-type':'text/plain', 'host': 'localhost'},
                params={'file': path, 'user.name' : self.user},
                allow_redirects=False, verify=False, auth=self.kerberos_auth
            )
            logging.debug(response)
            if response.status_code != 201:
                response.raise_for_status()


    def write_gzip_data(self, path, content):
        if isinstance(content, str):
            content = content.encode()
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
            f.write(content)
        self.write_data(path, out.getvalue())

    def read_gzip_data(self, path):
        return gzip.GzipFile(fileobj=io.BytesIO(self.read_data(path, universal_newlines=False))).read().decode()
