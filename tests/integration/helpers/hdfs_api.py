# -*- coding: utf-8 -*-
import io
import gzip
import subprocess
import time
from tempfile import NamedTemporaryFile
import requests_kerberos as reqkerb
import socket
import tempfile
import logging
import os

g_dns_hook = None

def custom_getaddrinfo(*args):
    print("from custom_getaddrinfo g_dns_hook is None ", g_dns_hook is None)
    ret = g_dns_hook.custom_getaddrinfo(*args)
    print("g_dns_hook.custom_getaddrinfo result", ret)
    return ret


class mk_krb_conf(object):
    def __init__(self, krb_conf, kdc_ip):
        self.krb_conf = krb_conf
        self.kdc_ip = kdc_ip
        self.amended_krb_conf = None
    def __enter__(self):
        with open(self.krb_conf) as f:
            content = f.read()
        amended_content = content.replace('hdfs_kerberos', self.kdc_ip)
        self.amended_krb_conf = tempfile.NamedTemporaryFile(delete=False)
        self.amended_krb_conf.write(amended_content)
        self.amended_krb_conf.close()
        return self.amended_krb_conf.name
    def __exit__(self, type, value, traceback):
        if self.amended_krb_conf is not None:
            self.amended_krb_conf.close()


class dns_hook(object):
    def __init__(self, hdfs_api):
        print("dns_hook.init ", hdfs_api.kerberized)
        self.hdfs_api = hdfs_api
    def __enter__(self):
        global g_dns_hook
        g_dns_hook = self
        if self.hdfs_api.kerberized:
            print("g_dns_hook is None ", g_dns_hook is None)
            self.original_getaddrinfo = socket.getaddrinfo
            socket.getaddrinfo = custom_getaddrinfo
            return self
    def __exit__(self, type, value, traceback):
        global g_dns_hook
        g_dns_hook = None
        if self.hdfs_api.kerberized:
            socket.getaddrinfo = self.original_getaddrinfo
    def custom_getaddrinfo(self, *args):
        print("top of custom_getaddrinfo")
        (hostname, port) = args[:2]

        if hostname == self.hdfs_api.host and (port == self.hdfs_api.data_port or port == self.hdfs_api.proxy_port):
            print("dns_hook substitute")
            return [(socket.AF_INET, 1, 6, '', (self.hdfs_api.hdfs_ip, port))]
        else:
            return self.original_getaddrinfo(*args)


import requests


class HDFSApi(object):
    def __init__(self, user, timeout=100, kerberized=False, principal=None,
                 keytab=None, krb_conf=None,
                 host = "localhost", protocol = "http",
                 proxy_port = "50070", data_port = "50075", hdfs_ip = None, kdc_ip = None):
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

        logging.basicConfig(level=logging.DEBUG)

        if kerberized:
            self._run_kinit()
            self.kerberos_auth = reqkerb.HTTPKerberosAuth(principal=self.principal, hostname_override=self.host)
                #hostname_override=self.host, principal=self.principal)
                # , mutual_authentication=reqkerb.REQUIRED, force_preemptive=True)
        else:
            self.kerberos_auth = None

    def _run_kinit(self):
        if self.principal is None or self.keytab is None:
            raise Exception("kerberos principal and keytab are required")

        with mk_krb_conf(self.krb_conf, self.kdc_ip) as instantiated_krb_conf:
            print("instantiated_krb_conf ", instantiated_krb_conf)

            os.environ["KRB5_CONFIG"] = instantiated_krb_conf

            cmd = "KRB5_CONFIG={instantiated_krb_conf} kinit -R -t {keytab} -k {principal} || KRB5_CONFIG={instantiated_krb_conf} kinit -R -t {keytab} -k {principal}".format(instantiated_krb_conf=instantiated_krb_conf, keytab=self.keytab, principal=self.principal)

            print(cmd)

            start = time.time()

            while time.time() - start < self.timeout:
                try:
                    subprocess.call(cmd, shell=True)
                    print "KDC started, kinit successfully run"
                    return
                except Exception as ex:
                    print "Can't run kinit ... waiting" + str(ex)
                    time.sleep(1)

        raise Exception("Kinit running failure")

    def read_data(self, path, universal_newlines=True):
        response = requests.get("{protocol}://{host}:{port}/webhdfs/v1{path}?op=OPEN".format(protocol=self.protocol, host=self.host, port=self.proxy_port, path=path), headers={'host': 'localhost'}, allow_redirects=False, verify=False, auth=self.kerberos_auth)
        if response.status_code != 307:
            response.raise_for_status()
        additional_params = '&'.join(response.headers['Location'].split('&')[1:2])
        response_data = requests.get("{protocol}://{host}:{port}/webhdfs/v1{path}?op=OPEN&{params}".format(protocol=self.protocol, host=self.host, port=self.data_port, path=path, params=additional_params), headers={'host': 'localhost'}, verify=False, auth=self.kerberos_auth)
        if response_data.status_code != 200:
            response_data.raise_for_status()

        if universal_newlines:
            return response_data.text
        else:
            return response_data.content

    # Requests can't put file
    def _curl_to_put(self, filename, path, params):
        url = "{protocol}://{host}:{port}/webhdfs/v1{path}?op=CREATE&{params}".format(protocol=self.protocol,
                                                                                      host=self.host,
                                                                                      port=self.data_port,
                                                                                      path=path,
                                                                                      params=params)
        if self.kerberized:
            cmd = "curl -k --negotiate -s -i -X PUT -T {fname} '{url}'".format(fname=filename, url=url)
        else:
            cmd = "curl -s -i -X PUT -T {fname} '{url}'".format(fname=filename, url=url)
        output = subprocess.check_output(cmd, shell=True)
        return output

    def write_data(self, path, content):
        named_file = NamedTemporaryFile(mode='wb+')
        fpath = named_file.name
        if isinstance(content, str):
            content = content.encode()
        named_file.write(content)
        named_file.flush()
        print("before request.put")
        with dns_hook(self):
            response = requests.put(
                "{protocol}://{host}:{port}/webhdfs/v1{path}?op=CREATE".format(protocol=self.protocol, host=self.host,
                                                                               port=self.proxy_port,
                                                                               path=path, user=self.user),
                allow_redirects=False, headers={'host': 'localhost'}, verify=False, auth=self.kerberos_auth
            )
        print("after request.put", response.status_code)
        if response.status_code != 307:
            response.raise_for_status()
        print("after status code check")


        additional_params = '&'.join(
            response.headers['Location'].split('&')[1:2] + ["user.name={}".format(self.user), "overwrite=true"])

        if not self.kerberized:
            output = self._curl_to_put(fpath, path, additional_params)
            if "201 Created" not in output:
                raise Exception("Can't create file on hdfs:\n {}".format(output))
        else:
            with dns_hook(self), open(fpath) as fh:
                file_data = fh.read()
                response = requests.put(
                    "{protocol}://{host}:{port}/webhdfs/v1{path}?op=CREATE&{params}".format(protocol=self.protocol, host=self.host, port=self.proxy_port, path=path, user=self.user, params=additional_params),
                    data=file_data,
                    headers={'content-type':'text/plain', 'host': 'localhost'},
                    params={'file': path},
                    allow_redirects=False, verify=False, auth=self.kerberos_auth
                )


    def write_gzip_data(self, path, content):
        if isinstance(content, str):
            content = content.encode()
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
            f.write(content)
        self.write_data(path, out.getvalue())

    def read_gzip_data(self, path):
        return gzip.GzipFile(fileobj=io.BytesIO(self.read_data(path, universal_newlines=False))).read().decode()
