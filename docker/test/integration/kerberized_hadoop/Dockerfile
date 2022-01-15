# docker build -t clickhouse/kerberized-hadoop .

FROM sequenceiq/hadoop-docker:2.7.0

# https://community.letsencrypt.org/t/rhel-centos-6-openssl-client-compatibility-after-dst-root-ca-x3-expiration/161032/81
RUN sed -i s/xMDkzMDE0MDExNVow/0MDkzMDE4MTQwM1ow/ /etc/pki/tls/certs/ca-bundle.crt


RUN curl -o krb5-libs-1.10.3-65.el6.x86_64.rpm ftp://ftp.pbone.net/mirror/vault.centos.org/6.10/os/x86_64/Packages/krb5-libs-1.10.3-65.el6.x86_64.rpm && \
    curl -o krb5-workstation-1.10.3-65.el6.x86_64.rpm ftp://ftp.pbone.net/mirror/vault.centos.org/6.9/os/x86_64/Packages/krb5-workstation-1.10.3-65.el6.x86_64.rpm && \
    curl -o libkadm5-1.10.3-65.el6.x86_64.rpm ftp://ftp.pbone.net/mirror/vault.centos.org/6.10/os/x86_64/Packages/libkadm5-1.10.3-65.el6.x86_64.rpm && \
    curl -o libss-1.41.12-24.el6.x86_64.rpm ftp://ftp.pbone.net/mirror/vault.centos.org/6.9/cr/x86_64/Packages/libss-1.41.12-24.el6.x86_64.rpm && \
    curl -o libcom_err-1.41.12-24.el6.x86_64.rpm ftp://ftp.pbone.net/mirror/vault.centos.org/6.9/cr/x86_64/Packages/libcom_err-1.41.12-24.el6.x86_64.rpm && \
    rpm -Uvh libkadm5-1.10.3-65.el6.x86_64.rpm libss-1.41.12-24.el6.x86_64.rpm krb5-libs-1.10.3-65.el6.x86_64.rpm krb5-workstation-1.10.3-65.el6.x86_64.rpm libcom_err-1.41.12-24.el6.x86_64.rpm && \
    rm -fr *.rpm

RUN cd /tmp && \
	curl http://archive.apache.org/dist/commons/daemon/source/commons-daemon-1.0.15-src.tar.gz -o commons-daemon-1.0.15-src.tar.gz && \
	tar xzf commons-daemon-1.0.15-src.tar.gz && \
	cd commons-daemon-1.0.15-src/src/native/unix && \
	./configure && \
	make && \
	cp ./jsvc /usr/local/hadoop/sbin
