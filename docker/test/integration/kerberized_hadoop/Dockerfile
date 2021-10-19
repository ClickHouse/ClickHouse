# docker build -t clickhouse/kerberized-hadoop .

FROM sequenceiq/hadoop-docker:2.7.0

RUN sed -i -e 's/^\#baseurl/baseurl/' /etc/yum.repos.d/CentOS-Base.repo && \
	sed -i -e 's/^mirrorlist/#mirrorlist/' /etc/yum.repos.d/CentOS-Base.repo && \
	sed -i -e 's#http://mirror.centos.org/#http://vault.centos.org/#' /etc/yum.repos.d/CentOS-Base.repo

# https://community.letsencrypt.org/t/rhel-centos-6-openssl-client-compatibility-after-dst-root-ca-x3-expiration/161032/81
RUN sed -i s/xMDkzMDE0MDExNVow/0MDkzMDE4MTQwM1ow/ /etc/pki/tls/certs/ca-bundle.crt

RUN yum clean all && \
	rpm --rebuilddb &&	\
	yum -y update && \
	yum -y install yum-plugin-ovl && \
	yum --quiet -y install krb5-workstation.x86_64

RUN cd /tmp && \
	curl http://archive.apache.org/dist/commons/daemon/source/commons-daemon-1.0.15-src.tar.gz -o commons-daemon-1.0.15-src.tar.gz && \
	tar xzf commons-daemon-1.0.15-src.tar.gz && \
	cd commons-daemon-1.0.15-src/src/native/unix && \
	./configure && \
	make && \
	cp ./jsvc /usr/local/hadoop/sbin
