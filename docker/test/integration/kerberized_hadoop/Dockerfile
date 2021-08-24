# docker build -t yandex/clickhouse-kerberized-hadoop .

FROM sequenceiq/hadoop-docker:2.7.0
RUN sed -i -e 's/^\#baseurl/baseurl/' /etc/yum.repos.d/CentOS-Base.repo
RUN sed -i -e 's/^mirrorlist/#mirrorlist/' /etc/yum.repos.d/CentOS-Base.repo
RUN sed -i -e 's#http://mirror.centos.org/#http://vault.centos.org/#' /etc/yum.repos.d/CentOS-Base.repo
RUN yum clean all && \
	rpm --rebuilddb &&	\
	yum -y update && \
	yum -y install yum-plugin-ovl && \
	yum --quiet -y install krb5-workstation.x86_64
RUN cd /tmp && \
	curl http://archive.apache.org/dist/commons/daemon/source/commons-daemon-1.0.15-src.tar.gz   -o  commons-daemon-1.0.15-src.tar.gz && \
	tar xzf commons-daemon-1.0.15-src.tar.gz && \
	cd commons-daemon-1.0.15-src/src/native/unix && \
	./configure && \
	make && \
	cp ./jsvc /usr/local/hadoop/sbin
