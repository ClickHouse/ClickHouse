# docker build -t ilejn/kerberized-hadoop .
FROM sequenceiq/hadoop-docker:2.7.0
RUN yum --quiet --assumeyes install krb5-workstation.x86_64
RUN cd /tmp && \
	curl http://archive.apache.org/dist/commons/daemon/source/commons-daemon-1.0.15-src.tar.gz   -o  commons-daemon-1.0.15-src.tar.gz && \
	tar xzf commons-daemon-1.0.15-src.tar.gz && \
	cd commons-daemon-1.0.15-src/src/native/unix && \
	./configure && \
	make && \
	cp ./jsvc /usr/local/hadoop/sbin
