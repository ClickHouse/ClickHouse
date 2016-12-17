FROM ubuntu:14.04

RUN apt-get update -y

RUN apt-get install -y software-properties-common && \
	add-apt-repository ppa:ubuntu-toolchain-r/test

RUN apt-get update -y && \
	apt-get install -y cmake libssl-dev libcrypto++-dev \
		libglib2.0-dev libltdl-dev libicu-dev libmysql++-dev \
		gcc-5 g++-5 unixodbc-dev

ADD build.sh /
RUN chmod +x /build.sh


CMD ["/build.sh"]
