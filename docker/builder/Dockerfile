FROM ubuntu:14.04

RUN apt-get update -y

RUN apt-get install -y software-properties-common && \
	add-apt-repository ppa:ubuntu-toolchain-r/test

RUN apt-get update -y && \
	apt-get install -y git cmake3 libssl-dev libcrypto++-dev \
		libglib2.0-dev libltdl-dev libicu-dev libmysql++-dev \
		libreadline-dev libmysqlclient-dev unixodbc-dev \
		gcc-7 g++-7 unixodbc-dev devscripts dupload fakeroot debhelper

ADD build.sh /
ADD build_gitlab_ci.sh /
RUN chmod +x /build.sh
RUN chmod +x /build_gitlab_ci.sh

CMD ["/build.sh"]
