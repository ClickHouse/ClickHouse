FROM ubuntu:17.10

RUN apt update -y

RUN apt update -y && \
	apt install -y git cmake libssl-dev libcrypto++-dev \
		libglib2.0-dev libltdl-dev libicu-dev libmysql++-dev \
		libreadline-dev libmysqlclient-dev unixodbc-dev \
		gcc-7 g++-7 unixodbc-dev devscripts dupload fakeroot debhelper \
		liblld-5.0-dev libclang-5.0-dev liblld-5.0

ADD build.sh /
RUN chmod +x /build.sh


CMD ["/build.sh"]
