FROM ubuntu:18.04

RUN apt-get update && apt-get -y install tzdata

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
