# docker build -t clickhouse/kerberized-hadoop .

FROM prasanthj/docker-hadoop:2.6.0

RUN apt update && \
    apt install -y krb5-user && \
    apt install -y krb5-multidev && \
    apt install -y openssh-server && \
    apt install -y libssl-dev && \
    apt install -y jsvc

RUN cd /tmp && \
    mkdir /var/log/kerberos && \
    { [ -e /usr/local/hadoop ] || ln -s ./hadoop-2.6.0 /usr/local/hadoop; }
