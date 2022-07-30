FROM ubuntu:20.04
MAINTAINER lgbo-ustc <lgbo.ustc@gmail.com>

RUN apt-get update 
RUN apt-get install -y wget openjdk-8-jre

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.1.0/hadoop-3.1.0.tar.gz && \
        tar -xf hadoop-3.1.0.tar.gz && rm -rf hadoop-3.1.0.tar.gz
RUN wget https://dlcdn.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz && \
        tar -xf apache-hive-2.3.9-bin.tar.gz && rm -rf apache-hive-2.3.9-bin.tar.gz
RUN apt install -y vim

RUN apt install -y openssh-server openssh-client

RUN apt install -y mysql-server

RUN mkdir -p /root/.ssh && \
        ssh-keygen -t rsa -b 2048 -P '' -f /root/.ssh/id_rsa && \
        cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys && \
        cp /root/.ssh/id_rsa /etc/ssh/ssh_host_rsa_key && \
        cp /root/.ssh/id_rsa.pub /etc/ssh/ssh_host_rsa_key.pub

RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.27.tar.gz &&\
        tar -xf mysql-connector-java-8.0.27.tar.gz && \
        mv mysql-connector-java-8.0.27/mysql-connector-java-8.0.27.jar /apache-hive-2.3.9-bin/lib/ && \
        rm -rf mysql-connector-java-8.0.27.tar.gz mysql-connector-java-8.0.27

RUN apt install -y iputils-ping net-tools

ENV JAVA_HOME=/usr
ENV HADOOP_HOME=/hadoop-3.1.0
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root HDFS_DATANODE_SECURE_USER=hdfs
COPY hdfs-site.xml /hadoop-3.1.0/etc/hadoop
COPY mapred-site.xml /hadoop-3.1.0/etc/hadoop
COPY yarn-site.xml /hadoop-3.1.0/etc/hadoop
COPY hadoop-env.sh /hadoop-3.1.0/etc/hadoop/
#COPY core-site.xml /hadoop-3.1.0/etc/hadoop
COPY core-site.xml.template /hadoop-3.1.0/etc/hadoop
COPY hive-site.xml /apache-hive-2.3.9-bin/conf
COPY prepare_hive_data.sh /
COPY demo_data.txt /

ENV PATH=/apache-hive-2.3.9-bin/bin:/hadoop-3.1.0/bin:/hadoop-3.1.0/sbin:$PATH
RUN service ssh start && sed s/HOSTNAME/$HOSTNAME/ /hadoop-3.1.0/etc/hadoop/core-site.xml.template > /hadoop-3.1.0/etc/hadoop/core-site.xml && hdfs namenode -format
COPY start.sh /

