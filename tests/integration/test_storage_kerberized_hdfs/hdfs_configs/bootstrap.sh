#!/bin/bash

: "${HADOOP_PREFIX:=/usr/local/hadoop}"

cat >> $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh <<EOF
export HADOOP_SECURE_DN_USER=hdfs
export HADOOP_SECURE_DN_PID_DIR=$HADOOP_PREFIX/pid
export HADOOP_SECURE_DN_LOG_DIR=$HADOOP_PREFIX/logs/hdfs
export JSVC_HOME=$HADOOP_PREFIX/sbin
EOF
$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

mkdir -p "${HADOOP_SECURE_DN_PID_DIR}"
mkdir -p "${HADOOP_SECURE_DN_LOG_DIR}"

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd "${HADOOP_PREFIX}/share/hadoop/common" || exit
for cp in ${ACP//,/ }; do  echo "== ${cp}"; curl -LO "${cp}" ; done;
cd - || exit

# altering the core-site configuration
sed "s/HOSTNAME/${HOSTNAME}/" /usr/local/hadoop/etc/hadoop/core-site.xml.template | grep -v '/configuration' > /usr/local/hadoop/etc/hadoop/core-site.xml

cat >> /usr/local/hadoop/etc/hadoop/core-site.xml << EOF
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value> <!-- A value of "simple" would disable security. -->
  </property>
   <property>
     <name>hadoop.security.authorization</name>
     <value>true</value>
   </property>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://kerberizedhdfs1:9010</value>
  </property>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://kerberizedhdfs1:9010</value>
  </property>
  <!--
  <property>
      <name>hadoop.rpc.protection</name>
      <value>privacy</value>
  </property>
  -->
</configuration>
EOF


cat > /usr/local/hadoop/etc/hadoop/hdfs-site.xml << EOF
<configuration>
<!--
<property>
	<name>dfs.permissions.enabled</name>
	<value>false</value>
</property>
-->
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
<!-- General HDFS security config -->
<property>
  <name>dfs.block.access.token.enable</name>
  <value>true</value>
</property>
<!-- NameNode security config -->
<property>
  <name>dfs.namenode.keytab.file</name>
  <value>/usr/local/hadoop/etc/hadoop/conf/hdfs.keytab</value> <!-- path to the HDFS keytab -->
</property>
<property>
  <name>dfs.namenode.kerberos.principal</name>
  <value>hdfs/_HOST@TEST.CLICKHOUSE.TECH</value>
</property>
<property>
  <name>dfs.namenode.kerberos.internal.spnego.principal</name>
  <value>HTTP/_HOST@TEST.CLICKHOUSE.TECH</value>
</property>
<!-- Secondary NameNode security config -->
<property>
  <name>dfs.secondary.namenode.keytab.file</name>
  <value>/usr/local/hadoop/etc/hadoop/conf/hdfs.keytab</value> <!-- path to the HDFS keytab -->
</property>
<property>
  <name>dfs.secondary.namenode.kerberos.principal</name>
  <value>hdfs/_HOST@TEST.CLICKHOUSE.TECH</value>
</property>
<property>
  <name>dfs.secondary.namenode.kerberos.internal.spnego.principal</name>
  <value>HTTP/_HOST@TEST.CLICKHOUSE.TECH</value>
</property>
<!-- DataNode security config
<property>
   <name>dfs.data.transfer.protectionл</name>
   <value>integrity</value>
</property>
-->
<property>
  <name>dfs.datanode.data.dir.perm</name>
  <value>700</value>
</property>
<property>
  <name>dfs.datanode.address</name>
  <value>0.0.0.0:1004</value>
</property>
<property>
  <name>dfs.datanode.http.address</name>
  <value>0.0.0.0:1006</value>
</property>
<!--
<property>
  <name>dfs.http.policy</name>
  <value>HTTPS_ONLY</value>
</property>
-->


<property>
  <name>dfs.datanode.keytab.file</name>
  <value>/usr/local/hadoop/etc/hadoop/conf/hdfs.keytab</value> <!-- path to the HDFS keytab -->
</property>
<property>
  <name>dfs.datanode.kerberos.principal</name>
  <value>hdfs/_HOST@TEST.CLICKHOUSE.TECH</value>
</property>

<!-- Web Authentication config -->
<property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
</property>
<property>
<name>dfs.encrypt.data.transfer</name>
<value>false</value>
</property>
<property>
  <name>dfs.web.authentication.kerberos.principal</name>
  <value>HTTP/_HOST@TEST.CLICKHOUSE.TECH</value>
</property>
<property>
    <name>dfs.web.authentication.kerberos.keytab</name>
  <value>/usr/local/hadoop/etc/hadoop/conf/hdfs.keytab</value> <!-- path to the HDFS keytab -->
</property>
</configuration>
EOF



# cat > /usr/local/hadoop/etc/hadoop/ssl-server.xml << EOF
# <configuration>
# <property>
#   <name>ssl.server.truststore.location</name>
#   <value>/usr/local/hadoop/etc/hadoop/conf/hdfs.jks</value>
# </property>
# <property>
#   <name>ssl.server.truststore.password</name>
#   <value>masterkey</value>
# </property>
# <property>
#   <name>ssl.server.keystore.location</name>
#   <value>/usr/local/hadoop/etc/hadoop/conf/hdfs.jks</value>
# </property>
# <property>
#   <name>ssl.server.keystore.password</name>
#   <value>masterkey</value>
# </property>
# <property>
#   <name>ssl.server.keystore.keypassword</name>
#   <value>masterkey</value>
# </property>
# </configuration>
# EOF

cat > /usr/local/hadoop/etc/hadoop/log4j.properties << EOF
# Set everything to be logged to the console
log4j.rootCategory=DEBUG, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Set the default spark-shell log level to WARN. When running the spark-shell, the
# log level for this class is used to overwrite the root logger's log level, so that
# the user can have different defaults for the shell and regular Spark apps.
log4j.logger.org.apache.spark.repl.Main=INFO

# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark_project.jetty=DEBUG
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR

log4j.logger.org.apache.spark.deploy=DEBUG
log4j.logger.org.apache.spark.executor=DEBUG
log4j.logger.org.apache.spark.scheduler=DEBUG
EOF

useradd -u 1098 hdfs

# keytool -genkey -alias kerberized_hdfs1.test.clickhouse.tech -keyalg rsa -keysize 1024 -dname "CN=kerberized_hdfs1.test.clickhouse.tech" -keypass masterkey -keystore /usr/local/hadoop/etc/hadoop/conf/hdfs.jks -storepass masterkey
keytool -genkey -alias kerberizedhdfs1 -keyalg rsa -keysize 1024 -dname "CN=kerberizedhdfs1" -keypass masterkey -keystore /usr/local/hadoop/etc/hadoop/conf/hdfs.jks -storepass masterkey

chmod g+r /usr/local/hadoop/etc/hadoop/conf/hdfs.jks


service sshd start

# yum --quiet --assumeyes install krb5-workstation.x86_64
# yum --quiet --assumeyes install tcpdump

# cd /tmp
# curl http://archive.apache.org/dist/commons/daemon/source/commons-daemon-1.0.15-src.tar.gz   -o  commons-daemon-1.0.15-src.tar.gz
# tar xzf commons-daemon-1.0.15-src.tar.gz
# cd commons-daemon-1.0.15-src/src/native/unix
# ./configure && make
# cp ./jsvc /usr/local/hadoop/sbin


until kinit -kt /usr/local/hadoop/etc/hadoop/conf/hdfs.keytab hdfs/kerberizedhdfs1@TEST.CLICKHOUSE.TECH; do sleep 2; done
echo "KDC is up and ready to go... starting up"

$HADOOP_PREFIX/sbin/start-dfs.sh
$HADOOP_PREFIX/sbin/start-yarn.sh
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver

chmod a+r /usr/local/hadoop/etc/hadoop/conf/hdfs.keytab   # create dedicated keytab for hdfsuser

$HADOOP_PREFIX/sbin/start-secure-dns.sh
sleep 3

/usr/local/hadoop/bin/hdfs dfsadmin -safemode leave

/usr/local/hadoop/bin/hdfs dfs -mkdir /user/specuser
/usr/local/hadoop/bin/hdfs dfs -chown specuser /user/specuser
echo "chown_completed" | /usr/local/hadoop/bin/hdfs dfs -appendToFile - /preparations_done_marker

kdestroy



# adduser --groups hdfs hdfsuser

# /usr/local/hadoop/sbin/hadoop-daemon.sh --config /usr/local/hadoop/etc/hadoop/ --script /usr/local/hadoop/sbin/hdfs start namenode
# /usr/local/hadoop/sbin/hadoop-daemon.sh --config /usr/local/hadoop/etc/hadoop/ --script /usr/local/hadoop/sbin/hdfs start datanode


if [[ $1 == "-d" ]]; then
  while true; do sleep 1000; done
fi

if [[ $1 == "-bash" ]]; then
  /bin/bash
fi
