# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:8-jre-slim AS build

RUN apt-get update && apt-get -y install curl

ENV HADOOP_VERSION=3.3.6
ENV AWS_SDK_BUNDLE=1.12.753

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -Lo /tmp/hadoop-aws-${HADOOP_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar -Lo /tmp/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar

FROM apache/hive:4.0.0

ENV HADOOP_VERSION=3.3.6
ENV AWS_SDK_BUNDLE=1.12.753

COPY --from=build /tmp/hadoop-aws-${HADOOP_VERSION}.jar /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar
COPY --from=build /tmp/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
