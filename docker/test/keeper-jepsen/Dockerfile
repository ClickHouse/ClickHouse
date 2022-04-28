# rebuild in #33610
# docker build -t clickhouse/keeper-jepsen-test .
ARG FROM_TAG=latest
FROM altinityinfra/test-base:$FROM_TAG

ENV DEBIAN_FRONTEND=noninteractive
ENV CLOJURE_VERSION=1.10.3.814

# arguments
ENV PR_TO_TEST=""
ENV SHA_TO_TEST=""

ENV NODES_USERNAME="root"
ENV NODES_PASSWORD=""
ENV TESTS_TO_RUN="30"
ENV TIME_LIMIT="30"


# volumes
ENV NODES_FILE_PATH="/nodes.txt"
ENV TEST_OUTPUT="/test_output"

RUN mkdir "/root/.ssh"
RUN touch "/root/.ssh/known_hosts"

# install java
RUN apt-get update && apt-get install default-jre default-jdk libjna-java libjna-jni ssh gnuplot graphviz --yes --no-install-recommends

# install clojure
RUN curl -O "https://download.clojure.org/install/linux-install-${CLOJURE_VERSION}.sh" && \
    chmod +x "linux-install-${CLOJURE_VERSION}.sh" && \
    bash "./linux-install-${CLOJURE_VERSION}.sh"

# install leiningen
RUN curl -O "https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein" && \
    chmod +x ./lein && \
    mv ./lein /usr/bin

COPY run.sh /

CMD ["/bin/bash", "/run.sh"]
