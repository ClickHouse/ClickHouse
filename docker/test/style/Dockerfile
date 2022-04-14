# docker build -t clickhouse/style-test .
FROM ubuntu:20.04
ARG ACT_VERSION=0.2.25
ARG ACTIONLINT_VERSION=1.6.8

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    curl \
    git \
    libxml2-utils \
    moreutils \
    pylint \
    python3-pip \
    shellcheck \
    yamllint \
    && pip3 install black boto3 codespell dohq-artifactory PyGithub unidiff

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH

# Get act and actionlint from releases
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) act_arch=x86_64 ;; \
      arm64) act_arch=$arch ;; \
    esac \
  && curl -o /tmp/act.tgz -L \
    "https://github.com/nektos/act/releases/download/v${ACT_VERSION}/act_Linux_${act_arch}.tar.gz" \
  && tar xf /tmp/act.tgz -C /usr/bin act \
  && rm /tmp/act.tgz \
  && curl -o /tmp/actiolint.zip -L \
    "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION}_linux_${arch}.tar.gz" \
  && tar xf /tmp/actiolint.zip -C /usr/bin actionlint \
  && rm /tmp/actiolint.zip


COPY run.sh /
COPY process_style_check_result.py /
CMD ["/bin/bash", "/run.sh"]
